# Databricks notebook source
# Job parameters: values supplied by a job, or entered above the notebook.

def select_ready_delivery(spark, requested_batch_id):
    """Require this exact ready delivery; never fall back to a different one."""
    import re
    label = requested_batch_id.strip()
    if not re.fullmatch(r"[0-9a-f]{64}", label):
        raise ValueError("batch_id must be the 64-character delivery ID from Bronze. "
                         "For jobs use {{tasks.ingest.values.batch_id}} as a task parameter.")
    rows = spark.sql("""
        SELECT batch_id, payment_row_count, bank_row_count
        FROM workspace.reconciliation_bronze.ingestion_batches
        WHERE batch_id = :batch AND status = 'ready'
        LIMIT 2
    """, args={"batch": label}).collect()
    if len(rows) != 1 or rows[0]["batch_id"] != label:
        raise ValueError("The selected delivery must have exactly one ready registry row. "
                         "Complete Bronze loading for this delivery first.")
    return rows[0]


dbutils.widgets.text("batch_id", "", "Delivery ID from Bronze")
selected_delivery = select_ready_delivery(spark, dbutils.widgets.get("batch_id"))

# END JOB PARAMETERS

# Notebook: 02_prepare_silver | Language: Python
# Aim: prepare comparable fields from the stored Bronze tables.
# Creates two persistent SQL views. Does not change Bronze rows or validate payments.
# Runs independently of notebook 01's Python variables.
# Only deliveries whose registry status is ready are included.

spark.sql("""
    CREATE OR REPLACE VIEW workspace.reconciliation_silver.stg_payment_transactions AS
    SELECT
        p.batch_id,
        p.source_row_number,
        p.provider_transaction_id AS transaction_id,
        CAST(p.created AS DATE) AS transaction_date,
        UPPER(p.currency) AS currency,
        CAST(CAST(p.amount AS DECIMAL(18, 0)) / 100 AS DECIMAL(18, 2)) AS amount_gross,
        CAST(CAST(p.fee AS DECIMAL(18, 0)) / 100 AS DECIMAL(18, 2)) AS fee_amount,
        CAST(CAST(p.net AS DECIMAL(18, 0)) / 100 AS DECIMAL(18, 2)) AS amount_net,
        p.status,
        p.type AS transaction_type,
        p.reporting_category,
        'stripe_sample_csv' AS source_system,
        p.source AS provider_source_reference,
        p.imported_at
    FROM workspace.reconciliation_bronze.raw_payment_history p
    WHERE EXISTS (
        SELECT 1
        FROM workspace.reconciliation_bronze.ingestion_batches b
        WHERE b.batch_id = p.batch_id AND b.status = 'ready'
    )
""")

spark.sql("""
    CREATE OR REPLACE VIEW workspace.reconciliation_silver.stg_bank_transactions AS
    SELECT
        k.batch_id,
        k.source_row_number,
        k.bank_transaction_id,
        CAST(k.booking_date AS DATE) AS booking_date,
        k.reference_text,
        NULLIF(regexp_extract(k.reference_text, '(txn_[0-9]+|pp_[0-9]+)', 0), '')
            AS matched_transaction_id,
        CAST(k.amount AS DECIMAL(12, 2)) AS bank_amount,
        UPPER(k.currency) AS currency,
        k.counterparty,
        k.imported_at
    FROM workspace.reconciliation_bronze.raw_bank_history k
    WHERE EXISTS (
        SELECT 1
        FROM workspace.reconciliation_bronze.ingestion_batches b
        WHERE b.batch_id = k.batch_id AND b.status = 'ready'
    )
""")

print("Silver views created. Bronze data unchanged.")

# COMMAND ----------

# Preview the selected ready delivery. The views themselves contain all ready batches.
# Expected for 01_initial: 5 payment rows and 3 bank rows.
# Always retain batch_id when comparing records from multiple deliveries later.
# matched_transaction_id is a candidate reference, not proof of a valid payment.

preview_batch_id = selected_delivery["batch_id"]

silver_payments = spark.sql("""
    SELECT * FROM workspace.reconciliation_silver.stg_payment_transactions
    WHERE batch_id = :batch
    ORDER BY source_row_number
""", args={"batch": preview_batch_id})

silver_bank = spark.sql("""
    SELECT * FROM workspace.reconciliation_silver.stg_bank_transactions
    WHERE batch_id = :batch
    ORDER BY source_row_number
""", args={"batch": preview_batch_id})

payment_count = silver_payments.count()
bank_count = silver_bank.count()
if payment_count != selected_delivery["payment_row_count"]:
    raise ValueError("Silver payment count differs from the registered delivery.")
if bank_count != selected_delivery["bank_row_count"]:
    raise ValueError("Silver bank count differs from the registered delivery.")

payment_types = dict(silver_payments.dtypes)
bank_types = dict(silver_bank.dtypes)
for column in ("amount_gross", "fee_amount", "amount_net"):
    if payment_types[column] != "decimal(18,2)":
        raise TypeError(f"Unexpected payment type for {column}: {payment_types[column]}")
if bank_types["bank_amount"] != "decimal(12,2)":
    raise TypeError(f"Unexpected bank amount type: {bank_types['bank_amount']}")
if payment_types["transaction_date"] != "date" or bank_types["booking_date"] != "date":
    raise TypeError("Prepared booking and transaction dates must have the DATE type.")

print(f"Preview batch: {preview_batch_id}")
print(f"Silver payment rows: {payment_count}")
print(f"Silver bank rows: {bank_count}")
print("Row counts and prepared column types checked.")
print("Preparation only. No payment has been classified as valid or invalid yet.")

display(silver_payments.select(
    "source_row_number", "transaction_id", "transaction_date", "currency",
    "amount_gross", "fee_amount", "amount_net", "status", "transaction_type"
).limit(25))

display(silver_bank.select(
    "source_row_number", "bank_transaction_id", "booking_date",
    "reference_text", "matched_transaction_id", "bank_amount", "currency"
).limit(25))
