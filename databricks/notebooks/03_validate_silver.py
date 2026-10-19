# Databricks notebook source
# Notebook: 03_validate_silver | Language: Python
# Aim: apply the existing project's nine business rules to prepared Silver data.
# Creates two persistent views. Does not modify Bronze or store historical results.
# Reference: dbt_validation_pipeline/models/validation/dbt_invalid_transactions.sql
#            dbt_validation_pipeline/models/validation/dbt_valid_transactions.sql
# Batch IDs are included in every join/grouping to keep deliveries separate.
# Run ingestion, preparation and validation sequentially.

findings_sql = """
WITH payments AS (
    SELECT * FROM workspace.reconciliation_silver.stg_payment_transactions
), bank AS (
    SELECT * FROM workspace.reconciliation_silver.stg_bank_transactions
), duplicate_transactions AS (
    SELECT batch_id, transaction_id
    FROM payments
    WHERE transaction_id IS NOT NULL
    GROUP BY batch_id, transaction_id
    HAVING COUNT(*) > 1
)

-- Rule 1: preserve one duplicate finding for EACH duplicated source row.
SELECT p.batch_id, p.source_row_number, p.transaction_id,
    'payment_provider' AS record_source,
    'duplicate_transaction_id' AS error_code,
    'This payment ID occurs more than once in this delivery.' AS error_description,
    'transaction_id' AS error_field,
    CAST(NULL AS BIGINT) AS related_bank_row_number,
    CURRENT_TIMESTAMP AS detected_at
FROM payments p
JOIN duplicate_transactions d
    ON p.batch_id = d.batch_id AND p.transaction_id = d.transaction_id

UNION ALL

-- Rule 2: missing gross OR net; a zero amount is not missing.
SELECT p.batch_id, p.source_row_number, p.transaction_id,
    'payment_provider', 'missing_amount',
    'The payment gross or net amount is missing.',
    'amount_gross / amount_net', CAST(NULL AS BIGINT), CURRENT_TIMESTAMP
FROM payments p
WHERE p.amount_gross IS NULL OR p.amount_net IS NULL

UNION ALL

-- Rule 3: retain the original SQL behavior for a non-EUR payment currency.
SELECT p.batch_id, p.source_row_number, p.transaction_id,
    'payment_provider', 'invalid_currency',
    'The payment currency is not EUR.',
    'currency', CAST(NULL AS BIGINT), CURRENT_TIMESTAMP
FROM payments p WHERE p.currency <> 'EUR'

UNION ALL

-- Rule 4: pending is an exception to this project's final-payment reporting.
SELECT p.batch_id, p.source_row_number, p.transaction_id,
    'payment_provider', 'pending_transaction',
    'The payment is pending and is not counted as a final payment.',
    'status', CAST(NULL AS BIGINT), CURRENT_TIMESTAMP
FROM payments p WHERE p.status = 'pending'

UNION ALL

-- Rule 5: refunds are identified separately from ordinary charges.
SELECT p.batch_id, p.source_row_number, p.transaction_id,
    'payment_provider', 'refund_transaction',
    'The transaction is a refund, not an ordinary charge.',
    'transaction_type', CAST(NULL AS BIGINT), CURRENT_TIMESTAMP
FROM payments p WHERE p.transaction_type = 'refund'

UNION ALL

-- Rule 6: a matching reference with a net difference above one cent.
SELECT p.batch_id, p.source_row_number, p.transaction_id,
    'payment_provider', 'amount_mismatch',
    'The payment net and bank amount differ by more than EUR 0.01.',
    'amount_net / bank_amount', b.source_row_number, CURRENT_TIMESTAMP
FROM payments p
JOIN bank b ON p.batch_id = b.batch_id
    AND p.transaction_id = b.matched_transaction_id
WHERE ABS(p.amount_net - b.bank_amount) > CAST(0.01 AS DECIMAL(18,2))

UNION ALL

-- Rule 7: only an available charge requires a matching bank booking here.
SELECT p.batch_id, p.source_row_number, p.transaction_id,
    'payment_provider', 'missing_bank_match',
    'No bank booking with a matching payment ID was found in this delivery.',
    'matched_transaction_id', CAST(NULL AS BIGINT), CURRENT_TIMESTAMP
FROM payments p
LEFT JOIN bank b ON p.batch_id = b.batch_id
    AND p.transaction_id = b.matched_transaction_id
WHERE b.matched_transaction_id IS NULL
    AND p.status = 'available' AND p.transaction_type = 'charge'

UNION ALL

-- Rule 8: bank reference contains an ID, but that payment does not exist here.
SELECT b.batch_id, b.source_row_number, b.matched_transaction_id,
    'bank_export', 'unmatched_bank_transaction',
    'The bank reference ID has no payment record in this delivery.',
    'matched_transaction_id', CAST(NULL AS BIGINT), CURRENT_TIMESTAMP
FROM bank b
LEFT JOIN payments p ON b.batch_id = p.batch_id
    AND b.matched_transaction_id = p.transaction_id
WHERE b.matched_transaction_id IS NOT NULL AND p.transaction_id IS NULL

UNION ALL

-- Rule 9: no supported payment ID could be extracted from the bank reference.
-- Here transaction_id carries the bank ID, matching the original model.
SELECT b.batch_id, b.source_row_number, b.bank_transaction_id,
    'bank_export', 'bank_reference_id_not_extractable',
    'No supported payment ID could be extracted from the bank reference.',
    'reference_text', CAST(NULL AS BIGINT), CURRENT_TIMESTAMP
FROM bank b WHERE b.matched_transaction_id IS NULL
"""

valid_sql = """
SELECT p.batch_id, p.source_row_number,
    b.source_row_number AS bank_source_row_number, b.bank_transaction_id,
    p.transaction_id, p.transaction_date,
    p.amount_gross, p.fee_amount, p.amount_net, b.bank_amount,
    p.currency, p.status, 'valid' AS validation_status
FROM workspace.reconciliation_silver.stg_payment_transactions p
JOIN workspace.reconciliation_silver.stg_bank_transactions b
    ON p.batch_id = b.batch_id AND p.transaction_id = b.matched_transaction_id
WHERE p.currency = 'EUR'
    AND p.amount_gross IS NOT NULL AND p.amount_net IS NOT NULL
    AND p.status = 'available' AND p.transaction_type = 'charge'
    AND ABS(p.amount_net - b.bank_amount) <= CAST(0.01 AS DECIMAL(18,2))
    AND NOT EXISTS (
        SELECT 1 FROM workspace.reconciliation_silver.invalid_transactions i
        WHERE i.batch_id = p.batch_id
            AND i.record_source = 'payment_provider'
            AND i.transaction_id = p.transaction_id
    )
"""

# No Python variables from another notebook are needed.
ready = spark.sql("""
    SELECT batch_id FROM workspace.reconciliation_bronze.ingestion_batches
    WHERE status = 'ready' ORDER BY loaded_at DESC, batch_id DESC LIMIT 1
""").collect()
if not ready:
    raise ValueError("No ready delivery found. Complete Bronze loading first.")
validation_batch_id = ready[0]["batch_id"]

spark.sql("CREATE OR REPLACE VIEW workspace.reconciliation_silver.invalid_transactions AS "
          + findings_sql)
spark.sql("CREATE OR REPLACE VIEW workspace.reconciliation_silver.valid_transactions AS "
          + valid_sql)
print("Validation views created. Bronze data unchanged.")

# COMMAND ----------

# Show results for the latest ready delivery; the views retain all ready batches.
from collections import Counter

findings = spark.sql("""
    SELECT * FROM workspace.reconciliation_silver.invalid_transactions
    WHERE batch_id = :batch
    ORDER BY record_source, source_row_number, error_code, related_bank_row_number
""", args={"batch": validation_batch_id})

valid = spark.sql("""
    SELECT * FROM workspace.reconciliation_silver.valid_transactions
    WHERE batch_id = :batch ORDER BY source_row_number, bank_source_row_number
""", args={"batch": validation_batch_id})

print(f"Validated batch: {validation_batch_id}")
print(f"Valid payment rows: {valid.count()}")
print(f"Finding rows: {findings.count()}")
print("A finding is one detected issue, not necessarily one different payment.")

display(valid.select("transaction_id", "amount_net", "bank_amount", "validation_status"))
display(findings.select("source_row_number", "transaction_id", "record_source",
                        "error_code", "error_description", "related_bank_row_number"))

# COMMAND ----------

# Fixture check for the files we are currently using.
# For 02_updated, change expected_scenario to "02_updated".
# For your own data, set it to None; fixed example expectations are then skipped.
# Select the intended ready batch before running a scenario check in future jobs.
expected_scenario = "01_initial"
expected_findings = {
    "01_initial": Counter({
        ("payment_provider", "txn_103", "duplicate_transaction_id"): 2,
        ("payment_provider", "txn_102", "pending_transaction"): 1,
        ("payment_provider", "txn_104", "amount_mismatch"): 1,
    }),
    "02_updated": Counter({
        ("payment_provider", "txn_103", "duplicate_transaction_id"): 2,
        ("payment_provider", "txn_104", "amount_mismatch"): 1,
    }),
}
expected_valid_ids = {"01_initial": ["txn_101"], "02_updated": ["txn_101", "txn_102"]}
if expected_scenario is not None:
    if expected_scenario not in expected_findings:
        raise ValueError("Unknown expected scenario; use 01_initial, 02_updated or None.")
    actual_findings = Counter(
        (r["record_source"], r["transaction_id"], r["error_code"])
        for r in findings.select("record_source", "transaction_id", "error_code").collect()
    )
    actual_valid_ids = sorted(r["transaction_id"] for r in valid.select("transaction_id").collect())
    if actual_findings != expected_findings[expected_scenario]:
        raise AssertionError(f"Unexpected findings: {actual_findings}")
    if actual_valid_ids != expected_valid_ids[expected_scenario]:
        raise AssertionError(f"Unexpected valid payments: {actual_valid_ids}")
    print(f"Scenario check passed: {expected_scenario}.")
print("Views show current results. Permanent result history comes in the Gold step.")
