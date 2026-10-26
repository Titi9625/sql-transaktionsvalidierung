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


def parse_expected_scenario(value):
    label = value.strip()
    if label == "none":
        return None
    if label not in {"01_initial", "02_updated"}:
        raise ValueError("expected_scenario must be 01_initial, 02_updated or none.")
    return label

dbutils.widgets.text("expected_scenario", "01_initial", "Example expectations")
expected_scenario = parse_expected_scenario(dbutils.widgets.get("expected_scenario"))

dbutils.widgets.text("batch_id", "", "Delivery ID from Bronze")
selected_delivery = select_ready_delivery(spark, dbutils.widgets.get("batch_id"))
report_batch_id = selected_delivery["batch_id"]

# END JOB PARAMETERS

# Notebook: 04_report_gold | Python | Databricks Free Edition / serverless
# Aim: report the Silver validation results and save each checked result package.
# Creates two live reporting views, one Delta history table and four saved-result views.
# Does not change Bronze. Independent of variables in notebooks 00-03.
# This bounded learning prototype captures at most 10,000 detail/report rows per run.
# Run one pipeline at a time; no concurrent raw changes or view replacements.

summary_sql = """
WITH payment_counts AS (
    SELECT batch_id, COUNT(*) AS total_payment_records,
        COUNT(DISTINCT transaction_id) AS distinct_payment_count
    FROM workspace.reconciliation_silver.stg_payment_transactions GROUP BY batch_id
), bank_counts AS (
    SELECT batch_id, COUNT(*) AS total_bank_records
    FROM workspace.reconciliation_silver.stg_bank_transactions GROUP BY batch_id
), valid_counts AS (
    SELECT batch_id, COUNT(*) AS valid_records,
        COUNT(DISTINCT transaction_id) AS valid_payment_count
    FROM workspace.reconciliation_silver.valid_transactions GROUP BY batch_id
), error_counts AS (
    SELECT batch_id, COUNT(*) AS total_validation_errors,
        COUNT(DISTINCT CASE WHEN record_source = 'payment_provider' THEN transaction_id END)
            AS invalid_payment_transactions,
        SUM(CASE WHEN record_source = 'payment_provider' THEN 1 ELSE 0 END) AS payment_error_rows,
        SUM(CASE WHEN record_source = 'bank_export' THEN 1 ELSE 0 END) AS bank_error_rows,
        SUM(CASE WHEN error_code = 'duplicate_transaction_id' THEN 1 ELSE 0 END) AS duplicate_errors,
        SUM(CASE WHEN error_code = 'missing_amount' THEN 1 ELSE 0 END) AS missing_value_errors,
        SUM(CASE WHEN error_code = 'invalid_currency' THEN 1 ELSE 0 END) AS currency_errors,
        SUM(CASE WHEN error_code = 'pending_transaction' THEN 1 ELSE 0 END) AS pending_errors,
        SUM(CASE WHEN error_code = 'refund_transaction' THEN 1 ELSE 0 END) AS refund_errors,
        SUM(CASE WHEN error_code = 'amount_mismatch' THEN 1 ELSE 0 END) AS amount_mismatch_errors,
        SUM(CASE WHEN error_code = 'missing_bank_match' THEN 1 ELSE 0 END) AS missing_bank_match_errors,
        SUM(CASE WHEN error_code = 'unmatched_bank_transaction' THEN 1 ELSE 0 END) AS unmatched_bank_transactions,
        SUM(CASE WHEN error_code = 'bank_reference_id_not_extractable' THEN 1 ELSE 0 END)
            AS bank_reference_id_not_extractable_errors
    FROM workspace.reconciliation_silver.invalid_transactions GROUP BY batch_id
)
SELECT r.batch_id, CURRENT_DATE AS validation_date,
    COALESCE(p.total_payment_records, 0) AS total_payment_records,
    COALESCE(b.total_bank_records, 0) AS total_bank_records,
    COALESCE(p.distinct_payment_count, 0) AS distinct_payment_count,
    COALESCE(v.valid_records, 0) AS valid_records,
    COALESCE(v.valid_payment_count, 0) AS valid_payment_count,
    COALESCE(e.invalid_payment_transactions, 0) AS invalid_payment_transactions,
    COALESCE(p.distinct_payment_count, 0) - COALESCE(v.valid_payment_count, 0)
        - COALESCE(e.invalid_payment_transactions, 0) AS unclassified_payment_count,
    CAST(ROUND(100.0 * COALESCE(v.valid_payment_count, 0)
        / NULLIF(p.distinct_payment_count, 0), 2) AS DECIMAL(7,2)) AS validation_pass_rate_pct,
    COALESCE(e.total_validation_errors, 0) AS total_validation_errors,
    COALESCE(e.payment_error_rows, 0) AS payment_error_rows,
    COALESCE(e.bank_error_rows, 0) AS bank_error_rows,
    COALESCE(e.duplicate_errors, 0) AS duplicate_errors,
    COALESCE(e.missing_value_errors, 0) AS missing_value_errors,
    COALESCE(e.currency_errors, 0) AS currency_errors,
    COALESCE(e.pending_errors, 0) AS pending_errors,
    COALESCE(e.refund_errors, 0) AS refund_errors,
    COALESCE(e.amount_mismatch_errors, 0) AS amount_mismatch_errors,
    COALESCE(e.missing_bank_match_errors, 0) AS missing_bank_match_errors,
    COALESCE(e.unmatched_bank_transactions, 0) AS unmatched_bank_transactions,
    COALESCE(e.bank_reference_id_not_extractable_errors, 0) AS bank_reference_id_not_extractable_errors
FROM workspace.reconciliation_bronze.ingestion_batches r
LEFT JOIN payment_counts p ON p.batch_id = r.batch_id
LEFT JOIN bank_counts b ON b.batch_id = r.batch_id
LEFT JOIN valid_counts v ON v.batch_id = r.batch_id
LEFT JOIN error_counts e ON e.batch_id = r.batch_id
WHERE r.status = 'ready'
"""

daily_sql = """
WITH payment_ids AS (
    SELECT batch_id, source_system, transaction_id,
        MIN(transaction_date) AS payment_date, COUNT(*) AS source_row_count
    FROM workspace.reconciliation_silver.stg_payment_transactions
    GROUP BY batch_id, source_system, transaction_id
), payment_errors AS (
    SELECT batch_id, transaction_id, COUNT(*) AS error_row_count
    FROM workspace.reconciliation_silver.invalid_transactions
    WHERE record_source = 'payment_provider' GROUP BY batch_id, transaction_id
), valid_ids AS (
    -- A payment ID is counted once even if several bank rows match it.
    SELECT batch_id, transaction_id, MAX(amount_net) AS amount_net
    FROM workspace.reconciliation_silver.valid_transactions GROUP BY batch_id, transaction_id
), daily AS (
    SELECT p.batch_id, p.payment_date, p.source_system,
        SUM(p.source_row_count) AS payment_row_count,
        COUNT(*) AS distinct_payment_count,
        SUM(CASE WHEN v.transaction_id IS NOT NULL THEN 1 ELSE 0 END) AS valid_payment_count,
        SUM(CASE WHEN e.transaction_id IS NOT NULL THEN 1 ELSE 0 END) AS flagged_payment_count,
        SUM(COALESCE(e.error_row_count, 0)) AS payment_error_row_count,
        SUM(CASE WHEN p.source_row_count > 1 THEN 1 ELSE 0 END) AS duplicate_payment_id_count,
        CAST(SUM(COALESCE(v.amount_net, 0)) AS DECIMAL(18,2)) AS valid_net_amount_eur
    FROM payment_ids p
    LEFT JOIN valid_ids v ON p.batch_id = v.batch_id AND p.transaction_id = v.transaction_id
    LEFT JOIN payment_errors e ON p.batch_id = e.batch_id AND p.transaction_id = e.transaction_id
    GROUP BY p.batch_id, p.payment_date, p.source_system
)
SELECT CONCAT(batch_id, ':', CAST(payment_date AS STRING), ':', source_system) AS reporting_key,
    batch_id, payment_date, source_system, payment_row_count, distinct_payment_count,
    valid_payment_count, flagged_payment_count, payment_error_row_count,
    duplicate_payment_id_count,
    CAST(ROUND(100.0 * valid_payment_count / NULLIF(distinct_payment_count, 0), 2)
        AS DECIMAL(7,2)) AS validation_pass_rate_pct,
    valid_net_amount_eur
FROM daily
"""

# COMMAND ----------

from collections import Counter
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
from uuid import uuid4
from pyspark.sql import functions as F


def check_report_package(package, expected_scenario=None):
    """Check the captured package before writing any historical result rows."""
    summary = package["summary"]
    valid = package["valid_payments"]
    findings = package["findings"]
    daily = package["daily_reports"]
    batch = package["batch_id"]
    if summary["batch_id"] != batch:
        raise ValueError("Summary belongs to a different delivery.")
    for rows in (valid, findings, daily):
        if any(r["batch_id"] != batch for r in rows):
            raise ValueError("Mixed delivery IDs in the result package.")
    if sum(map(len, (valid, findings, daily))) > 10000:
        raise ValueError("Result package exceeds the 10,000-row learning-demo limit.")
    if summary["valid_records"] != len(valid) or summary["total_validation_errors"] != len(findings):
        raise ValueError("Summary/detail counts differ.")
    valid_ids = {r["transaction_id"] for r in valid}
    flagged_ids = {r["transaction_id"] for r in findings if r["record_source"] == "payment_provider"}
    if valid_ids & flagged_ids:
        raise ValueError("A payment cannot be both accepted and flagged.")
    if len(valid_ids) != summary["valid_payment_count"]:
        raise ValueError("Accepted payment-ID count differs.")
    if len(flagged_ids) != summary["invalid_payment_transactions"]:
        raise ValueError("Flagged payment-ID count differs.")
    source_counts = Counter(r["record_source"] for r in findings)
    if source_counts["payment_provider"] != summary["payment_error_rows"]:
        raise ValueError("Payment finding count differs.")
    if source_counts["bank_export"] != summary["bank_error_rows"]:
        raise ValueError("Bank finding count differs.")
    for field, expected in (
        ("payment_row_count", summary["total_payment_records"]),
        ("distinct_payment_count", summary["distinct_payment_count"]),
        ("valid_payment_count", summary["valid_payment_count"]),
        ("flagged_payment_count", summary["invalid_payment_transactions"]),
        ("payment_error_row_count", summary["payment_error_rows"]),
    ):
        if sum(r[field] for r in daily) != expected:
            raise ValueError(f"Daily totals disagree with summary: {field}.")
    total = summary["distinct_payment_count"]
    if summary["unclassified_payment_count"] != total - len(valid_ids) - len(flagged_ids):
        raise ValueError("Unclassified payment-ID count differs.")
    rate = ((Decimal(100) * len(valid_ids) / total)
            .quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)) if total else None
    if summary["validation_pass_rate_pct"] != rate:
        raise ValueError("Pass rate does not use distinct payment IDs.")
    accepted_net_by_id = {}
    for row in valid:
        tid, amount = row["transaction_id"], row["amount_net"]
        if tid in accepted_net_by_id and accepted_net_by_id[tid] != amount:
            raise ValueError("One accepted payment ID has inconsistent net amounts.")
        accepted_net_by_id[tid] = amount
    if sum(r["valid_net_amount_eur"] for r in daily) != sum(accepted_net_by_id.values()):
        raise ValueError("Daily accepted net amounts are inconsistent or double-counted.")
    for row in daily:
        expected_rate = ((Decimal(100) * row["valid_payment_count"] / row["distinct_payment_count"])
                         .quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        if row["validation_pass_rate_pct"] != expected_rate:
            raise ValueError("A daily pass rate has an incorrect denominator or rounding.")
    expected = {
        "01_initial": (5, 3, 4, 1, 3, 4, Decimal("25.00")),
        "02_updated": (5, 4, 4, 2, 2, 3, Decimal("50.00")),
    }
    if expected_scenario is not None:
        if expected_scenario not in expected:
            raise ValueError("Use scenario 01_initial, 02_updated or None.")
        actual = tuple(summary[k] for k in (
            "total_payment_records", "total_bank_records", "distinct_payment_count",
            "valid_payment_count", "invalid_payment_transactions", "total_validation_errors",
            "validation_pass_rate_pct",
        ))
        if actual != expected[expected_scenario]:
            raise ValueError(f"Unexpected scenario report: {actual}")


spark.sql("CREATE OR REPLACE VIEW workspace.reconciliation_gold.validation_summary AS " + summary_sql)
spark.sql("CREATE OR REPLACE VIEW workspace.reconciliation_gold.mart_daily_reconciliation AS " + daily_sql)

# Use the parameter none for your own files; it selects no fixed example answers.
# expected_scenario is read from the notebook/job parameter above.

definitions = {}
for name in (
    "workspace.reconciliation_silver.stg_payment_transactions",
    "workspace.reconciliation_silver.stg_bank_transactions",
    "workspace.reconciliation_silver.invalid_transactions",
    "workspace.reconciliation_silver.valid_transactions",
    "workspace.reconciliation_gold.validation_summary",
    "workspace.reconciliation_gold.mart_daily_reconciliation",
):
    definitions[name] = spark.sql(f"SHOW CREATE TABLE {name}").first()[0]
definition_fingerprint = hashlib.sha256(
    json.dumps(definitions, sort_keys=True).encode("utf-8")
).hexdigest()


def selected(table):
    return spark.table(table).where(F.col("batch_id") == F.lit(report_batch_id))


def packed_rows(table, label):
    frame = selected(table)
    # Keep collection bounded even if unexpected joins generate many findings.
    return frame.limit(10001).agg(
        F.collect_list(F.struct(*[F.col(c) for c in frame.columns])).alias(label)
    )


summary_frame = selected("workspace.reconciliation_gold.validation_summary")
packet_frame = summary_frame.select(
    F.struct(*[F.col(c) for c in summary_frame.columns]).alias("summary")
)
for table, label in (
    ("workspace.reconciliation_silver.valid_transactions", "valid_payments"),
    ("workspace.reconciliation_silver.invalid_transactions", "findings"),
    ("workspace.reconciliation_gold.mart_daily_reconciliation", "daily_reports"),
):
    packet_frame = packet_frame.crossJoin(packed_rows(table, label))
packet_frame = (packet_frame
    .withColumn("validation_run_id", F.lit(str(uuid4())))
    .withColumn("batch_id", F.lit(report_batch_id))
    .withColumn("captured_at", F.current_timestamp())
    .withColumn("definition_fingerprint", F.lit(definition_fingerprint))
    .withColumn("snapshot_format_version", F.lit("gold_snapshot_v1")))

# Capture once, check that exact package, then write those same values.
# Bounded driver capture is deliberate for these small synthetic fixtures.
captured = packet_frame.collect()
if len(captured) != 1:
    raise ValueError("Expected exactly one result package for this delivery.")
check_report_package(captured[0].asDict(recursive=True), expected_scenario)
history = "workspace.reconciliation_gold.validation_run_history"
(spark.createDataFrame(captured, schema=packet_frame.schema).write
    .format("delta").mode("append").saveAsTable(history))
saved_run_id = captured[0]["validation_run_id"]

# One complete package is appended to ONE Delta table in one write.
# Rerunning creates a new report run; it does not import the CSVs again.
# This is not a multi-table transaction or an immutable administrator-proof audit log.

# COMMAND ----------

spark.sql("""
    CREATE OR REPLACE VIEW workspace.reconciliation_gold.saved_validation_summaries AS
    SELECT h.validation_run_id, h.captured_at, h.definition_fingerprint, h.summary.*
    FROM workspace.reconciliation_gold.validation_run_history h
""")
for view, collection in (
    ("saved_valid_payments", "valid_payments"),
    ("saved_validation_findings", "findings"),
    ("saved_daily_reports", "daily_reports"),
):
    spark.sql(f"""
        CREATE OR REPLACE VIEW workspace.reconciliation_gold.{view} AS
        SELECT h.validation_run_id, h.captured_at, item.*
        FROM workspace.reconciliation_gold.validation_run_history h
        LATERAL VIEW EXPLODE(h.{collection}) expanded AS item
    """)

read_back = spark.table(history).where(F.col("validation_run_id") == saved_run_id).collect()
if len(read_back) != 1:
    raise ValueError("Saved result package could not be uniquely read back.")
if read_back[0].asDict(recursive=True) != captured[0].asDict(recursive=True):
    raise ValueError("Saved result package differs from the checked package.")

print("Gold report checked and saved successfully.")
print(f"Delivery ID: {report_batch_id}")
print(f"Saved report run ID: {saved_run_id}")
print("Earlier report runs were not overwritten.")
display(spark.sql("""
    SELECT total_payment_records, total_bank_records, distinct_payment_count,
        valid_payment_count, invalid_payment_transactions AS flagged_payment_ids,
        total_validation_errors AS finding_rows, validation_pass_rate_pct
    FROM workspace.reconciliation_gold.saved_validation_summaries
    WHERE validation_run_id = :run
""", args={"run": saved_run_id}))
display(spark.sql("""
    SELECT payment_date, distinct_payment_count, valid_payment_count,
        flagged_payment_count, payment_error_row_count, duplicate_payment_id_count,
        validation_pass_rate_pct, valid_net_amount_eur
    FROM workspace.reconciliation_gold.saved_daily_reports
    WHERE validation_run_id = :run
""", args={"run": saved_run_id}))
print("Saved runs for this delivery:",
      spark.table(history).where(F.col("batch_id") == report_batch_id).count())

# Publish the report ID only after complete saved-package read-back succeeded.
dbutils.jobs.taskValues.set(key="validation_run_id", value=saved_run_id)
dbutils.jobs.taskValues.set(key="batch_id", value=report_batch_id)
