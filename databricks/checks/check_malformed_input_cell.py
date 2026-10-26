# Paste into a NEW Python cell at the bottom of 01_ingest_bronze.
# First run that notebook's existing four cells with its 02_updated paths.
# Aim: reject the broken CSV and compare all four physical tables before/after.
# Required uploads: 01_initial, 02_updated and 03_broken_format file pairs.
# Do not run another loader, report writer or editor during this check.
# This is a notebook-cell check, not a standalone local Python program.

from collections import Counter
import hashlib
import json
from pathlib import Path

if not all(callable(globals().get(name)) for name in ("capture_source", "load_bronze_pair")):
    raise RuntimeError("Run the existing four cells of 01_ingest_bronze with 02_updated first.")

check_volume = Path("/Volumes/workspace/reconciliation_bronze/incoming_files")
check_tables = (
    "workspace.reconciliation_bronze.ingestion_batches",
    "workspace.reconciliation_bronze.raw_payment_history",
    "workspace.reconciliation_bronze.raw_bank_history",
    "workspace.reconciliation_gold.validation_run_history",
)


def check_pair_id(folder):
    identity = {
        "contract": "csv_pair_v1",
        "payment": hashlib.sha256((folder / "payments.csv").read_bytes()).hexdigest(),
        "bank": hashlib.sha256((folder / "bank.csv").read_bytes()).hexdigest(),
    }
    return hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def check_table_contents():
    result = {}
    for table in check_tables:
        rows = spark.table(table).limit(1001).collect()
        if len(rows) > 1000:
            raise ValueError("This small-fixture check supports at most 1,000 rows per table.")
        records = [row.asDict(recursive=True) for row in rows]
        encoded = sorted(json.dumps(record, sort_keys=True, default=str, separators=(",", ":"))
                         for record in records)
        result[table] = {
            "count": len(records),
            "by_batch": dict(Counter(record["batch_id"] for record in records)),
            "fingerprint": hashlib.sha256("\n".join(encoded).encode()).hexdigest(),
        }
    return result


initial_check_id = check_pair_id(check_volume / "01_initial")
updated_check_id = check_pair_id(check_volume / "02_updated")
broken_check_folder = check_volume / "03_broken_format"
broken_check_id = check_pair_id(broken_check_folder)

excluded = {"batch_id", "source_row_number", "imported_at"}
check_payment_columns = [c for c in spark.table(check_tables[1]).columns if c not in excluded]
check_bank_columns = [c for c in spark.table(check_tables[2]).columns if c not in excluded]
broken_check_payment = capture_source(broken_check_folder / "payments.csv", check_payment_columns)
broken_check_bank = capture_source(broken_check_folder / "bank.csv", check_bank_columns)

before_bad_input = check_table_contents()
# This also checks that repeating Gold did not add another copy of these raw rows.
for label, delivery, expected in (
    ("Initial", initial_check_id, (5, 3)),
    ("Updated", updated_check_id, (5, 4)),
):
    actual = tuple(before_bad_input[table]["by_batch"].get(delivery, 0) for table in check_tables[1:3])
    if actual != expected:
        raise AssertionError(f"Unexpected {label.lower()} source-history counts: {actual}")
    print(f"{label} raw rows checked: {actual[0]} payment / {actual[1]} bank.")
if any(state["by_batch"].get(broken_check_id, 0) for state in before_bad_input.values()):
    raise AssertionError("The broken pair already has stored records; inspect them before proceeding.")

try:
    load_bronze_pair(spark, broken_check_payment, broken_check_bank, "csv_pair_v1", broken_check_id)
except ValueError as error:
    expected_error = "bank.csv: data row 4: amount must be numeric with at most two decimal places"
    if str(error) != expected_error:
        raise AssertionError(f"Unexpected rejection reason: {error}") from error
    print(f"Expected rejection: {error}")
else:
    raise AssertionError("Broken input was accepted unexpectedly. Stop and inspect the stored data.")

after_bad_input = check_table_contents()
if before_bad_input != after_bad_input:
    changed = [table for table in check_tables if before_bad_input[table] != after_bad_input[table]]
    raise AssertionError(f"Stored data changed during malformed-input rejection: {changed}")

print("PASS: broken input rejected before loading.")
print("PASS: batch registry, raw histories and saved report contents unchanged.")
display(spark.createDataFrame([
    {"table": table.rsplit(".", 1)[-1], "rows_before": before_bad_input[table]["count"],
     "rows_after": after_bad_input[table]["count"], "contents_unchanged": True}
    for table in check_tables
]))
