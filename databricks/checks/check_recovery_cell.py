# Paste into a NEW Python cell at the bottom of 01_ingest_bronze.
# First run that notebook's existing cells with the 02_updated input paths.
# Aim: exercise the actual loader after a controlled stop between source writes.
# All test writes go to three NEW tables in workspace.reconciliation_checks.
# Existing uploaded 01_initial files are reused. No extra upload is required.
# Run serially. Each execution keeps its small test tables as evidence.

import hashlib
import json
from pathlib import Path
import re
from uuid import uuid4


class SimulatedBankWriteFailure(RuntimeError):
    """A deliberate driver exception before dispatching the bank MERGE."""


class RecoverySparkProxy:
    """Route the real loader to private Delta targets; block one bank write.

    Reads, DataFrame construction and SQL still execute on real Spark/Delta.
    This adapter does not emulate database writes or modify the loader function.
    """
    def __init__(self, delegate, table_mapping):
        required = {
            "workspace.reconciliation_bronze.ingestion_batches",
            "workspace.reconciliation_bronze.raw_payment_history",
            "workspace.reconciliation_bronze.raw_bank_history",
        }
        if set(table_mapping) != required:
            raise ValueError("Recovery routing must specify exactly the three Bronze tables.")
        tags = set()
        for original, target in table_mapping.items():
            stem = original.rsplit(".", 1)[-1]
            match = re.fullmatch(r"workspace\.reconciliation_checks\." + stem + r"_([0-9a-f]{12})", target)
            if not match:
                raise ValueError("A recovery target must be its own table in the checks schema.")
            tags.add(match[1])
        if len(tags) != 1:
            raise ValueError("All recovery targets must share the same test-run suffix.")
        self.delegate = delegate
        self.mapping = dict(table_mapping)
        self.block_bank_once = True
        self.blocked_bank_writes = 0

    def table(self, name):
        if name not in self.mapping:
            raise ValueError(f"Unexpected recovery table read: {name}")
        return self.delegate.table(self.mapping[name])

    def createDataFrame(self, *args, **kwargs):
        return self.delegate.createDataFrame(*args, **kwargs)

    def sql(self, query, *args, **kwargs):
        words = query.split()
        upper = [word.upper() for word in words]
        if upper[:2] == ["MERGE", "INTO"] and len(words) > 2:
            target = words[2].lower()
            is_bank_merge = target == "workspace.reconciliation_bronze.raw_bank_history"
        elif upper[:1] == ["UPDATE"] and len(words) > 1:
            target, is_bank_merge = words[1].lower(), False
        else:
            raise ValueError("Unexpected SQL operation in the recovery adapter.")
        if target not in self.mapping:
            raise ValueError("SQL target is outside the recovery mapping.")
        rewritten = query
        for original, private in self.mapping.items():
            rewritten = re.sub(re.escape(original) + r"(?![a-zA-Z0-9_])", private,
                               rewritten, flags=re.IGNORECASE)
        if "workspace.reconciliation_bronze" in rewritten.lower():
            raise ValueError("Unmapped Bronze reference; refusing to execute SQL.")
        if is_bank_merge and self.block_bank_once:
            self.block_bank_once = False
            self.blocked_bank_writes += 1
            raise SimulatedBankWriteFailure("Bank write deliberately blocked for recovery verification.")
        return self.delegate.sql(rewritten, *args, **kwargs)


def recovery_contents(table):
    rows = spark.table(table).limit(1001).collect()
    if len(rows) > 1000:
        raise ValueError("This learning check supports at most 1,000 rows per table.")
    encoded = sorted(json.dumps(row.asDict(recursive=True), sort_keys=True,
                                default=str, separators=(",", ":")) for row in rows)
    return {"count": len(rows), "fingerprint": hashlib.sha256("\n".join(encoded).encode()).hexdigest()}


if not all(callable(globals().get(name)) for name in
           ("capture_source", "describe_bronze_pair", "load_bronze_pair")):
    raise RuntimeError("Run the existing cells of 01_ingest_bronze first, keeping the 02_updated paths.")

recovery_tag = uuid4().hex[:12]
production_tables = (
    "workspace.reconciliation_bronze.ingestion_batches",
    "workspace.reconciliation_bronze.raw_payment_history",
    "workspace.reconciliation_bronze.raw_bank_history",
    "workspace.reconciliation_gold.validation_run_history",
)
production_before_recovery = {table: recovery_contents(table) for table in production_tables}
recovery_mapping = {
    original: f"workspace.reconciliation_checks.{original.rsplit('.', 1)[-1]}_{recovery_tag}"
    for original in production_tables[:3]
}

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.reconciliation_checks")
for original, target in recovery_mapping.items():
    # Copy only the schema; none of the working data is copied or modified.
    (spark.table(original).limit(0).write.format("delta")
        .mode("error").saveAsTable(target))
recovery_spark = RecoverySparkProxy(spark, recovery_mapping)

recovery_folder = Path("/Volumes/workspace/reconciliation_bronze/incoming_files/01_initial")
metadata = {"batch_id", "source_row_number", "imported_at"}
recovery_payment = capture_source(recovery_folder / "payments.csv", [
    c for c in recovery_spark.table(production_tables[1]).columns if c not in metadata
])
recovery_bank = capture_source(recovery_folder / "bank.csv", [
    c for c in recovery_spark.table(production_tables[2]).columns if c not in metadata
])
recovery_batch_id = describe_bronze_pair(recovery_payment, recovery_bank, "csv_pair_v1")["batch_id"]
if (len(recovery_payment["rows"]), len(recovery_bank["rows"])) != (5, 3):
    raise ValueError("The recovery check expects the unchanged 01_initial fixture (5/3 rows).")

try:
    load_bronze_pair(recovery_spark, recovery_payment, recovery_bank, "csv_pair_v1", recovery_batch_id)
except SimulatedBankWriteFailure:
    print("Simulated stop: bank write deliberately blocked.")
else:
    raise AssertionError("The intended bank-write interruption did not occur.")

recovery_registry = recovery_mapping[production_tables[0]]
recovery_payment_table = recovery_mapping[production_tables[1]]
recovery_bank_table = recovery_mapping[production_tables[2]]
failed_registration = spark.table(recovery_registry).collect()
if (len(failed_registration) != 1 or failed_registration[0]["status"] != "failed"
        or failed_registration[0]["loaded_at"] is not None
        or failed_registration[0]["last_error_type"] != "SimulatedBankWriteFailure"):
    raise AssertionError("The incomplete test delivery was not recorded as failed.")
partial_payments = recovery_contents(recovery_payment_table)
if partial_payments["count"] != 5 or recovery_contents(recovery_bank_table)["count"] != 0:
    raise AssertionError("Expected the controlled partial state: five payments and no bank rows.")
print("After stop: status=failed, payment rows=5, bank rows=0.")

recovery_result = load_bronze_pair(
    recovery_spark, recovery_payment, recovery_bank, "csv_pair_v1", recovery_batch_id
)
if (recovery_result["status"], recovery_result["payment_rows_added"],
        recovery_result["bank_rows_added"], recovery_result["reused"]) != ("ready", 0, 3, False):
    raise AssertionError(f"Unexpected recovery outcome: {recovery_result}")
if recovery_contents(recovery_payment_table) != partial_payments:
    raise AssertionError("Retry changed existing payment rows or their import timestamps.")
ready_registration = spark.table(recovery_registry).collect()
if (len(ready_registration) != 1 or ready_registration[0]["status"] != "ready"
        or ready_registration[0]["loaded_at"] is None
        or ready_registration[0]["last_error_type"] is not None
        or recovery_contents(recovery_bank_table)["count"] != 3):
    raise AssertionError("Recovered sources are not complete and ready.")
print("Retry: status=ready, payment rows added=0, bank rows added=3.")

complete_before_repeat = {table: recovery_contents(table) for table in recovery_mapping.values()}
repeat_recovery = load_bronze_pair(
    recovery_spark, recovery_payment, recovery_bank, "csv_pair_v1", recovery_batch_id
)
if (repeat_recovery["status"], repeat_recovery["payment_rows_added"],
        repeat_recovery["bank_rows_added"], repeat_recovery["reused"]) != ("ready", 0, 0, True):
    raise AssertionError(f"Unexpected repeat result: {repeat_recovery}")
if complete_before_repeat != {table: recovery_contents(table) for table in recovery_mapping.values()}:
    raise AssertionError("The completed repeat changed the isolated test contents.")
if recovery_spark.blocked_bank_writes != 1:
    raise AssertionError("Expected exactly one controlled interruption.")
if production_before_recovery != {table: recovery_contents(table) for table in production_tables}:
    raise AssertionError("Working project data or saved reports changed during the isolated check.")

print("Repeat: payment rows added=0, bank rows added=0.")
print("PASS: original payment rows preserved during retry.")
print("PASS: recovered test delivery is complete and repeatable.")
print("PASS: working Bronze data and saved report contents unchanged.")
print("Recovery evidence tables:")
for target in recovery_mapping.values():
    print(target)
