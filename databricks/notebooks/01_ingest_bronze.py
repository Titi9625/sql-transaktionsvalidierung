# Databricks notebook source
# Job parameters: values supplied by a job, or entered above the notebook.

def parse_input_folder(value):
    """Accept a delivery folder inside this project's existing input volume."""
    folder = value.strip().rstrip("/")
    root = "/Volumes/workspace/reconciliation_bronze/incoming_files"
    if (not folder.startswith(root + "/")
            or any(part in {".", "..", ""} for part in folder[len(root) + 1:].split("/"))
            or "\\" in folder or any(ord(char) < 32 for char in folder)):
        raise ValueError("input_folder must be a delivery directory inside " + root)
    return folder

dbutils.widgets.text("input_folder",
    "/Volumes/workspace/reconciliation_bronze/incoming_files/01_initial", "CSV delivery folder")
configured_input_folder = parse_input_folder(dbutils.widgets.get("input_folder"))

# END JOB PARAMETERS

# Cell 1 - Read and preview the initial CSV pair.
# Aim: verify the uploaded paths, source columns and row counts before writing tables.
# Expected for the 01_initial fixture: 5 payment rows and 3 bank rows.
# This cell performs no table writes and does not implement the business rules
# or the complete strict CSV contract used by the PostgreSQL implementation.

input_folder = configured_input_folder

# Read each file separately, using the first line as column names.
# Preserve values as strings during this raw-input preview.
payments_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("mode", "FAILFAST")
    .csv(f"{input_folder}/payments.csv")
)

bank_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("mode", "FAILFAST")
    .csv(f"{input_folder}/bank.csv")
)

print(f"Payment rows read: {payments_df.count()}")
print(f"Bank rows read: {bank_df.count()}")

display(payments_df)
display(bank_df)

# COMMAND ----------

# Cell 2 - Create the empty Bronze Delta tables.
# Aim: define permanent destinations for source rows and input-pair metadata.
# Expected on first execution: three empty tables; no CSV rows inserted yet.
# Each source row will later be identified by batch_id and source_row_number.
# Provider transaction IDs deliberately have no uniqueness constraint.
# Creating the registry alone does not implement repeat recognition or loading.

spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.reconciliation_bronze.ingestion_batches (
        batch_id STRING NOT NULL,
        contract_version STRING NOT NULL,
        pair_fingerprint STRING NOT NULL,
        payment_fingerprint STRING NOT NULL,
        bank_fingerprint STRING NOT NULL,
        payment_file_name STRING NOT NULL,
        bank_file_name STRING NOT NULL,
        payment_row_count BIGINT NOT NULL,
        bank_row_count BIGINT NOT NULL,
        status STRING NOT NULL,
        registered_at TIMESTAMP,
        loaded_at TIMESTAMP,
        last_error_type STRING
    ) USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.reconciliation_bronze.raw_payment_history (
        batch_id STRING NOT NULL,
        source_row_number BIGINT NOT NULL,
        provider_transaction_id STRING,
        created STRING,
        currency STRING,
        amount STRING,
        fee STRING,
        net STRING,
        status STRING,
        type STRING,
        reporting_category STRING,
        source STRING,
        imported_at TIMESTAMP
    ) USING DELTA
""")

spark.sql("""
    CREATE TABLE IF NOT EXISTS workspace.reconciliation_bronze.raw_bank_history (
        batch_id STRING NOT NULL,
        source_row_number BIGINT NOT NULL,
        bank_transaction_id STRING,
        booking_date STRING,
        reference_text STRING,
        amount STRING,
        currency STRING,
        counterparty STRING,
        imported_at TIMESTAMP
    ) USING DELTA
""")

display(spark.sql("SHOW TABLES IN workspace.reconciliation_bronze"))

# COMMAND ----------

# Cell 3 of 01_ingest_bronze - Capture and identify the input pair.
# Aim: preserve source row positions and fingerprint the same bytes we parse.
# This preparation cell does not write to Delta tables or run business rules.
import csv
import hashlib
import io
import json
from pathlib import Path

input_folder = Path(configured_input_folder)
bronze = "workspace.reconciliation_bronze"


def capture_source(path, expected_columns):
    content = path.read_bytes()
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig"), newline=""), strict=True)
    header = reader.fieldnames or []
    if len(header) != len(expected_columns) or set(header) != set(expected_columns):
        raise ValueError(f"{path.name}: unexpected CSV columns")
    rows = []
    for number, row in enumerate(reader, start=1):
        if None in row or any(value is None for value in row.values()):
            raise ValueError(f"{path.name}: data row {number} has the wrong number of fields")
        record = {column: row[column].strip() or None for column in expected_columns}
        record["source_row_number"] = number
        rows.append(record)
    if not rows:
        raise ValueError(f"{path.name}: empty snapshots are not supported")
    return {"file_name": path.name, "rows": rows,
            "fingerprint": hashlib.sha256(content).hexdigest()}


metadata_columns = {"batch_id", "source_row_number", "imported_at"}
payment_columns = [c for c in spark.table(f"{bronze}.raw_payment_history").columns
                   if c not in metadata_columns]
bank_columns = [c for c in spark.table(f"{bronze}.raw_bank_history").columns
                if c not in metadata_columns]

payment_input = capture_source(input_folder / "payments.csv", payment_columns)
bank_input = capture_source(input_folder / "bank.csv", bank_columns)

contract_version = "csv_pair_v1"
identity = {"contract": contract_version,
            "payment": payment_input["fingerprint"], "bank": bank_input["fingerprint"]}
pair_fingerprint = hashlib.sha256(
    json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
).hexdigest()

# Use the deterministic pair fingerprint as the Databricks batch ID.
batch_id = pair_fingerprint
print(f"Prepared payment rows: {len(payment_input['rows'])}")
print(f"Prepared bank rows: {len(bank_input['rows'])}")
print(f"Batch ID: {batch_id}")
print("Input pair prepared. No Delta rows written yet.")

# COMMAND ----------

# Cell 4 of 01_ingest_bronze: validate input formats and load the prepared pair.
# Requires Cell 3's payment_input, bank_input, contract_version and batch_id.
# First run: 5 payment rows + 3 bank rows, one batch with status ready.
# Repeat: 0 additional raw rows. Existing rows/timestamps remain unchanged.
# Run one loader at a time. This is not a concurrent multi-writer design.
# Delta writes commit per table. Failed attempts may leave partial raw rows;
# only batches marked ready may be consumed by the later Silver pipeline.

from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import re


def validate_source_format(source, kind):
    """Check formats before any writes, keeping raw values as text or None."""
    if kind not in {"payment", "bank"}:
        raise ValueError("Unknown source kind")
    columns = (
        ("provider_transaction_id", "created", "currency", "amount", "fee", "net",
         "status", "type", "reporting_category", "source") if kind == "payment" else
        ("bank_transaction_id", "booking_date", "reference_text", "amount", "currency", "counterparty")
    )
    required = [columns[0], "currency"] + (["status", "type"] if kind == "payment" else [])
    date_column = "created" if kind == "payment" else "booking_date"
    money_columns = ["amount", "fee", "net"] if kind == "payment" else ["amount"]
    if not source["rows"]:
        raise ValueError("Empty snapshots are not supported")
    if not re.fullmatch(r"[0-9a-f]{64}", source["fingerprint"]):
        raise ValueError("Invalid file fingerprint; rerun Cell 3")
    for number, row in enumerate(source["rows"], 1):
        location = f"{source['file_name']}: data row {number}"
        if set(row) != {*columns, "source_row_number"}:
            raise ValueError(f"{location}: unexpected fields; rerun Cell 3")
        if type(row["source_row_number"]) is not int or row["source_row_number"] != number:
            raise ValueError(f"{location}: invalid source row position")
        if any(value is not None and not isinstance(value, str)
               for key, value in row.items() if key != "source_row_number"):
            raise ValueError(f"{location}: raw fields must remain text or None")
        for column in required:
            if row[column] is None or not row[column].strip():
                raise ValueError(f"{location}: {column} is required")
        value = row[date_column] or ""
        if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
            raise ValueError(f"{location}: {date_column} must use YYYY-MM-DD")
        date.fromisoformat(value)
        for column in money_columns:
            value = row[column]
            if value is None:
                continue  # A missing amount is a later business-rule finding.
            if kind == "payment":
                if not re.fullmatch(r"[+-]?[0-9]+", value) or not -2147483648 <= int(value) <= 2147483647:
                    raise ValueError(f"{location}: {column} must contain whole cents within the supported range")
            elif (not re.fullmatch(r"[+-]?[0-9]+(?:\.[0-9]{1,2})?", value)
                  or not Decimal("-9999999999.99") <= Decimal(value) <= Decimal("9999999999.99")):
                raise ValueError(f"{location}: {column} must be numeric with at most two decimal places")


def describe_bronze_pair(payment, bank, version):
    """Check both sources and reproduce Cell 3's content-based delivery label."""
    if version != "csv_pair_v1":
        raise ValueError("Unsupported input contract version")
    validate_source_format(payment, "payment")
    validate_source_format(bank, "bank")
    identity = {"contract": version, "payment": payment["fingerprint"], "bank": bank["fingerprint"]}
    fingerprint = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "batch_id": fingerprint, "contract_version": version, "pair_fingerprint": fingerprint,
        "payment_fingerprint": payment["fingerprint"], "bank_fingerprint": bank["fingerprint"],
        "payment_file_name": payment["file_name"], "bank_file_name": bank["file_name"],
        "payment_row_count": len(payment["rows"]), "bank_row_count": len(bank["rows"]),
    }


def load_bronze_pair(spark, payment, bank, version, expected_batch_id,
                     namespace="workspace.reconciliation_bronze"):
    """Preserve raw history; insert missing row positions and publish ready last.

    Retry a failed/incomplete pair serially. Correct existing rows are reused;
    unexpected or changed rows cause an error rather than an overwrite.
    This does not offer atomic rollback across the three Delta tables.
    """
    # Both sources must pass the format checks before any registration/writes.
    metadata = describe_bronze_pair(payment, bank, version)
    label = metadata["batch_id"]
    if label != expected_batch_id:
        raise ValueError("Prepared inputs changed; rerun Cell 3")
    if namespace != "workspace.reconciliation_bronze":
        raise ValueError("This learning loader targets the project Bronze schema")
    from pyspark.sql import functions as F
    registry = f"{namespace}.ingestion_batches"
    timestamp = datetime.now(timezone.utc)
    sources = [("raw_payment_history", payment), ("raw_bank_history", bank)]
    frames = []
    for table, source in sources:
        full_name = f"{namespace}.{table}"
        schema = spark.table(full_name).schema
        rows = [{**row, "batch_id": label, "imported_at": timestamp} for row in source["rows"]]
        frame = spark.createDataFrame(rows, schema=schema)
        columns = [column for column in frame.columns if column != "imported_at"]
        frames.append((full_name, frame, columns))

    def stored_rows(table, columns):
        return spark.table(table).filter(F.col("batch_id") == F.lit(label)).select(*columns)

    def require_exact(table, frame, columns):
        actual, expected = stored_rows(table, columns), frame.select(*columns)
        if (actual.exceptAll(expected).limit(1).count()
                or expected.exceptAll(actual).limit(1).count()):
            raise ValueError(f"{table}: stored rows do not match the prepared input")

    def read_registration():
        matches = spark.table(registry).filter(
            (F.col("pair_fingerprint") == F.lit(label)) | (F.col("batch_id") == F.lit(label))
        ).collect()
        if len(matches) > 1:
            raise ValueError("More than one registry row exists for this pair")
        return matches[0].asDict() if matches else None

    existing = read_registration()
    if existing is None:
        registration = {**metadata, "status": "registered", "registered_at": timestamp,
                        "loaded_at": None, "last_error_type": None}
        spark.createDataFrame([registration], schema=spark.table(registry).schema).createOrReplaceTempView("_bronze_new_batch")
        spark.sql(f"""
            MERGE INTO {registry} AS target USING _bronze_new_batch AS incoming
            ON target.pair_fingerprint = incoming.pair_fingerprint
            WHEN NOT MATCHED THEN INSERT *
        """)
        existing = read_registration()
    # Filenames are descriptive: renaming the same bytes must still reuse a batch.
    for key in metadata.keys() - {"payment_file_name", "bank_file_name"}:
        if existing is None or existing[key] != metadata[key]:
            raise ValueError("Existing registry metadata differs from the prepared pair")
    if existing["status"] == "ready":
        for table, frame, columns in frames:
            require_exact(table, frame, columns)
        return {"batch_id": label, "status": "ready", "payment_rows_added": 0,
                "bank_rows_added": 0, "reused": True}
    if existing["status"] not in {"registered", "loading", "failed"}:
        raise ValueError("Unexpected batch loading status")

    added = []
    try:
        spark.sql(f"""UPDATE {registry} SET status='loading',loaded_at=NULL,last_error_type=NULL
            WHERE batch_id=:batch""", args={"batch": label})
        for table, frame, columns in frames:
            actual, expected = stored_rows(table, columns), frame.select(*columns)
            if actual.exceptAll(expected).limit(1).count():
                raise ValueError(f"{table}: unexpected existing records; no overwrite performed")
            added.append(expected.exceptAll(actual).count())
            frame.createOrReplaceTempView("_bronze_source_rows")
            spark.sql(f"""
                MERGE INTO {table} AS target USING _bronze_source_rows AS incoming
                ON target.batch_id = incoming.batch_id
                   AND target.source_row_number = incoming.source_row_number
                WHEN NOT MATCHED THEN INSERT *
            """)
            require_exact(table, frame, columns)
        # Recheck both sources before making this batch visible as ready.
        for table, frame, columns in frames:
            require_exact(table, frame, columns)
        spark.sql(f"""UPDATE {registry} SET status='ready',loaded_at=current_timestamp(),last_error_type=NULL
            WHERE batch_id=:batch""", args={"batch": label})
    except Exception as error:
        try:
            spark.sql(f"""UPDATE {registry} SET status='failed',last_error_type=:error
                WHERE batch_id=:batch AND status <> 'ready'""",
                args={"batch": label, "error": type(error).__name__})
        except Exception:
            pass  # Preserve the original load failure if the registry is unavailable.
        raise
    return {"batch_id": label, "status": "ready", "payment_rows_added": added[0],
            "bank_rows_added": added[1], "reused": False}


# Execute the loader using the input pair prepared in Cell 3.
load_result = load_bronze_pair(
    spark, payment_input, bank_input, contract_version, batch_id
)
print("Same delivery already loaded." if load_result["reused"] else "Delivery loaded successfully.")
print(f"Payment rows added: {load_result['payment_rows_added']}")
print(f"Bank rows added: {load_result['bank_rows_added']}")
print(f"Status: {load_result['status']}")
display(spark.sql("""
    SELECT batch_id, status, payment_row_count, bank_row_count
    FROM workspace.reconciliation_bronze.ingestion_batches
    WHERE batch_id=:batch
""", args={"batch": batch_id}))

# Publish only after loading and verification succeeded. In interactive runs
# taskValues.set has no job to update; in a job it passes the exact ID onward.
dbutils.jobs.taskValues.set(key="batch_id", value=load_result["batch_id"])
