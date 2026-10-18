# Databricks notebook source
# Cell 1 - Read and preview the initial CSV pair.
# Aim: verify the uploaded paths, source columns and row counts before writing tables.
# Expected for the 01_initial fixture: 5 payment rows and 3 bank rows.
# This cell performs no table writes and does not implement the business rules
# or the complete strict CSV contract used by the PostgreSQL implementation.

input_folder = "/Volumes/workspace/reconciliation_bronze/incoming_files/01_initial"

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

input_folder = Path("/Volumes/workspace/reconciliation_bronze/incoming_files/01_initial")
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
