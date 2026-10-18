# Databricks payment-reconciliation extension

Work in progress: a second implementation of the existing PostgreSQL/dbt project,
using Databricks Free Edition, PySpark, SQL and Delta tables. All inputs are
synthetic payment-provider and bank exports.

## Current checkpoint: initial Bronze load

Verified interactively in Databricks Free Edition on serverless compute:

| Check | Observed result |
|---|---|
| Project setup | Bronze, Silver and Gold schemas; managed `incoming_files` volume |
| Initial input preview | 5 payment rows and 3 bank rows |
| First Bronze load | 5 payment rows added, 3 bank rows added, batch `ready` |
| Repeat load | Same batch recognised; 0 payment rows and 0 bank rows added |

`txn_103` remains two separate source records (positions 3 and 4). The loader
identifies a stored source record by its batch ID and source record position,
not by the provider transaction ID. Detecting duplicate payment IDs remains a
later business-validation step.

The initial pair fingerprint/batch ID is
`8199572c3db5105dd6a290e6fc969eea50df9f896301a23881f6febbfe98936d` for the uploaded bytes.
Changing bytes, including line endings, can produce a different ID. PostgreSQL
stores the pair fingerprint separately from its generated UUID batch ID; this
Databricks implementation uses the pair fingerprint itself as the batch ID.

## What the loader does

1. Capture both CSVs, preserve their record positions and fingerprint their bytes.
2. Check columns, required fields, dates and exact amount formats before writes.
   Source amounts remain text in Bronze. Missing amounts stay missing; zero
   stays zero. Pending payments, refunds, non-EUR currencies and duplicate IDs
   remain available for later business checks.
3. Register the pair in `ingestion_batches`.
4. Insert missing source record positions into the two raw-history Delta tables.
   Unexpected or changed existing records cause an error rather than an overwrite.
5. Verify both stored sources before setting the batch to `ready`.
6. On a repeat of a ready pair, verify its existing records and skip new inserts.

The source file, dataset/DataFrame and Delta table have different roles:
uploaded CSVs are the original files; DataFrames describe data used during
processing; Delta tables persist the loaded records.

## Source notebooks and execution order

| File | Cells and purpose |
|---|---|
| [00_setup_payment_reconciliation.py](notebooks/00_setup_payment_reconciliation.py) | Inspect the environment; create schemas and a managed input-file volume. |
| [01_ingest_bronze.py](notebooks/01_ingest_bronze.py) | 1: preview inputs; 2: create tables; 3: prepare the pair; 4: validate formats and load Bronze. |

These are Databricks Python source notebooks. `# COMMAND ----------` separates
cells. Import them as notebooks; they use Databricks-provided `spark` and
`display` objects and are not standalone local Python programs.

To reproduce:

1. Run both setup-notebook cells.
2. Create the directory `01_initial` inside
   `workspace.reconciliation_bronze.incoming_files`.
3. Upload `testdaten/batch_scenarios/01_initial/payments.csv` and `bank.csv` to:
   `/Volumes/workspace/reconciliation_bronze/incoming_files/01_initial/`.
4. Run all four ingestion-notebook cells in order. Expect 5/3 inserted rows and
   status `ready`.
5. Run Cell 4 again. Expect 0/0 inserted rows, with the same stored delivery.

After a notebook-session restart, rerun Cell 3 before Cell 4. The prepared Python
variables are session state; uploaded files and Delta tables persist separately.
`ready` means both sources were loaded, not that every payment is valid.

## Tests and evidence

[test_databricks_input_checks.py](../tests/test_databricks_input_checks.py) contains
seven portable tests of the actual notebook's input/helper functions. It checks
initial and updated pair identities, renamed inputs, duplicate record positions,
invalid dates/amounts/ranges, missing versus zero amounts, and rejection of the
malformed bank fixture before Spark is accessed.

Run the portable checks without a Databricks account or PostgreSQL connection:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_databricks_input_checks.py -v
```

The test loader extracts imports and function definitions from the source
notebook without running its top-level Spark cells. These tests do not execute
Delta `MERGE` or prove cloud execution. The cloud evidence for this checkpoint
is the manually verified first load and repeat. The existing CI runs the portable
tests as part of Python discovery on pull requests to `main`; the branch name
`databricks-extension` does not itself trigger the current push workflow.

## Boundaries and remaining work

- Delta commits occur per table. This loader does not provide PostgreSQL-style
  rollback across both raw tables and the registry. An incomplete/failed attempt
  may leave partial rows. It is not published as ready; a serial retry inserts
  missing positions and checks both sources. **Live failure/retry injection is
  not yet verified.** Silver consumers must select only ready batches.
- Run one loader at a time. Registry and source keys are checked in code rather
  than protected by enforced unique constraints. Concurrent multi-writer use is
  not supported by this checkpoint.
- The notebook captures each small file in its Python process. This preserves
  record positions and ties fingerprints to the exact parsed bytes, but is not
  distributed large-file ingestion or a scale benchmark.
- Updated snapshot loading, live malformed-file rejection and saved-result
  history still need cloud verification. The portable checks cover formats and
  identity only.
- Silver preparation and the nine validation rules, Gold reports, and a
  Databricks job are the next implementation milestones. The PostgreSQL/dbt
  version remains the complete regression reference.
