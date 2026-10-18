# Databricks payment-reconciliation extension

Work in progress: a second implementation of the existing PostgreSQL/dbt project,
using Databricks Free Edition, PySpark, SQL and Delta tables. Inputs are synthetic
payment-provider and bank exports, not real customer or financial data.

## Current checkpoint

Verified interactively in Databricks:

- The setup notebook runs on serverless compute in the `workspace` catalog.
- Three schemas exist: `reconciliation_bronze`, `reconciliation_silver`,
  `reconciliation_gold`.
- The Bronze `incoming_files` volume holds the initial CSV pair in `01_initial/`.
- The ingestion notebook previews 5 payment rows and 3 bank rows, preserving both
  copies of `txn_103`.
- Three empty Delta tables exist: `ingestion_batches`, `raw_payment_history`,
  `raw_bank_history`.
- Input preparation preserves source record positions and calculates a file-pair
  fingerprint. The initial pair fingerprint is
  `8199572c3db5105dd6a290e6fc969eea50df9f896301a23881f6febbfe98936d`.

**No transaction rows have been inserted into these Delta tables yet.**
CSV structure checks currently cover column names, field counts and nonempty
snapshots. Amount/date-format checks, repeat-safe loading, validation rules,
reporting and automated jobs are not yet implemented in this extension.

The existing PostgreSQL/dbt implementation remains the regression reference;
its successful CI does not establish that these cloud notebooks execute.

## Source notebooks

| File | Cells and purpose |
|---|---|
| [00_setup_payment_reconciliation.py](notebooks/00_setup_payment_reconciliation.py) | Inspect the environment; create project schemas and the input-file volume. |
| [01_ingest_bronze.py](notebooks/01_ingest_bronze.py) | Preview inputs; create empty Bronze tables; prepare captured source rows and pair identity. |

These are Databricks Python source notebooks. The `# COMMAND ----------` lines
separate cells. Import them into Databricks as notebooks; they require the
Databricks-provided `spark` and `display` objects and are not standalone local
Python programs.

## Reproduce this checkpoint

1. Run both cells of `00_setup_payment_reconciliation` in order.
2. In the volume `workspace.reconciliation_bronze.incoming_files`, create the
   directory `01_initial`.
3. Upload the two files from the repository's
   `testdaten/batch_scenarios/01_initial/` directory to:
   `/Volumes/workspace/reconciliation_bronze/incoming_files/01_initial/`.
4. Run the first three cells of `01_ingest_bronze` in order.

Bronze keeps incoming records; Silver will prepare and validate them; Gold will
provide reporting outputs. A batch ID labels one payment/bank file pair. The
Databricks implementation uses its pair fingerprint as that label; PostgreSQL
stores the pair fingerprint separately from its generated UUID batch ID.

## Next milestones

1. Load both sources, check repeat behaviour and handle incomplete attempts.
2. Implement comparable Silver fields and the nine validation rules.
3. Reproduce initial and updated fixture results in Gold reports and tests.
4. Connect the steps into a Databricks job and document a reproducible demo.

This extension currently reads each small input file into the notebook's Python
process to preserve CSV record positions and fingerprint the exact parsed bytes.
It is not yet a distributed large-file ingestion design or a scale benchmark.
