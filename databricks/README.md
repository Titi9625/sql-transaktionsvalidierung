# Databricks payment-reconciliation extension

Work in progress: a second implementation of the existing PostgreSQL/dbt project,
using Databricks Free Edition, PySpark, SQL and Delta tables. All inputs are
synthetic payment-provider and bank exports.

## Current checkpoint: initial and updated snapshots with saved history

Verified interactively in Databricks Free Edition on serverless compute:

| Check | Observed result |
|---|---|
| Project setup | Bronze, Silver and Gold schemas; managed `incoming_files` volume |
| Initial input preview | 5 payment rows and 3 bank rows |
| First Bronze load | 5 payment rows added, 3 bank rows added, batch `ready` |
| Repeat load | Same batch recognised; 0 payment rows and 0 bank rows added |
| Silver preparation (19 October 2026) | 5 payment rows, 3 bank rows; row-count and SQL-type checks passed |
| Initial Silver validation (19 October 2026) | 1 valid payment, 4 findings; exact scenario check passed |
| Initial Gold report (19 October 2026) | Checked report saved and fully read back; one saved run for the initial batch |
| Updated Bronze load (20 October 2026) | New ready batch; 5 payment rows and 4 bank rows added |
| Saved report comparison (20 October 2026) | Initial run 1 valid / 4 findings / 25%; two updated runs each 2 / 3 / 50% |
| Malformed-input rejection (20 October 2026) | Expected bank-row-4 amount error; registry, raw-history and saved-report contents unchanged |
| Source-history counts after report reruns | Initial batch 5 payment / 3 bank; updated batch 5 / 4 |
| Isolated controlled recovery (20 October 2026) | Stop at 5/0, failed; retry adds 0/3 and becomes ready; completed repeat adds 0/0 |

`txn_103` remains two separate source records (positions 3 and 4). The loader
identifies a stored source record by its batch ID and source record position,
not by the provider transaction ID. Business validation flags both duplicate
`txn_103` records separately.

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
| [02_prepare_silver.py](notebooks/02_prepare_silver.py) | Create two persistent preparation views; preview the latest ready batch and check counts/types. |
| [03_validate_silver.py](notebooks/03_validate_silver.py) | Apply nine business rules in two views; display results and check expected example IDs/error codes. |
| [04_report_gold.py](notebooks/04_report_gold.py) | Create summary/daily views; check and append a complete report package; expose saved results and verify full read-back. |
| [05_inspect_saved_history.py](notebooks/05_inspect_saved_history.py) | Read-only inspection of saved reports; does not load inputs or create a report run. |

[check_malformed_input_cell.py](checks/check_malformed_input_cell.py) is a
separate check-cell fragment. Run it at the bottom of notebook 01 after its
existing four cells; it reuses that notebook session's actual loader.
[check_recovery_cell.py](checks/check_recovery_cell.py) is another cell
fragment for notebook 01. It runs that same loader against fresh isolated
Delta tables and checks controlled interruption, retry and repeat behavior.

The numbered files are Databricks Python source notebooks. `# COMMAND ----------` separates
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
6. Run both cells of `02_prepare_silver`. It reads stored tables and needs no
   Python variables from notebook 01. Expect 5 payment and 3 bank rows for
   the initial batch. When pasting its entire source into one Python cell,
   run that single cell instead.
7. Run all three cells of `03_validate_silver` (or paste its entire source into
   one Python cell). For the initial pair expect 1 valid payment, 4 findings
   and `Scenario check passed: 01_initial.`
8. Run all three cells of `04_report_gold` (or paste its entire source into
   one Python cell). Expect `Gold report checked and saved successfully.`,
   a report run UUID, 25.00% pass rate, and EUR 98.00 accepted net.
   The first successful execution has one saved run for this batch.

After a notebook-session restart, rerun Cell 3 before Cell 4. The prepared Python
variables are session state; uploaded files and Delta tables persist separately.
`ready` means both sources were loaded, not that every payment is valid.

## Silver preparation

`workspace.reconciliation_silver.stg_payment_transactions` converts payment
gross, fee and net from whole-cent text into exact `DECIMAL(18,2)` euro values.
`workspace.reconciliation_silver.stg_bank_transactions` casts bank amounts
directly to `DECIMAL(12,2)` because they already represent euros. Both views
prepare DATE fields and uppercase currencies. The bank view extracts a
candidate `txn_` or `pp_` ID followed by digits; no match becomes NULL.

These persistent SQL views store query definitions, not prepared result rows.
They include only batches with registry status `ready`, retain `batch_id`
and `source_row_number`, and leave Bronze unchanged. They retain duplicates,
pending payments and differences for the later business rules. Future joins
and duplicate checks must include batch ID to avoid mixing snapshots.

The preview selects the latest ready batch. Its initial cloud execution
confirmed `txn_101` net/bank 98.00, both `txn_103` source positions,
`txn_102` still pending, and `txn_104` net 29.00 versus bank 28.95.
The notebook's count and date/decimal-type checks also passed.
[Verified initial Silver output](evidence/silver_initial.png).
This verifies preparation for this fixture; it does not establish
business-rule parity, failure recovery, multiple-batch cloud behavior or scale.

## Silver validation

The two views `workspace.reconciliation_silver.invalid_transactions` and
`workspace.reconciliation_silver.valid_transactions` port the nine rules and
acceptance conditions from the PostgreSQL dbt validation models. Every join,
duplicate grouping and exclusion includes batch ID. Source record positions
remain available; mismatch findings identify the related bank row position.

The initial cloud result was inspected and its exact scenario check passed:

| Payment | Result |
|---|---|
| `txn_101` | Valid: net and bank amount both 98.00 |
| `txn_102` | One pending finding |
| `txn_103` | Two duplicate findings, for source positions 3 and 4 |
| `txn_104` | One mismatch finding: 29.00 versus 28.95 |

[Verified initial validation output](evidence/validation_initial.png).
Four findings concern three distinct payment IDs. Matching a reference
alone does not establish validity. Pending and refund are reporting
exceptions within this scenario, not proof of an erroneous economic event.

The preview selects the latest ready delivery. The final notebook cell uses
`expected_scenario = "01_initial"`; change it to `"02_updated"` for that
fixture or `None` for your own data. The label chooses expected answers,
not input files or the batch to validate. Views return current results;
`detected_at` is computed during live queries. Gold captures and stores
those values together with the other results.

## Gold reporting and saved results

The initial Gold notebook completed successfully on 19 October 2026. Its summary
contains 5 payment rows, 3 bank rows, 4 different payment IDs, 1 accepted ID,
3 flagged IDs and 4 findings. The successful fixed-scenario check establishes
the 25.00% pass rate (1/4), even where screenshot columns are horizontally clipped.
The checked package also reconciles its accepted net of EUR 98.00 with the daily
report. The notebook writes the checked values once and compares the full stored
package after reading it back. [Initial Gold evidence](evidence/gold_initial.png).
On 20 October 2026 the updated snapshot was also loaded and processed. The
saved-summary query showed three report runs: the initial 25% result and
two updated 50% results with distinct report run IDs. This verifies visible
summary retention across changed input and repeated Gold execution. It does
not prove every raw record or every historical detail remained unchanged
under failures or concurrent edits.

All following objects are in `workspace.reconciliation_gold`:

| Object | Kind | Purpose |
|---|---|---|
| `validation_summary` | Live view | Per-batch counts and all nine finding-code totals. |
| `mart_daily_reconciliation` | Live view | Per-batch/provider-date/source counts and accepted net. |
| `validation_run_history` | Delta table | One complete checked package per report run. |
| `saved_validation_summaries` | View over history | Stored summary with run ID, capture time and definition fingerprint. |
| `saved_valid_payments` | View over history | Stored accepted matches for a run. |
| `saved_validation_findings` | View over history | Stored finding rows for a run. |
| `saved_daily_reports` | View over history | Stored daily figures for a run. |

Each history row stores a summary STRUCT and typed ARRAY-of-STRUCT detail/report
collections. One complete package is appended in a single Delta table write.
The saved views expose flat rows from the stored values; they do not rerun Silver
rules. This avoids distributing one result run across several independent writes.
It is not a multi-table transaction or an administrator-proof immutable audit log.

Batch ID identifies the input delivery; the UUID `validation_run_id` identifies
one saved report. A full Gold rerun is designed to append a new report run for
the same batch, without loading CSVs again. Filter saved results by the intended
run ID; adding repeated snapshots/runs would count the same payments again.

The daily model counts accepted IDs before joining, so several matching bank
rows cannot inflate accepted payment counts or accepted net. The summary keeps
both `valid_records` (joined result rows) and `valid_payment_count` (different
accepted IDs). It also reports `unclassified_payment_count` for existing rule
coverage gaps. Bank-only findings stay outside the payment daily mart.

The package captures the selected batch, capture timestamp, format version and
a SHA-256 fingerprint of the four Silver and two live Gold view definitions.
This is selected-SQL provenance, not a complete code or environment fingerprint.
Capture uses one composed Spark query followed by bounded driver-side checks,
then writes those captured values. A package may contain at most 10,000 combined
detail/report rows; this is a small-fixture learning design, not a scale benchmark.
Run serially without concurrent raw edits or view replacements. View creation
and history append are separate actions; a later notebook failure may leave a
complete stored package. No automatic result-schema evolution is enabled.

Example: inspect your saved reports in a Databricks SQL cell:

```sql
SELECT validation_run_id, batch_id, captured_at,
       distinct_payment_count, valid_payment_count,
       invalid_payment_transactions, total_validation_errors,
       validation_pass_rate_pct
FROM workspace.reconciliation_gold.saved_validation_summaries
ORDER BY captured_at;
```

## Updated snapshot and history verification

`02_updated` changes `txn_102` from pending to available and adds `bank_102`
for EUR 49.00. The duplicate `txn_103` and the EUR 0.05 mismatch for `txn_104`
remain. Expected updated results: 2 accepted IDs, 2 flagged IDs, 3 findings,
50.00% pass rate and EUR 147.00 accepted net. Fixed scenario checks in notebooks
03 and 04 verify these expectations when set to `02_updated`.

To reproduce after the initial run:

1. Create `02_updated` inside the existing `incoming_files` volume and upload
   both files from `testdaten/batch_scenarios/02_updated/`.
2. In notebook 01, change both input paths (Cell 1 preview and Cell 3 capture)
   from `01_initial` to `02_updated`. Run all four cells. Expect 5/4 new rows
   and ready status on the first load. [Observed updated load](evidence/updated_bronze.png).
   The repository keeps its initial-default paths so the first-run instructions
   remain reproducible; these are manual scenario-selection steps for now.
3. The existing Silver views already include new ready batches. In notebook 03
   set `expected_scenario = "02_updated"` and run it. Expect 2 valid rows,
   3 findings and a passed scenario check.
4. In notebook 04 set the same expected scenario and run it. Expect a new saved
   report with 50.00% pass rate. Repeating Gold creates another report run.
5. Run notebook 05, or paste its source into a new Python cell. It only reads
   saved summaries. [Observed comparison](evidence/report_history_comparison.png).

Observed on 20 October 2026:

| Delivery prefix | Report run prefix | Valid IDs | Findings | Pass rate |
|---|---|---|---|---|
| `8199572c3db5` | `b50bdd09` | 1 | 4 | 25.00% |
| `928cceb410a8` | `7850d94c` | 2 | 3 | 50.00% |
| `928cceb410a8` | `82149161` | 2 | 3 | 50.00% |

These are two deliveries and three saved reports. Repeated Gold execution does
not call the CSV loader. The extra report is expected, not evidence of duplicated
raw loading. The subsequent malformed-input check confirmed exact initial
and updated source-history counts of 5/3 and 5/4. Controlled partial-load
recovery was subsequently verified in separate Delta test tables, as described below.

## Malformed-input rejection without stored-data changes

On 20 October 2026 the user executed
[check_malformed_input_cell.py](checks/check_malformed_input_cell.py) in notebook
01. The bank file's fourth data record contains `NOT_A_NUMBER` as its amount.
The actual loader rejected it with the expected format error before loading.
[User-reported execution output](evidence/malformed_input_output.txt).

To reproduce:

1. Upload both `testdaten/batch_scenarios/03_broken_format/` files to
   `/Volumes/workspace/reconciliation_bronze/incoming_files/03_broken_format/`.
2. Keep notebook 01's two active input paths set to `02_updated`. Run its existing
   four cells: the already-loaded updated pair adds 0/0 rows. This defines the
   actual capture and loader functions in that notebook's current session.
3. Add a new Python cell at the bottom of the same notebook and paste the check
   fragment there. Run that new cell without another concurrent loader or writer.
   The fragment selects the broken pair separately; it does not replace the active
   source paths or import code.

The check verifies initial/updated source-history counts of 5/3 and 5/4. It then
captures row counts, per-batch counts and SHA-256 fingerprints of sorted complete
stored records before and after calling the real loader with the broken pair.
The compared physical tables are Bronze `ingestion_batches`, `raw_payment_history`,
`raw_bank_history` and Gold `validation_run_history`. The comparison includes
the nested saved report contents, not merely total row counts. The exact expected
ValueError must occur, and every stored-data signature must remain equal.

Pair IDs are reproduced from the uploaded bytes, preserving the real line-ending
identity. The check is bounded to 1,000 stored rows per table, assumes serial
access, and does not establish administrative/table-metadata immutability or
concurrent snapshot isolation. No cleanup is performed. This checks a format
rejection before loading, not recovery after a write partially succeeded.

## Controlled partial-load recovery in isolated Delta tables

On 20 October 2026 the user executed
[check_recovery_cell.py](checks/check_recovery_cell.py) at the bottom of notebook
01. [User-reported native execution output](evidence/recovery_output.txt).

| Stage | Stored payments | Stored bank rows | Registry state |
|---|---|---|---|
| Deliberate interruption before bank MERGE | 5 | 0 | failed |
| Serial retry | 5 | 3 | ready; 0 payment / 3 bank rows added |
| Completed repeat | 5 | 3 | ready; 0/0 rows added |

The check calls the existing `load_bronze_pair` function without changing its
body. A `RecoverySparkProxy` routes the three physical table names to fresh
tables in `workspace.reconciliation_checks` and raises one deliberate driver
exception before dispatching the bank MERGE. The real loader's handler must
record failed status, no loaded timestamp and the error type. Both actual
DataFrame operations and permitted SQL writes execute on native Spark/Delta.

The retry must preserve the five stored payment records, including their import
timestamps, add the three missing bank positions, clear the failure metadata and
mark ready only after checking both sources. A completed repeat must preserve
all isolated source/registry contents. Full before/after stored-record signatures
also verify that the working Bronze registry/histories and Gold report-history
contents were unchanged during this isolated check.

To reproduce, keep notebook 01's active paths at `02_updated` and run its existing
cells. Then paste the recovery fragment into a new Python cell and run only that
cell. It reuses the uploaded `01_initial` CSVs and creates three empty-schema test
tables with a shared random suffix. Write mode `error` is used for serverless
compatibility and fails if a generated target already exists. No additional
CSV upload or working-table overwrite is needed. Test tables remain as evidence;
rerunning creates a fresh set rather than deleting previous evidence.

The observed test tables share suffix `ce913a9a1522`:

- `workspace.reconciliation_checks.ingestion_batches_ce913a9a1522`
- `workspace.reconciliation_checks.raw_payment_history_ce913a9a1522`
- `workspace.reconciliation_checks.raw_bank_history_ce913a9a1522`

This verifies one controlled driver-exception boundary and serial recovery using
the real loader on separate Delta targets. It does not prove process-kill,
storage-outage, concurrency, or distributed-transaction recovery. Comparisons
are limited to 1,000 stored rows per table and require serial execution.

## Tests and evidence

[test_databricks_input_checks.py](../tests/test_databricks_input_checks.py) contains
seven portable tests of the actual notebook's input/helper functions. It checks
initial and updated pair identities, renamed inputs, duplicate record positions,
invalid dates/amounts/ranges, missing versus zero amounts, and rejection of the
malformed bank fixture before Spark is accessed.

[test_databricks_validation_rules.py](../tests/test_databricks_validation_rules.py)
adds nine portable checks of the actual notebook SQL. SQLite is a logic
adapter: qualified table names are adapted, and monetary values use exact
integer cents with a one-cent threshold. Results are compared with the actual
dbt queries for each batch separately. Checks cover the original 50/40-row
fixture, both small snapshots, all nine rules, isolation, the one-cent
boundary, multiple findings, NULL/zero behavior and bank-row lineage.
These are not Spark execution or DECIMAL-type tests.

[test_databricks_gold_reports.py](../tests/test_databricks_gold_reports.py) adds
eleven portable checks of actual Gold SQL and the package validator: initial,
updated and original fixtures, batch isolation, duplicate bank matches,
bank-only findings, rounding, unclassified records, inconsistent packages,
empty denominators and the size limit. It uses the same integer-cent SQLite
adapter and does not execute Spark nested schemas or Delta append/read-back.

[test_databricks_recovery_routing.py](../tests/test_databricks_recovery_routing.py)
adds six portable tests of the actual recovery adapter classes. They verify
safe target names, private read/write routing, rejection of unmapped operations,
DataFrame argument preservation and exactly one blocked bank write. Their
recording delegate does not execute Spark/Delta; the native recovery evidence
above is a separate manually executed integration check.

Run all 33 portable Databricks checks without a Databricks account or PostgreSQL connection:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p "test_databricks_*.py" -v
```

The test loader extracts imports and function definitions from the source
notebook without running its top-level Spark cells. These tests do not execute
Delta `MERGE` or prove cloud execution. The cloud evidence for this checkpoint
includes the first load/repeat, initial Silver/Gold path, updated Bronze
load, updated processing and saved-summary retention across three runs.
Portable tests remain separate from this observed cloud evidence. The
user-reported malformed-input output also confirms unchanged stored records
and exact source-history counts. Controlled driver interruption and serial
recovery were also verified on native Delta test tables using the real loader. The existing CI runs the portable
tests as part of Python discovery on pull requests to `main`; the branch name
`databricks-extension` does not itself trigger the current push workflow.

## Boundaries and remaining work

- Delta commits occur per table. This loader does not provide PostgreSQL-style
  rollback across both raw tables and the registry. An incomplete/failed attempt
  may leave partial rows. It is not published as ready; a serial retry inserts
  missing positions and checks both sources. Controlled driver-interruption
  and retry are verified on isolated Delta targets; real outages/concurrency
  are not established by that check. Silver consumers must select only ready batches.
- Run one loader at a time. Registry and source keys are checked in code rather
  than protected by enforced unique constraints. Concurrent multi-writer use is
  not supported by this checkpoint.
- The notebook captures each small file in its Python process. This preserves
  record positions and ties fingerprints to the exact parsed bytes, but is not
  distributed large-file ingestion or a scale benchmark.
- Updated loading, saved-summary retention, exact initial/updated raw counts
  and malformed-file rejection with unchanged stored records are cloud-verified.
  Controlled partial-load recovery and completed-repeat behavior are also
  verified in isolated Delta targets. The portable checks cover formats and
  identity, SQL rule semantics and batch isolation in the local adapter;
  they do not establish those additional cloud execution paths.
- Initial and updated Bronze-to-Gold processing with retained report
  summaries is cloud-verified. A parameterised Databricks job remains to be implemented.
  The PostgreSQL/dbt version remains the complete regression reference.
- The port preserves the original rule scope: no additional bank-currency,
  duplicate-bank, fee-arithmetic or unknown-status rules. A NULL currency
  does not satisfy SQL `<> 'EUR'`; normal CSV ingestion rejects missing
  currency before loading. Unknown statuses can produce neither a finding
  nor an accepted payment. Multiple matching bank records can multiply
  joined results. These limits are not silently changed by the port.
