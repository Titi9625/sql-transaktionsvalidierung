# Four-task Databricks job

Status: first native serverless Jobs execution completed on 21 October 2026.
The learner reported all four tasks succeeded. The report-task screenshot
confirms the updated delivery and its saved report output. Job repeat,
alternate-input and failure-propagation checks are still pending.

The job runs `Ingest -> prepare -> validate -> report`. Each task has its own
notebook/session. They communicate through persisted Delta tables and the exact
delivery ID published by ingestion, not Python variables or the newest batch.

## Verified first execution

Job: `payment-reconciliation-demo`; task keys are case-sensitive and the first
task in this workspace is **`Ingest`**, with a capital I. Downstream references
therefore use `{{tasks.Ingest.values.batch_id}}`.

- Job run ID: `569445875628778`.
- Scenario: `02_updated`.
- Delivery: `928cceb410a894c9ee04ac9d58c33100e4997d9a23d84b6125d73a5b81f3a27d`.
- Saved report ID: `5e5d5e98-9e38-4e97-9b4c-1783dc47d9c6`.
- Report: 4 distinct payment IDs, 2 accepted IDs, 2 flagged IDs, 3 findings,
  50.00% validation pass rate and EUR 147.00 accepted net.
- Ingestion output (user-provided): same delivery already loaded; 0 payment
  rows and 0 bank rows added; status ready.
- [Report-task screenshot](../evidence/job_report_updated.png) and
  [evidence record](../evidence/job_first_run.txt).
- All 41 portable Databricks checks passed locally against the repository files
  on 21 October 2026. These are separate from native cloud execution evidence.

The actual job settings have not yet been exported. Maximum concurrent runs=1
and disabled automatic retries are prescribed below; this screenshot does not
independently establish those settings.

## 1. Import clean source notebooks

In Workspace, create a folder named `payment_reconciliation_job` inside your
personal workspace folder. Right-click that folder, choose **Import**, browse
to each source file in `databricks/notebooks/`, and import notebooks 01, 02, 03
and 04. Keep their names. Their first-line marker makes them Python notebooks;
`# COMMAND ----------` splits their cells.

Use the updated Git source files, not the older learning notebooks with extra
malformed-input and recovery-check cells. Setup notebook 00 has already created
the schemas/input volume and is not a task. Inspection notebook 05 and the
failure checks are also not tasks. Creating/importing a notebook does not run it.

## 2. Create the job

1. Open **Jobs & Pipelines -> Create -> Job**.
2. Choose **Notebook** and name the first task `Ingest`.
3. For its path, choose the clean `01_ingest_bronze` you just imported.
4. Use **Serverless** compute. Do not create a classic cluster.
5. Save the task; rename the job `payment-reconciliation-demo`.
6. Add the remaining Notebook tasks using the table below.

| Task name | Notebook | Depends on | Task parameter |
|---|---|---|---|
| `Ingest` | `01_ingest_bronze` | none | none; receives job input_folder |
| `prepare` | `02_prepare_silver` | `Ingest` | `batch_id` = `{{tasks.Ingest.values.batch_id}}` |
| `validate` | `03_validate_silver` | `prepare` | `batch_id` = `{{tasks.Ingest.values.batch_id}}` |
| `report` | `04_report_gold` | `validate` | `batch_id` = `{{tasks.Ingest.values.batch_id}}` |

For each downstream task, click its box in the task graph and edit the
configuration below the graph. Under **Parameters**, keep the two inherited
**pushed down** job values. Click **JSON** next to **UI** in that middle panel
and enter only this task-specific object:

```json
{
  "batch_id": "{{tasks.Ingest.values.batch_id}}"
}
```

Then click **Save task**. The **Edit parameters** button in the right-hand
**Job parameters** panel edits shared job settings; it is the wrong location
for this task-value reference. An error saying the reference is not allowed
in a job parameter value indicates that wrong location. Keep the `batch_id`
key at task level only.

Use **All succeeded / ALL_SUCCESS** for downstream execution conditions. Do not
use **All done**, which would let a later task run after an earlier failure.
Leave automatic retries disabled for this first demonstration. Gold appends a
new report on every successful invocation; automatic retry after a late failure
is not guaranteed to create only one report for an orchestration attempt.

In **Job details -> Edit parameters**, add exactly these two job parameters:

| Key | Default value | Meaning |
|---|---|---|
| `input_folder` | `/Volumes/workspace/reconciliation_bronze/incoming_files/02_updated` | Folder containing the two CSVs to process. |
| `expected_scenario` | `02_updated` | Which synthetic example's expected answers to check. |

Do not add a job-level `batch_id`: job parameters take precedence over task
parameters, which would replace the ID supplied by ingestion. The curly-brace
reference belongs in the **task parameter value**, not in the Python editor.
For other data use `expected_scenario=none`; normal validation and package
consistency checks still run, while fixed example answers are skipped.

In **Advanced settings**, set maximum concurrent runs to **1**. Keep execution
manual: no recurring schedule or notifications are required. This prevents two
runs of this job overlapping; it does not lock out manual notebooks, other jobs
or external writers. Do not run them alongside this prototype job.

## 3. Acceptance checks in the cloud

First run the updated example using **Run now**. Open the run graph and confirm
all four tasks succeed in order. Open their outputs to compare the full batch
IDs. For the already-loaded fixture, ingestion should add 0/0 raw rows. Gold
should save 2 valid payment IDs, 3 findings and 50.00%, with its own report UUID.

Run it once more after completion. Expect the same batch ID and no additional
raw rows, plus one new report UUID. Earlier reports remain present. Then run
with different parameters for `01_initial` (change both folder and scenario).
Expect 1 valid ID, 4 findings and 25.00%, even if the updated batch is newer.

Finally run with `input_folder` pointing to `03_broken_format` and
`expected_scenario=none`. Ingestion must fail on the bank amount; downstream
tasks must be skipped/upstream-failed, and no report may be saved for that
attempt. The existing malformed-input check separately verifies unchanged
stored project data. Do not label this job failure check verified until its
run graph and outputs are observed.

Capture the successful graph, report output and failed graph without personal
email/credentials. After setup is verified, save a sanitised export of the
actual job definition here; this guide is not a deployed job configuration.

## Local checks and boundaries

`tests/test_databricks_job_parameters.py` exercises the notebook helpers against
a local SQLite registry: exact-ID selection, non-ready/missing/duplicate registry
rejection, missing/unresolved parameters before stage writes, input-directory
guards and scenario parsing. It does not execute Spark, Delta or Jobs. Existing
input, SQL validation, Gold-package and recovery-routing checks remain separate.

The loader, nine business rules, staging SQL, report definitions and saved-result
checks are unchanged. Views still contain all ready batches; outputs and saved
packages are filtered to the selected ID. This is a small synthetic serial
prototype, not a large-data/concurrent ingestion benchmark. Workspace imports
are manual copies; a Git push does not update them. Re-import updated source
files deliberately before claiming a newer code version ran in the cloud.

## Official references

Checked 21 October 2026:

- [Import source notebooks](https://docs.databricks.com/aws/en/notebooks/notebook-export-import)
- [Configure jobs](https://docs.databricks.com/aws/en/jobs/configure-job)
- [Job parameters and precedence](https://docs.databricks.com/aws/en/jobs/job-parameters)
- [Task parameters](https://docs.databricks.com/aws/en/jobs/task-parameters)
- [Pass task values](https://docs.databricks.com/aws/en/jobs/task-values)
