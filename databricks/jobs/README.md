# Four-task Databricks job

Status: first native serverless Jobs execution completed on 21 October 2026.
The learner reported all four tasks succeeded. The report-task screenshot
confirms the updated delivery and its saved report output. A distinct repeat
run was verified on 23 October: ingestion added 0/0 rows, the report saved a new
UUID and the updated results remained 2 accepted IDs / 3 findings / 50%.
Initial-input selection was also verified from same-run Ingest/report screenshots
on 25 October: 0/0 added rows, 1 accepted ID, 4 findings, 25% and EUR 98.
The malformed-input failure graph was also verified on 25 October: Ingest
failed and all three downstream tasks were upstream-failed. The revised actual job JSON was reviewed on 26 October: concurrency is limited
to one, dependencies/parameters are correct, and all four tasks explicitly
disable normal and additional serverless retries.

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

## Reviewed actual job configuration - 26 October 2026

[Sanitised job settings](payment_reconciliation_job.json) were copied from the
learner's actual export after saving all four retry policies. Replace
`<your-user>` in its four notebook paths with your workspace user before using
it with the Jobs API or CLI. Import notebooks 01-04 to those paths and run setup
00/upload the example files first. This settings file is not a full deployment
bundle and has not been applied to a second workspace.

Verified values:

- `max_concurrent_runs=1`, queue enabled, no recurring trigger.
- Four enabled `ALL_SUCCESS` tasks with the intended dependency chain.
- Exact `{{tasks.Ingest.values.batch_id}}` parameter in all three later tasks.
- Updated-input defaults; no job-level `batch_id` override.
- On every task: `max_retries=0`, `retry_on_timeout=false`, and
  `disable_auto_optimization=true`.
- Serverless environment version 6; `PERFORMANCE_OPTIMIZED` compute mode.

The exported retry interval is 900000 milliseconds (15 minutes). It is unused
with the configured retries disabled. Performance mode is separate from retry
auto-optimisation. [Detailed review](../evidence/job_configuration_review.txt).
The cloud checks below preceded the final retry-policy change; the new settings
are verified from the supplied export, without claiming another cloud run.

## Verified repeat execution - 23 October 2026

- New Job run ID: `238559574710030` (different from the first job execution).
- Same updated delivery: `928cceb410a894c9ee04ac9d58c33100e4997d9a23d84b6125d73a5b81f3a27d`.
- Ingest task succeeded: 0 payment / 0 bank rows added; stored counts 5/4; ready.
- Report task succeeded with new report ID `d0d98c27-c6c7-46ee-b2e5-c0e9605d1725`.
- Results: 4 payment IDs, 2 accepted, 2 flagged, 3 findings, 50.00%, EUR 147.00.
- The output states earlier reports were not overwritten; saved run count is 4.
- [Transcribed repeat-run evidence](../evidence/job_repeat_run.txt).

Both observed task outputs belong to the same new job run. The screenshots do
not show the intermediate outputs or full graph, or compare every historical
report's full contents. This repeat-run evidence is separate from the initial-input, malformed-input
and configuration checks documented below.

## Verified initial-input selection - observed 25 October 2026

- Job run ID: `716287669097190`.
- Resolved inputs: `01_initial` folder and expected scenario.
- Delivery: `8199572c3db5105dd6a290e6fc969eea50df9f896301a23881f6febbfe98936d`; matches ingestion's published ID.
- Ingest succeeded: ready; 0/0 rows added; stored source counts 5/3.
- Report succeeded: new report ID `af7c4b41-43da-4f27-8b54-7926680db617`.
- Results: 4 payment IDs, 1 accepted, 3 flagged, 4 findings, 25.00%, EUR 98.00.
- Saved reports for this initial delivery: 3.
- [Transcribed initial-selection evidence](../evidence/job_initial_selection.txt).

This selects the older input pair explicitly after updated-input executions;
it does not pick the newest loaded batch. These screenshots show successful
Ingest and report tasks, not all intermediate outputs or full history contents.
Malformed-input job failure and final configuration review are documented below
and above, respectively.

## Malformed-input ingestion error - observed 25 October 2026

Job run `474488750298099` failed on the selected `03_broken_format` pair.
The Ingest task shows the expected `ValueError` for `bank.csv`, data row 4:
`amount must be numeric with at most two decimals`. Its displayed write count is
zero. [Transcribed error evidence](../evidence/job_malformed_input_error.txt).

The attempt selector shows `Retry: 1st (latest)`. This run preceded the final
retry-policy change. Its detailed graph confirms two ingestion attempts, followed
by `prepare`, `validate` and `report` all `Upstream failed` with zero-second
durations. The report task did not execute. The overall displayed write count
is also zero. Final concurrency/retry settings are now verified separately from the revised
JSON export. Do not infer a full stored-history comparison from that metric.

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
For this prototype, configure each task with zero explicit retries and disable
**Enable serverless auto-optimization (may include additional retries)** in its
Retry Policy dialog, then save the task. Serverless auto-optimisation can retry
failed tasks even when no explicit retry policy was intended. This is separate
from the **Performance optimized** compute mode. The observed malformed-input
run made two ingestion attempts before the configuration was changed. The
revised export verifies that additional retries are now disabled for all tasks.
Gold appends a new report on every successful invocation; retry after a late
failure is not guaranteed to create only one report for an orchestration attempt.

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

The malformed-input failure-propagation check is now observed for run
`474488750298099`, including the expected error and all downstream statuses.

The observed outputs and failed graph are transcribed in the linked evidence
records; personal account details are omitted. The sanitised final settings
file is included above. These manual observations are not downloaded backend
logs or a full before/after comparison of all stored history.

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

Setup references checked 21 October; configuration references checked 26 October 2026:

- [Import source notebooks](https://docs.databricks.com/aws/en/notebooks/notebook-export-import)
- [Configure jobs](https://docs.databricks.com/aws/en/jobs/configure-job)
- [Job parameters and precedence](https://docs.databricks.com/aws/en/jobs/job-parameters)
- [Task parameters](https://docs.databricks.com/aws/en/jobs/task-parameters)
- [Pass task values](https://docs.databricks.com/aws/en/jobs/task-values)

Serverless retry-setting reference (checked 25 October 2026):
[Disable additional automatic retries](https://docs.databricks.com/aws/en/jobs/run-serverless-jobs#configure-serverless-compute-auto-optimization-to-disallow-retries).
