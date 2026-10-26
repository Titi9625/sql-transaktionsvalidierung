# Payment reconciliation: a three-minute walkthrough

This project compares payment-provider and bank records, shows which records
need review, and keeps earlier deliveries and reports. It has a PostgreSQL/dbt
implementation and a Databricks Free Edition extension.

All examples are synthetic. They use one bank booking per payment to make the
checks easy to inspect. Real payouts can group payments; matching those payouts
would require a different reconciliation model.

## 1. The problem, using one payment

The provider export and bank export describe different sides of a payment.
For `txn_101`, the customer pays **EUR 100.00 gross**, the provider deducts a
**EUR 2.00 fee**, and the expected bank amount is **EUR 98.00 net**.
The bank records EUR 98.00, so the amounts agree. Other rules still apply.

The initial example has five payment rows but only four payment IDs:

| Payment ID | What the files contain | Validation result |
|---|---|---|
| `txn_101` | Available; EUR 98.00 net and EUR 98.00 in the bank | Accepted |
| `txn_102` | Pending; no matching bank row | One pending finding; excluded from final-payment reporting |
| `txn_103` | The ID appears in two payment rows | Two duplicate findings, one for each source row |
| `txn_104` | EUR 29.00 net but EUR 28.95 in the bank | One amount-mismatch finding |

Result: **1 accepted ID, 3 flagged IDs, 4 findings**. A finding means one
detected issue, not one different payment. The pass rate is 1 accepted ID /
4 distinct payment IDs = **25%**.

## 2. The four processing steps

1. **Ingest / Bronze:** check both file formats, identify the pair and keep its
   original rows. Loading the same pair again adds no raw rows. Repeated IDs
   inside a source file remain visible for validation.
2. **Prepare / Silver:** give fields consistent names and types. Provider
   amount `10000` cents becomes EUR `100.00`; `eur` becomes `EUR`.
3. **Validate / Silver:** apply the nine business rules and compare payments
   with bank records for the selected delivery. Produce accepted rows and findings.
4. **Report / Gold:** check and save results and daily metrics under a new
   report-run ID. Keep earlier reports available.

The native job runs **Ingest -> prepare -> validate -> report**. Ingest passes
its delivery ID to later tasks so they use the same pair. Each task waits for
the previous task to succeed. [Observed report](evidence/job_report_updated.png).

## 3. The updated delivery

In `02_updated`, `txn_102` becomes available and gains a matching EUR 49.00 bank
record. The duplicate and amount-mismatch cases remain.

| Measure | Initial delivery | Updated delivery |
|---|---:|---:|
| Payment rows / bank rows | 5 / 3 | 5 / 4 |
| Distinct payment IDs | 4 | 4 |
| Accepted IDs | 1 | 2 |
| Flagged IDs | 3 | 2 |
| Findings | 4 | 3 |
| Validation pass rate | 25% | 50% |
| Accepted net amount | EUR 98.00 | EUR 147.00 |

These are expected synthetic example results, not a measured improvement in a
real company's payment operations. Both deliveries remain stored. Repeating a
delivery reuses its raw rows but saves another successful report with a new ID.
The older initial delivery can still be selected explicitly after the updated one.

## 4. Failure behavior and verification

A third example has an invalid bank amount. In the observed native job, Ingest
rejected it with a row-level error; prepare, validate and report did not execute.
The failed run displayed zero rows written. Separate isolated checks compared
stored contents and tested recovery after a deliberately interrupted load.

GitHub Actions run **#13** succeeded for commit **`95d411e`** on
`databricks-extension`, confirmed by the run-summary screenshot on 26 October
2026. It checks PostgreSQL/dbt and Python tests, including portable Databricks
logic checks. It does **not** execute Spark, Delta writes or the native job.
[Cloud records and job setup](jobs/README.md) document the separate cloud evidence.

## Scope to explain in an interview

This is a small, serial batch-processing prototype. Its strengths are traceable
inputs, repeat-safe raw loading, explicit delivery selection, visible failures
and retained reports. It is not a scale benchmark or a production deployment.
Delta commits separately per table. Controlled serial recovery was checked;
concurrent writers and real infrastructure outages were not established.
Gold saves a new report on each successful call; exactly-once report creation
after a late failure/retry is not guaranteed. Final retry settings were reviewed
from the exported configuration after the recorded cloud runs.

To reproduce the job, follow the [setup instructions](README.md) and
[four-task job guide](jobs/README.md). Read the
[detailed limits](README.md#boundaries-and-remaining-work) before extending it.
