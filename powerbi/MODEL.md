# Power BI model

The delivery selector uses `report_runs.delivery_name`. Each label in this export maps to one saved run. The three relationships between the imported tables use the full `validation_run_id` and filter in one direction from `report_runs` to the detail tables.

| Table | One row represents | Demo rows |
|---|---|---:|
| `report_runs` | One selected saved report run | 2 |
| `valid_payments` | One accepted payment result within a run | 3 |
| `findings` | One detected issue within a run | 7 |
| `daily_reports` | One payment date and source group within a run | 2 |

`report_runs.validation_run_id` is unique on the one side of each active relationship. Both deliveries contain four distinct payment IDs. The pass rate uses that count as the denominator. Raw row counts include duplicated IDs, and a payment can have multiple findings.

The findings table displays the source row number as a field without aggregation. This keeps rows 3 and 4 visible separately when both refer to `txn_103`. The report shows findings at source row and issue level. The daily summaries are available in the model for review.
