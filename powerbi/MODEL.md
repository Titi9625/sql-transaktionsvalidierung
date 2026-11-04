# Power BI model

Model settings inspected directly in Power BI Desktop on 4 November 2026.

| Parent | Child | Key in both tables | Cardinality | Filter direction | Status |
|---|---|---|---|---|---|
| report_runs | daily_reports | validation_run_id | One to many | Parent to child only | Active |
| report_runs | findings | validation_run_id | One to many | Parent to child only | Active |
| report_runs | valid_payments | validation_run_id | One to many | Parent to child only | Active |

These are the only three relationships. The two automatically inferred inactive batch_id links through daily_reports were removed by the learner. All three final settings were verified in Manage relationships, and the file was saved.

## Table grain

- report_runs: one selected saved validation run per delivery in this export.
- daily_reports: one payment-date/source group within a saved validation run. The current small demo contains one group per run; the relationship allows future multi-day deliveries.
- valid_payments: one accepted payment result within a saved validation run.
- findings: one detected issue within a saved validation run. Multiple issues or duplicate source rows can refer to the same payment ID.

Use report_runs to choose the delivery/run. The initial and updated deliveries are successive snapshots of the same payment population; they must not be totalled as additional business payments. Join results by the full validation_run_id, which identifies the saved report version, rather than by a shortened display label or transaction_id alone.

## First interactive acceptance check

Screen select.png confirms a delivery_name slicer with Single select On. The first Card visual uses the Valid Payments measure documented in measures/. Its complete DAX formula, home table report_runs and whole-number format were inspected directly in Power BI Desktop.

Native test on 4 November 2026: Updated delivery showed 2, switching to Initial delivery changed the card to 1, and switching back to Updated restored 2. The working PBIX was then saved. This proves filter propagation from report_runs to valid_payments for the two demo selections.

The CSV snapshot is imported locally; this is not a live Databricks connection. Filter propagation to findings and daily_reports, further measures, zero/ambiguous-selection behaviour, report layout and final acceptance are still pending.
