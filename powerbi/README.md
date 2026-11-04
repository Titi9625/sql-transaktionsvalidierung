# Payment reconciliation report (Power BI)

Status: data preparation, model, delivery selector and first Valid Payments card verified in Power BI Desktop on 4 November 2026. The remaining metrics, exception view and final layout are in progress.

## Purpose

Show how many payment IDs passed validation, which need attention, and why. This report uses saved results from the Databricks prototype with synthetic data.

## Open the working report

1. Open `payment_reconciliation.pbix` in Power BI Desktop.
2. Choose Home > Transform data > Edit parameters and set `pDataFolder` to the absolute path of this checkout's `powerbi/data` folder.
3. Choose Home > Refresh. The imported demo is already present in the saved file; the parameter change makes subsequent refreshes use your checkout.
4. Select `02 Updated delivery`: the Valid Payments card should show 2. Select `01 Initial delivery`: it should show 1. Restore Updated for the default view.

The file is a local CSV snapshot, not a live Databricks connection. No service publication or account sign-in is needed for this local demo. The layout is an initial working checkpoint.

## Data and model

| CSV | One row represents | Demo rows |
|---|---|---:|
| report_runs.csv | One saved report run for a delivery | 2 |
| valid_payments.csv | One accepted payment result within a saved run | 3 |
| findings.csv | One detected issue within a saved run | 7 |
| daily_reports.csv | One payment-date/source group within a saved run | 2 |

The initial and updated deliveries are successive snapshots of the same payment population, not additional sales. The delivery selector uses single selection. The first measure additionally returns blank unless exactly one saved run is in scope.

`report_runs.validation_run_id` is the unique parent key. It has three active one-to-many relationships, one to each detail table on the same full key. Filtering is single-direction from report_runs to detail. Findings can have several rows for one payment; they are not a count of distinct payments. See [MODEL.md](MODEL.md) for the design and observed checks.

## Reviewable sources

- [powerquery/](powerquery/): four M expressions, explicit number/date types, UTC field naming, missing bank references and delivery labels.
- [measures/](measures/): implemented DAX and its native checks.
- [evidence/model_and_card_checks.json](evidence/model_and_card_checks.json): observed acceptance scope, saved file hash and outstanding checks.
- `sql/`: four read-only Databricks exports using the same two pinned saved run IDs.
- `demo_export_selection.json`: selected IDs and expected source metrics.

To recreate the report, import each CSV separately through Get data > Text/CSV > Transform Data, create the Text parameter pDataFolder, and replace each complete expression through Advanced Editor with the matching M source. Configure the relationships from MODEL.md, create Valid Payments from its DAX source, add a delivery_name slicer with Single select On, and put the measure in a Card.

## Source exports and limits

The existing CSV exports reconcile to the pinned initial/updated runs and accepted net amounts of EUR 98.00 / EUR 147.00. For another Databricks workspace, select its corresponding saved runs and change the selection consistently in all four SQL files before downloading new exports. Capture date is not the scenario order.

Source pass rates are percentage points (25 and 50). The future percentage measure must divide valid-payment count by distinct-payment count, then apply percentage formatting. Do not directly percentage-format the source 25 as a fraction.

Verified so far: native M previews and load, three relationship settings, single-selection configuration, full Valid Payments expression and Card switching 2 -> 1 -> 2. Still pending: propagation checks for findings/daily_reports, remaining metrics, exception table, final layout, screenshots and walkthrough. No production-scale or live-refresh claims are made.
