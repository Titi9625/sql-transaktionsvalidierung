# Payment reconciliation report (Power BI)

Status: all four CSV source exports verified on 3 November 2026. Power BI import, modelling and the report are pending.

## Purpose

Help a reviewer see how many distinct payment IDs passed validation, which need attention, and why. This report uses saved results from the existing Databricks prototype with synthetic data.

## Source files

Run the four read-only SQL files in `sql/` in Databricks SQL Editor and download each result as CSV. All four queries deliberately select the same two saved report IDs. The selection and expected example values are recorded in `demo_export_selection.json`.

| CSV | One row represents | Expected demo rows |
| --- | --- | ---: |
| report_runs.csv | One saved report run for a delivery | 2 |
| valid_payments.csv | One accepted payment result within a saved run | 3 |
| findings.csv | One detected issue within a saved run | 7 |
| daily_reports.csv | One payment-date group within a saved run | 2 |

The initial and updated deliveries describe successive snapshots of the same payment population. They are not additional sales. Select one delivery/report when displaying payment totals. Findings are individual issues, not distinct payments.

## Planned model

Use `report_runs.validation_run_id` as a unique parent key, with one-to-many relationships to the three detail tables on `validation_run_id`. Keep filtering from the report-run table toward detail tables. Do not join the valid-payment and finding tables directly: repeated findings can multiply rows.

## Reproduction and limits

The pinned UUIDs refer to the verified reports in this demonstration workspace. For another workspace, choose its corresponding saved initial and updated runs and replace the selection consistently in all four SQL files. CSVs provide a local, reviewable snapshot; they are not a live Databricks connection.

The four provided CSVs have been checked against the selected runs, cross-file keys, expected counts and accepted net amounts (EUR 98.00 initially; EUR 147.00 updated). Preserve identifier columns as text. Handle Databricks CSV literal `null` values as missing data, and apply explicit decimal/date types in Power Query so regional settings do not change amounts.

## First import

In Power BI Desktop, choose Home > Get data > Text/CSV and open `data/report_runs.csv`. Set file origin to UTF-8 (65001), delimiter to Comma, and data type detection to Do not detect data types. Choose Transform Data so identifiers, dates and numbers can be typed explicitly before loading. Import the three remaining files as separate tables, not as one combined folder.

Report creation, metric definitions, source PBIX and screenshots are still pending. Keep the scenario selection explicit; report capture date is not the scenario sequence.
