# Payment Reconciliation in Power BI

The report compares two deliveries of the same synthetic payment population. Choose a delivery to see how many payments passed validation, their net value after fees, and the issues the reconciliation pipeline found.

Open [payment_reconciliation.pbix](payment_reconciliation.pbix) in Power BI Desktop. The file contains the imported demo data and opens on **Delivery Overview** with **02 Updated delivery** selected.

| Delivery | Valid payments | Accepted net | Validation pass rate | Findings |
|---|---:|---:|---:|---:|
| 01 Initial delivery | 1 | €98.00 | 25% | 4 |
| 02 Updated delivery | 2 | €147.00 | 50% | 3 |

Switch to **01 Initial delivery** and look for the pending payment `txn_102`. In **02 Updated delivery**, that payment passed and adds €49.00 to the accepted net. The table still shows two source rows for the duplicate payment ID `txn_103` and an amount mismatch for `txn_104`. Each delivery has four distinct payment IDs, so one accepted payment gives 25% and two give 50%.

The single-selection delivery filter compares successive snapshots. Findings count detected issues, which can refer to multiple source rows for the same payment ID.

## Data and refresh

The report imports [four CSV snapshots](data/) of saved Databricks Gold results. To refresh from your own checkout, choose **Home → Transform data → Edit parameters** and set `pDataFolder` to the absolute path of its `powerbi/data` directory. Then choose **Home → Refresh**. Opening the imported demo only requires Power BI Desktop.

- [Export SQL](sql/) and [saved-run selection](demo_export_selection.json) identify the two source runs.
- [Power Query](powerquery/) contains the import expressions for the four CSVs.
- [Model notes](MODEL.md) describe table grains and how the delivery filter reaches the detail tables.
- [Measures](measures/) contains the four DAX expressions and display formats.
- [Report checks](evidence/report_checks.json) separate the observed report output from the values checked against the source data.
