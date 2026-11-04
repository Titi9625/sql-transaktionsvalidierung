# Power Query preparation

Status: all four queries executed in native Power Query on 4 November 2026 and loaded with Close & Apply. The PBIX in the parent directory also contains the three verified relationships, a delivery selector and the first verified Card; see `../MODEL.md`.

Create a Text parameter named `pDataFolder` whose current value is the absolute path to the repository `powerbi/data` folder. In each existing query, use Home > Advanced Editor and replace the complete expression with the matching `.pq` file. Keep the existing query names. Do not paste a query into the formula bar.

Set the parameter to your own checkout's absolute `powerbi/data` directory. In the parameter dialog, enter the actual folder path as plain text, without quotation marks or explanatory phrases. This setting points to a folder, not an individual CSV.

Each query rereads the original CSV. Headers are promoted once and types are assigned explicitly using the source decimal convention. IDs remain text; counts become whole numbers. Amounts use fixed-decimal Currency.Type. Literal `null` bank references become missing values. Source UTC times are normalised and retained in plain DateTime columns suffixed `_utc`, avoiding dependence on the computer timezone when loaded into the Power BI model.

`report_runs` additionally supplies friendly delivery labels and a scenario order. Its output is expected to have 27 columns and 2 rows. The other output row counts remain 3 valid payments, 7 findings and 2 daily report groups.

The CSV rate values are percentage points (25.00 and 50.00). Later calculate the report's percentage measure from valid count / distinct-payment count; do not format 25.00 directly as 25 percent. Both deliveries refer to successive snapshots of the same population, so payment metrics must be scoped to one selected delivery/report.

Native preview results: report_runs 27 columns / 2 rows; valid_payments 15 / 3; findings 11 / 7; daily_reports 14 / 2. Amounts and missing bank references were visually checked. Close & Apply returned to the report canvas with all four data tables present and no load error dialog. See `../evidence/model_and_card_checks.json` for the observed scope of this checkpoint.
