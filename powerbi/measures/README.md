# Report measures

All four measures are in `report_runs`. They return blank unless one saved run is in scope, which keeps the two deliveries separate.

| Measure | Calculation | Display |
|---|---|---|
| [Valid Payments](valid_payments.dax) | Count accepted payment rows | Whole number |
| [Accepted Net EUR](accepted_net_eur.dax) | Sum accepted payments after fees | Euro with two decimals |
| [Validation Pass Rate](validation_pass_rate.dax) | Divide valid payments by the selected run's distinct payment count | Percentage with zero decimals |
| [Validation Findings](validation_findings.dax) | Count findings rows | Whole number |

The pass rate uses a fraction such as 0.5, which Power BI displays as 50%. The exported summary's percentage column stores percentage points, so its equivalent value is 50.
