# Report measures

`valid_payments.dax` is implemented as a Power BI measure, with its home table set to report_runs and its display format set to whole number. Its full expression was inspected in Power BI Desktop and matches the source file.

It counts accepted result rows in the currently selected saved run, returning zero for an empty accepted-results set. It returns blank when more than one (or no) run is in scope, protecting against adding successive delivery snapshots together.

The table grain is one accepted payment result within a saved validation run. Native Card checks observed directly on 4 November 2026: select 02 Updated delivery -> 2; select 01 Initial delivery -> 1; restore 02 Updated delivery -> 2, then save. The visual correctly responds to the delivery selector.

The learner screenshot Screen select.png confirms a delivery_name slicer with Single select On. Empty-data and ambiguous-run guard branches have not been executed in the native model; their behaviour is specified by the expression but is not claimed as an observed test result.
