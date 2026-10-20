# Databricks notebook source
# Notebook: 05_inspect_saved_history | Language: Python
# Aim: inspect saved report history without loading files or saving another report.
# This is the same query verified interactively on 20 October 2026.
# Observed: initial 1 valid / 4 findings / 25%; two updated runs 2 / 3 / 50%.
# More rows are normal after further Gold runs: each saved report has its own ID.
# Displayed IDs are shortened for readability; storage retains the complete IDs.

display(spark.sql("""
    SELECT
        LEFT(batch_id, 12) AS delivery,
        LEFT(validation_run_id, 8) AS report_run,
        valid_payment_count AS valid_payments,
        total_validation_errors AS findings,
        validation_pass_rate_pct AS pass_rate_pct
    FROM workspace.reconciliation_gold.saved_validation_summaries
    ORDER BY captured_at
"""))
