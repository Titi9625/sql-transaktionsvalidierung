-- Read-only export of the two saved reports verified on 2 November 2026.
-- Download result as findings.csv. Expected rows for this demo: 7.
SELECT *
FROM workspace.reconciliation_gold.saved_validation_findings
WHERE validation_run_id IN (
    'af7c4b41-43da-4f27-8b54-7926680db617',
    'd0d98c27-c6c7-46ee-b2e5-c0e9605d1725'
)
ORDER BY validation_run_id;
