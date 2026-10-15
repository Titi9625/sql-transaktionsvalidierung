# Batch ingestion examples

Six synthetic CSV files in the same column format as the original thesis fixtures.
The existing large fixtures remain the regression baseline for all nine rules.
These smaller examples focus on batch history, repeated submissions, updates,
duplicates, and a broken file. They do not represent real Stripe payouts.

## Files and expected results

Each folder contains `payments.csv` and `bank.csv`, a paired full snapshot.

| Folder | Payment rows | Bank rows | Valid payments | Flagged payment IDs | Error rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| 01_initial | 5 | 3 | 1 | 3 | 4 |
| 02_updated | 5 | 4 | 2 | 2 | 3 |
| 03_broken_format | 5 | 4 | Reject the pair | No new result | No new result |

Five payment rows contain four distinct IDs. `txn_103` appears twice.

| Payment ID | Initial snapshot | Updated snapshot |
| --- | --- | --- |
| txn_101 | Available charge. Provider net EUR 98.00 matches bank EUR 98.00. Valid. | Unchanged. Valid. |
| txn_102 | Pending charge. No bank row. One pending_transaction finding. | Available charge, matching bank row EUR 49.00 added. Valid. |
| txn_103 | Two provider rows, each net EUR 19.50, with one matching bank row. Two duplicate_transaction_id findings. | Both rows remain, and both stay flagged. |
| txn_104 | Provider net EUR 29.00, bank EUR 28.95. One amount_mismatch finding. | Unchanged. Still flagged. |

The initial summary has four error rows: two duplicate findings, one pending
finding and one amount mismatch. The updated summary has three: the two
duplicate findings and the amount mismatch.

The malformed pair uses the updated payments but replaces the fourth bank
data row's amount with `NOT_A_NUMBER`. The loader rejects it
without publishing a new completed batch or replacing previous results.
This is an import-format error, not one of the nine SQL business rules.

## Input contract

- Inputs are one provider CSV and one bank CSV from the same export cycle.
  Selecting them together declares the pairing; filenames alone do not prove it.
- Each pair is a complete snapshot, not a list of individual changes.
- Encoding: UTF-8. Separator: comma. Header names are exact and case-sensitive.
  CSV quotation marks must protect any field containing a comma or newline.
- Payment columns: provider_transaction_id, created, currency, amount, fee, net,
  status, type, reporting_category, source.
- Bank columns: bank_transaction_id, booking_date, reference_text, amount,
  currency, counterparty.
- Dates use YYYY-MM-DD. Payment amounts use whole minor units: 9800 means
  EUR 98.00 on an EUR record. Bank amounts use major units: 98.00 means EUR 98.00.
  A decimal point is the canonical separator for these examples.
- Empty amounts remain missing; they are not converted to zero. Structurally
  readable business problems (duplicates, pending payments, missing amounts,
  and non-EUR payments) must remain available to the validation rules.
- Malformed CSVs, missing required columns, unreadable dates or non-numeric
  non-empty amounts reject the pair. Missing required IDs reject the pair.
- A byte-identical pair is recognised using fingerprints of both file contents.
  Renaming those files does not make them new data. Equivalent CSVs with changed
  bytes are new snapshots for this first version. A changed partner file makes
  a new pair even if the other file's bytes are unchanged.
- Store batch ID and source row number separately from the supplied columns.
  Preserve duplicate provider rows. Check duplicates within a batch.
- Make a batch ready for validation only after both inputs load successfully.
  A failed attempt may be logged, but must not become a completed batch.
- Preserve raw history and saved validation results. A later rules version may
  revalidate an existing batch without loading another copy of its raw rows.

## Acceptance sequence

1. Import 01_initial. Save the initial raw rows and validation result.
2. Submit the same pair again. Add zero raw rows and no second successful batch.
   Recording a separate skipped import attempt is allowed.
3. Import 02_updated. Save a new batch, including txn_102's changed status.
   Keep the earlier pending row and its saved result in the first batch.
   History now contains 10 payment rows and 7 bank rows across two batches.
4. Submit 03_broken_format. Reject it. The updated batch remains the most recent
   successful batch. Successful raw-history counts stay at 10 and 7.

`expected_results.json` contains the exact acceptance expectations.

## Current implementation references and limits

Format sources: `testdaten/sample_stripe_transactions_large.csv` and
`testdaten/sample_bank_transactions_large.csv`.
Unit conversion and ID extraction: `dbt_validation_pipeline/models/staging/`.
Business rules: `dbt_validation_pipeline/models/validation/dbt_invalid_transactions.sql`.
Valid selection: `dbt_validation_pipeline/models/validation/dbt_valid_transactions.sql`.
Summary counts: `dbt_validation_pipeline/models/reports/dbt_validation_summary.sql`.

Use `run_batch_pipeline.py` with the two CSV paths and the matching `--scenario`
to load, validate and save one fixture pair. `run_pipeline.py` remains the original
50-payment/40-bank regression path and does not read these smaller fixtures.

Each successful validation saves a new run ID. Repeating a pair adds no raw rows,
but deliberately saves another validation run. The batch ID identifies the input
pair; the validation run ID identifies one execution of the validation logic.
