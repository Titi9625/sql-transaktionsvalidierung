{% docs __overview__ %}
# Payment and bank validation pipeline

This portfolio extends a bachelor thesis at HTW Berlin. It demonstrates how to
load two synthetic CSV exports, prepare comparable fields, apply nine SQL
validation rules, and report the results with Python, PostgreSQL and dbt.

[Source code and setup instructions](https://github.com/Titi9625/sql-transaktionsvalidierung)

## Follow a payment through the project

| Layer | Models / sources | Responsibility |
|---|---|---|
| Raw inputs | `raw_payment_transactions`, `raw_bank_transactions` | Store the imported CSV rows, including deliberate errors. |
| Staging | `dbt_stg_payment_transactions`, `dbt_stg_bank_transactions` | Convert provider minor units to major units, uppercase currencies and extract a candidate payment ID from the bank reference. |
| Validation | `dbt_invalid_transactions`, `dbt_valid_transactions` | Emit rule findings and select rows satisfying the prototype's validity conditions. |
| Summary | `dbt_validation_summary` | Count current source rows, valid results and findings across both sources. |
| Daily report | `mart_daily_reconciliation` | Report distinct payment IDs and validation pass rate by provider date and source. |

Use the model list to read each definition and column description. Open the
dependency graph to see the model relationships. All six models are ordinary
PostgreSQL views: they store query definitions and read the current data.

## Nine business rules

| Code | What triggers the finding |
|---|---|
| `duplicate_transaction_id` | More than one provider row has the same non-NULL ID; each affected row emits a finding. |
| `missing_amount` | Provider gross or net amount is NULL. |
| `invalid_currency` | Provider currency is non-NULL and differs from EUR. |
| `pending_transaction` | Provider status is pending. |
| `refund_transaction` | Provider type is refund. |
| `amount_mismatch` | Matching provider/bank rows differ in net amount by more than 0.01 major units. |
| `missing_bank_match` | An available charge has no bank row with the same extracted ID. |
| `unmatched_bank_transaction` | A bank reference yields an ID, but no provider row has that ID. |
| `bank_reference_id_not_extractable` | The bank reference contains no ID matching the supported extraction pattern. |

Matching a reference is only the first step. Valid results also require provider
EUR currency, available status, charge type, present gross/net amounts, a net
difference no greater than 0.01 and no payment-side finding for the ID.
`available` does not prove a payout. Pending payments and refunds are scenario
exceptions, not necessarily incorrect real payments.

## Understand the counts

The supplied fixture has **50 payment rows, 45 distinct payment IDs and 40 bank
rows**. It produces **20 valid results, 25 flagged payment IDs and 46 finding
rows**. Of those findings, 36 are payment-side and 10 are bank-side.

One ID can emit several findings. Five duplicated IDs produce ten duplicate
finding rows. The daily report counts each payment ID once before calculating
its validation pass rate. The overall rate is **20 / 45 = 44.44%**. Recalculate
the overall rate from summed counts; do not average daily percentages.

`valid_net_amount_eur` sums provider net amounts for valid EUR payments. It is
not gross sales or a payout total. Bank-only findings are not included in the
payment reporting mart; the summary still includes them.

## What the tests establish

There are **28 dbt data tests**, separate from the nine business rules. They check
declared column properties, the fixed fixture results, exclusion of flagged
payments from valid results, and the report's classification and aggregation.
Four Python tests separately cover importer rollback/logging and runner control
flow. Tests demonstrate behaviour for this fixture, not production readiness.

## Scope and limitations

- The main pipeline uses synthetic CSVs. The optional Stripe API proof of concept
  is separate and does not feed these models.
- Each importer replaces one raw table in its own transaction. The entire pipeline
  is not one atomic transaction. `import_runs` records import attempts separately.
- Views and reports describe the currently loaded snapshot, not stored historical
  pipeline results. Summary dates and finding timestamps are evaluated at query time.
- The report assumes one date per payment ID and the current single provider
  source. Missing or conflicting payment dates fail the reporting consistency test.
- Real settlement batches, foreign-exchange conversion, incremental ingestion,
  broader NULL/status handling, scheduling and a business dashboard are outside
  the current implementation.

{% enddocs %}
