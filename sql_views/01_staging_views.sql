create or replace view stg_payment_transactions as
select
    provider_transaction_id as transaction_id,
    created as transaction_date,
    upper(currency) as currency,
    amount / 100.0 as amount_gross,
    fee / 100.0 as fee_amount,
    net / 100.0 as amount_net,
    status,
    type as transaction_type,
    reporting_category,
    'stripe_sample_csv' as source_system,
    source as provider_source_reference,
    imported_at
from raw_payment_transactions;


create or replace view stg_bank_transactions as
select
    bank_transaction_id,
    booking_date,
    reference_text,
    substring(reference_text from '(txn_[0-9]+|pp_[0-9]+)') as matched_transaction_id,
    amount as bank_amount,
    upper(currency) as currency,
    counterparty,
    imported_at
from raw_bank_transactions;