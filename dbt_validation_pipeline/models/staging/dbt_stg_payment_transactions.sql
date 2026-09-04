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
from {{ source('raw_data', 'raw_payment_transactions') }}