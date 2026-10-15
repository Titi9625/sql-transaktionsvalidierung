{% set batch_id = selected_batch_id() | trim %}
with raw_records as (
{% if batch_id %}
    select p.*
    from {{ source('raw_data', 'raw_payment_history') }} p
    inner join {{ source('raw_data', 'ingestion_batches') }} b
        on p.batch_id = b.batch_id
    where p.batch_id = '{{ batch_id }}'::uuid
      and b.status = 'ready'
{% else %}
    select * from {{ source('raw_data', 'raw_payment_transactions') }}
{% endif %}
)
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
from raw_records
