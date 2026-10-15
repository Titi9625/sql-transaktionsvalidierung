{% set batch_id = selected_batch_id() | trim %}
with raw_records as (
{% if batch_id %}
    select k.*
    from {{ source('raw_data', 'raw_bank_history') }} k
    inner join {{ source('raw_data', 'ingestion_batches') }} b
        on k.batch_id = b.batch_id
    where k.batch_id = '{{ batch_id }}'::uuid
      and b.status = 'ready'
{% else %}
    select * from {{ source('raw_data', 'raw_bank_transactions') }}
{% endif %}
)
select
    bank_transaction_id,
    booking_date,
    reference_text,
    substring(reference_text from '(txn_[0-9]+|pp_[0-9]+)') as matched_transaction_id,
    amount as bank_amount,
    upper(currency) as currency,
    counterparty,
    imported_at
from raw_records
