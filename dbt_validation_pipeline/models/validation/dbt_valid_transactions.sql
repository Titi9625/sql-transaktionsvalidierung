select
    p.transaction_id,
    p.transaction_date,
    p.amount_gross,
    p.fee_amount,
    p.amount_net,
    b.bank_amount,
    p.currency,
    p.status,
    'valid' as validation_status
from {{ ref('dbt_stg_payment_transactions') }} p
inner join {{ ref('dbt_stg_bank_transactions') }} b
    on p.transaction_id = b.matched_transaction_id
where p.currency = 'EUR'
  and p.amount_gross is not null
  and p.amount_net is not null
  and p.status = 'available'
  and p.transaction_type = 'charge'
  and abs(p.amount_net - b.bank_amount) <= 0.01
  and not exists (
      select 1
      from {{ ref('dbt_invalid_transactions') }} i
      where i.record_source = 'payment_provider'
        and i.transaction_id = p.transaction_id
  )