select
    v.transaction_id
from {{ ref('dbt_valid_transactions') }} v
inner join {{ ref('dbt_invalid_transactions') }} i
    on v.transaction_id = i.transaction_id
where i.record_source = 'payment_provider'