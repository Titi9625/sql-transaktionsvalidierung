-- Grain: one row per provider transaction date and source_system.
-- Payment IDs are counted once; source duplicates remain visible separately.
with payment_ids as (
    select
        source_system,
        transaction_id,
        min(transaction_date) as payment_date,
        count(*) as source_row_count
    from {{ ref('dbt_stg_payment_transactions') }}
    group by source_system, transaction_id
),

payment_errors as (
    select transaction_id, count(*) as error_row_count
    from {{ ref('dbt_invalid_transactions') }}
    where record_source = 'payment_provider'
    group by transaction_id
),

daily as (
    select
        p.payment_date,
        p.source_system,
        sum(p.source_row_count)::bigint as payment_row_count,
        count(*) as distinct_payment_count,
        count(*) filter (where v.transaction_id is not null) as valid_payment_count,
        count(*) filter (where e.transaction_id is not null) as flagged_payment_count,
        coalesce(sum(e.error_row_count), 0)::bigint as payment_error_row_count,
        count(*) filter (where p.source_row_count > 1) as duplicate_payment_id_count,
        coalesce(sum(v.amount_net), 0)::numeric as valid_net_amount_eur
    from payment_ids p
    left join {{ ref('dbt_valid_transactions') }} v
        on p.transaction_id = v.transaction_id
    left join payment_errors e
        on p.transaction_id = e.transaction_id
    group by p.payment_date, p.source_system
)

select
    payment_date::text || ':' || source_system as reporting_key,
    payment_date,
    source_system,
    payment_row_count,
    distinct_payment_count,
    valid_payment_count,
    flagged_payment_count,
    payment_error_row_count,
    duplicate_payment_id_count,
    round(100.0 * valid_payment_count / nullif(distinct_payment_count, 0), 2)
        as validation_pass_rate_pct,
    valid_net_amount_eur
from daily
