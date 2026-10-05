-- Each returned row is a test failure.
-- Check classification, rate definition and counts at the declared report grain.
select
    'inconsistent_daily_metrics' as issue,
    reporting_key as affected_key
from {{ ref('mart_daily_reconciliation') }}
where payment_row_count < distinct_payment_count
   or distinct_payment_count <= 0
   or valid_payment_count < 0
   or flagged_payment_count < 0
   or valid_payment_count + flagged_payment_count <> distinct_payment_count
   or payment_error_row_count < flagged_payment_count
   or duplicate_payment_id_count < 0
   or duplicate_payment_id_count > flagged_payment_count
   or validation_pass_rate_pct not between 0 and 100
   or validation_pass_rate_pct <>
       round(100.0 * valid_payment_count / nullif(distinct_payment_count, 0), 2)

union all

-- Do not silently assign a conflicting/missing-date payment to an arbitrary day.
select
    'ambiguous_or_missing_payment_date' as issue,
    source_system || ':' || transaction_id as affected_key
from {{ ref('dbt_stg_payment_transactions') }}
group by source_system, transaction_id
having count(distinct transaction_date) > 1
    or count(*) filter (where transaction_date is null) > 0

union all

-- Source totals and valid-result totals must survive the aggregation.
select 'inconsistent_report_totals' as issue, 'all_dates' as affected_key
where (
    select coalesce(sum(payment_row_count), 0)
    from {{ ref('mart_daily_reconciliation') }}
) <> (
    select count(*) from {{ ref('dbt_stg_payment_transactions') }}
)
or (
    select coalesce(sum(distinct_payment_count), 0)
    from {{ ref('mart_daily_reconciliation') }}
) <> (
    select count(*) from (
        select distinct source_system, transaction_id
        from {{ ref('dbt_stg_payment_transactions') }}
    ) payment_ids
)
or (
    select coalesce(sum(valid_payment_count), 0)
    from {{ ref('mart_daily_reconciliation') }}
) <> (
    select count(*) from {{ ref('dbt_valid_transactions') }}
)
or (
    select coalesce(sum(payment_error_row_count), 0)
    from {{ ref('mart_daily_reconciliation') }}
) <> (
    select count(*) from {{ ref('dbt_invalid_transactions') }}
    where record_source = 'payment_provider'
)
or (
    select coalesce(sum(valid_net_amount_eur), 0)
    from {{ ref('mart_daily_reconciliation') }}
) <> (
    select coalesce(sum(amount_net), 0)
    from {{ ref('dbt_valid_transactions') }}
)
