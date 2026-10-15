{% set batch_id = selected_batch_id() | trim %}
{{ config(enabled=(batch_id != '')) }}
{% if batch_id %}
select 'selected_batch_missing_not_ready_or_incomplete' as issue
where not exists (
    select 1
    from {{ source('raw_data', 'ingestion_batches') }} b
    where b.batch_id = '{{ batch_id }}'::uuid
      and b.status = 'ready'
      and b.payment_row_count = (
          select count(*) from {{ ref('dbt_stg_payment_transactions') }}
      )
      and b.bank_row_count = (
          select count(*) from {{ ref('dbt_stg_bank_transactions') }}
      )
)
{% else %}
select 'batch_mode_not_selected' as issue where false
{% endif %}
