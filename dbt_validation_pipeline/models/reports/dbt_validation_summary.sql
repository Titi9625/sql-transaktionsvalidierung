select
    current_date as validation_date,

    (
        select count(*)
        from {{ ref('dbt_stg_payment_transactions') }}
    ) as total_payment_records,

    (
        select count(*)
        from {{ ref('dbt_stg_bank_transactions') }}
    ) as total_bank_records,

    (
        select count(*)
        from {{ ref('dbt_valid_transactions') }}
    ) as valid_records,

    (
        select count(distinct transaction_id)
        from {{ ref('dbt_invalid_transactions') }}
        where record_source = 'payment_provider'
    ) as invalid_payment_transactions,

    (
        select count(*)
        from {{ ref('dbt_invalid_transactions') }}
    ) as total_validation_errors,

    (
        select count(*)
        from {{ ref('dbt_invalid_transactions') }}
        where error_code = 'duplicate_transaction_id'
    ) as duplicate_errors,

    (
        select count(*)
        from {{ ref('dbt_invalid_transactions') }}
        where error_code = 'missing_amount'
    ) as missing_value_errors,

    (
        select count(*)
        from {{ ref('dbt_invalid_transactions') }}
        where error_code = 'invalid_currency'
    ) as currency_errors,

    (
        select count(*)
        from {{ ref('dbt_invalid_transactions') }}
        where error_code = 'amount_mismatch'
    ) as amount_mismatch_errors,

    (
        select count(*)
        from {{ ref('dbt_invalid_transactions') }}
        where error_code = 'unmatched_bank_transaction'
    ) as unmatched_bank_transactions,

    (
        select count(*)
        from {{ ref('dbt_invalid_transactions') }}
        where error_code = 'bank_reference_id_not_extractable'
    ) as bank_reference_id_not_extractable_errors