with duplicate_transactions as (
    select
        transaction_id
    from {{ ref('dbt_stg_payment_transactions') }}
    where transaction_id is not null
    group by transaction_id
    having count(*) > 1
),

invalid_payment_transactions as (

    select
        p.transaction_id,
        'payment_provider' as record_source,
        'duplicate_transaction_id' as error_code,
        'Die Transaktions-ID kommt mehrfach vor.' as error_description,
        'transaction_id' as error_field,
        current_timestamp as detected_at
    from {{ ref('dbt_stg_payment_transactions') }} p
    inner join duplicate_transactions d
        on p.transaction_id = d.transaction_id

    union all

    select
        p.transaction_id,
        'payment_provider' as record_source,
        'missing_amount' as error_code,
        'Der Brutto- oder Nettobetrag fehlt.' as error_description,
        'amount_gross / amount_net' as error_field,
        current_timestamp as detected_at
    from {{ ref('dbt_stg_payment_transactions') }} p
    where p.amount_gross is null
       or p.amount_net is null

    union all

    select
        p.transaction_id,
        'payment_provider' as record_source,
        'invalid_currency' as error_code,
        'Die Währung ist nicht EUR.' as error_description,
        'currency' as error_field,
        current_timestamp as detected_at
    from {{ ref('dbt_stg_payment_transactions') }} p
    where p.currency <> 'EUR'

    union all

    select
        p.transaction_id,
        'payment_provider' as record_source,
        'pending_transaction' as error_code,
        'Die Transaktion ist noch offen und wird nicht als finaler Umsatz gezählt.' as error_description,
        'status' as error_field,
        current_timestamp as detected_at
    from {{ ref('dbt_stg_payment_transactions') }} p
    where p.status = 'pending'

    union all

    select
        p.transaction_id,
        'payment_provider' as record_source,
        'refund_transaction' as error_code,
        'Die Transaktion ist eine Rückerstattung und wird nicht als normaler Umsatz gezählt.' as error_description,
        'transaction_type' as error_field,
        current_timestamp as detected_at
    from {{ ref('dbt_stg_payment_transactions') }} p
    where p.transaction_type = 'refund'

    union all

    select
        p.transaction_id,
        'payment_provider' as record_source,
        'amount_mismatch' as error_code,
        'Der Nettobetrag des Payment Providers stimmt nicht mit dem Bankbetrag überein.' as error_description,
        'amount_net / bank_amount' as error_field,
        current_timestamp as detected_at
    from {{ ref('dbt_stg_payment_transactions') }} p
    inner join {{ ref('dbt_stg_bank_transactions') }} b
        on p.transaction_id = b.matched_transaction_id
    where abs(p.amount_net - b.bank_amount) > 0.01

    union all

    select
        p.transaction_id,
        'payment_provider' as record_source,
        'missing_bank_match' as error_code,
        'Zu dieser Payment-Provider-Transaktion wurde keine passende Banktransaktion gefunden.' as error_description,
        'matched_transaction_id' as error_field,
        current_timestamp as detected_at
    from {{ ref('dbt_stg_payment_transactions') }} p
    left join {{ ref('dbt_stg_bank_transactions') }} b
        on p.transaction_id = b.matched_transaction_id
    where b.matched_transaction_id is null
      and p.status = 'available'
      and p.transaction_type = 'charge'
),

invalid_bank_transactions as (

    select
        b.matched_transaction_id as transaction_id,
        'bank_export' as record_source,
        'unmatched_bank_transaction' as error_code,
        'Im Bankexport existiert eine Buchung mit Transaktions-ID, aber ohne passende Payment-Provider-Transaktion.' as error_description,
        'matched_transaction_id' as error_field,
        current_timestamp as detected_at
    from {{ ref('dbt_stg_bank_transactions') }} b
    left join {{ ref('dbt_stg_payment_transactions') }} p
        on b.matched_transaction_id = p.transaction_id
    where b.matched_transaction_id is not null
      and p.transaction_id is null

    union all

    select
        b.bank_transaction_id as transaction_id,
        'bank_export' as record_source,
        'bank_reference_id_not_extractable' as error_code,
        'Aus dem Verwendungszweck der Bankbuchung konnte keine Transaktions-ID extrahiert werden.' as error_description,
        'reference_text' as error_field,
        current_timestamp as detected_at
    from {{ ref('dbt_stg_bank_transactions') }} b
    where b.matched_transaction_id is null
)

select *
from invalid_payment_transactions

union all

select *
from invalid_bank_transactions