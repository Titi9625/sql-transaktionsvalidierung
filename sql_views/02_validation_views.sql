create or replace view invalid_transactions as

with duplicate_payment_transactions as (
    select
        transaction_id
    from stg_payment_transactions
    where transaction_id is not null
    group by transaction_id
    having count(*) > 1
)

select
    transaction_id,
    'payment_provider' as record_source,
    'missing_transaction_id' as error_code,
    'Die Transaktions-ID fehlt.' as error_description,
    'transaction_id' as error_field,
    current_timestamp as detected_at
from stg_payment_transactions
where transaction_id is null
   or trim(transaction_id) = ''

union all

select
    p.transaction_id,
    'payment_provider' as record_source,
    'duplicate_transaction_id' as error_code,
    'Die Transaktions-ID kommt mehrfach vor.' as error_description,
    'transaction_id' as error_field,
    current_timestamp as detected_at
from stg_payment_transactions p
join duplicate_payment_transactions d
    on p.transaction_id = d.transaction_id

union all

select
    transaction_id,
    'payment_provider' as record_source,
    'missing_amount' as error_code,
    'Der Brutto- oder Nettobetrag fehlt.' as error_description,
    'amount_gross / amount_net' as error_field,
    current_timestamp as detected_at
from stg_payment_transactions
where amount_gross is null
   or amount_net is null

union all

select
    transaction_id,
    'payment_provider' as record_source,
    'invalid_currency' as error_code,
    'Die Währung ist nicht EUR.' as error_description,
    'currency' as error_field,
    current_timestamp as detected_at
from stg_payment_transactions
where currency is null
   or currency <> 'EUR'

union all

select
    transaction_id,
    'payment_provider' as record_source,
    'pending_transaction' as error_code,
    'Die Transaktion ist noch offen und wird nicht als finaler Umsatz gezählt.' as error_description,
    'status' as error_field,
    current_timestamp as detected_at
from stg_payment_transactions
where status = 'pending'

union all

select
    transaction_id,
    'payment_provider' as record_source,
    'net_amount_calculation_error' as error_code,
    'Der Nettobetrag entspricht nicht Bruttobetrag minus Gebühr.' as error_description,
    'amount_net' as error_field,
    current_timestamp as detected_at
from stg_payment_transactions
where amount_gross is not null
  and fee_amount is not null
  and amount_net is not null
  and round(amount_gross - fee_amount, 2) <> round(amount_net, 2)

union all

select
    transaction_id,
    'payment_provider' as record_source,
    'negative_charge_amount' as error_code,
    'Eine normale Zahlung darf keinen negativen Bruttobetrag haben.' as error_description,
    'amount_gross' as error_field,
    current_timestamp as detected_at
from stg_payment_transactions
where transaction_type = 'charge'
  and amount_gross < 0

union all

select
    transaction_id,
    'payment_provider' as record_source,
    'refund_transaction' as error_code,
    'Die Transaktion ist eine Rückerstattung und wird nicht als normaler Umsatz gezählt.' as error_description,
    'transaction_type' as error_field,
    current_timestamp as detected_at
from stg_payment_transactions
where transaction_type = 'refund'

union all

select
    p.transaction_id,
    'payment_provider' as record_source,
    'missing_bank_match' as error_code,
    'Zu dieser Payment-Provider-Transaktion wurde keine passende Banktransaktion gefunden.' as error_description,
    'matched_transaction_id' as error_field,
    current_timestamp as detected_at
from stg_payment_transactions p
left join stg_bank_transactions b
    on p.transaction_id = b.matched_transaction_id
where p.transaction_type = 'charge'
  and p.status = 'available'
  and p.transaction_id is not null
  and b.bank_transaction_id is null

union all

select
    p.transaction_id,
    'payment_provider' as record_source,
    'amount_mismatch' as error_code,
    'Der Nettobetrag des Payment Providers stimmt nicht mit dem Bankbetrag überein.' as error_description,
    'amount_net / bank_amount' as error_field,
    current_timestamp as detected_at
from stg_payment_transactions p
join stg_bank_transactions b
    on p.transaction_id = b.matched_transaction_id
where p.transaction_type = 'charge'
  and p.status = 'available'
  and p.amount_net is not null
  and b.bank_amount is not null
  and round(p.amount_net, 2) <> round(b.bank_amount, 2)

union all

select
    b.matched_transaction_id as transaction_id,
    'bank_export' as record_source,
    'unmatched_bank_transaction' as error_code,
    'Im Bankexport existiert eine Buchung mit Transaktions-ID, aber ohne passende Payment-Provider-Transaktion.' as error_description,
    'matched_transaction_id' as error_field,
    current_timestamp as detected_at
from stg_bank_transactions b
left join stg_payment_transactions p
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
from stg_bank_transactions b
where b.matched_transaction_id is null;


create or replace view valid_transactions as
select distinct
    p.transaction_id,
    p.transaction_date,
    round(p.amount_gross, 2) as amount_gross,
    round(p.fee_amount, 2) as fee_amount,
    round(p.amount_net, 2) as amount_net,
    round(b.bank_amount, 2) as bank_amount,
    p.currency,
    p.status,
    'valid' as validation_status
from stg_payment_transactions p
join stg_bank_transactions b
    on p.transaction_id = b.matched_transaction_id
where p.transaction_type = 'charge'
  and p.status = 'available'
  and not exists (
      select 1
      from invalid_transactions i
      where i.transaction_id = p.transaction_id
  );


create or replace view validation_summary as
select
    current_date as validation_date,

    (select count(*) from stg_payment_transactions) as total_payment_records,

    (select count(*) from stg_bank_transactions) as total_bank_records,

    (select count(*) from valid_transactions) as valid_records,

    (
        select count(distinct transaction_id)
        from invalid_transactions
        where record_source = 'payment_provider'
    ) as invalid_payment_transactions,

    (
        select count(*)
        from invalid_transactions
    ) as total_validation_errors,

    (
        select count(*)
        from invalid_transactions
        where error_code = 'duplicate_transaction_id'
    ) as duplicate_errors,

    (
        select count(*)
        from invalid_transactions
        where error_code = 'missing_amount'
    ) as missing_value_errors,

    (
        select count(*)
        from invalid_transactions
        where error_code = 'invalid_currency'
    ) as currency_errors,

    (
        select count(*)
        from invalid_transactions
        where error_code = 'amount_mismatch'
    ) as amount_mismatch_errors,

        (
        select count(*)
        from invalid_transactions
        where error_code = 'unmatched_bank_transaction'
    ) as unmatched_bank_transactions,

    (
        select count(*)
        from invalid_transactions
        where error_code = 'bank_reference_id_not_extractable'
    ) as bank_reference_id_not_extractable_errors;