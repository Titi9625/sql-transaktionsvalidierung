select count(*) as total_payment_records
from raw_payment_transactions;

select count(*) as total_bank_records
from raw_bank_transactions;

select *
from validation_summary;

select
    error_code,
    count(*) as error_count
from invalid_transactions
group by error_code
order by error_code;

select *
from valid_transactions
order by transaction_id;

select *
from invalid_transactions
order by error_code, transaction_id;