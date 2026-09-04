select
    bank_transaction_id,
    booking_date,
    reference_text,
    substring(reference_text from '(txn_[0-9]+|pp_[0-9]+)') as matched_transaction_id,
    amount as bank_amount,
    upper(currency) as currency,
    counterparty,
    imported_at
from {{ source('raw_data', 'raw_bank_transactions') }}