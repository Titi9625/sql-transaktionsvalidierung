select *
from {{ ref('dbt_validation_summary') }}
where total_payment_records <> 50
   or total_bank_records <> 40
   or valid_records <> 20
   or invalid_payment_transactions <> 25
   or total_validation_errors <> 46
   or duplicate_errors <> 10
   or missing_value_errors <> 3
   or currency_errors <> 3
   or amount_mismatch_errors <> 5
   or unmatched_bank_transactions <> 8
   or bank_reference_id_not_extractable_errors <> 2