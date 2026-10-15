{% set expected = var('expected_error_counts', none) %}
{{ config(enabled=(expected is not none)) }}
{% set codes = ['duplicate_transaction_id','missing_amount','invalid_currency',
    'pending_transaction','refund_transaction','amount_mismatch','missing_bank_match',
    'unmatched_bank_transaction','bank_reference_id_not_extractable'] %}
{% if expected is not none %}
    {% if expected is not mapping %}
        {{ exceptions.raise_compiler_error('expected_error_counts must be a mapping') }}
    {% endif %}
    {% for code in codes %}
        {% if expected.get(code, 0) is not integer or expected.get(code, 0) < 0 %}
            {{ exceptions.raise_compiler_error('expected_error_counts must contain non-negative integer counts') }}
        {% endif %}
    {% endfor %}
{% for code in codes %}
select '{{ code }}' as issue
where (select count(*) from {{ ref('dbt_invalid_transactions') }}
       where error_code = '{{ code }}') <> {{ expected.get(code, 0) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}
{% else %}
select 'no_exact_error_counts_requested' as issue where false
{% endif %}
