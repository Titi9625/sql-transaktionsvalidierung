{% set batch_id = selected_batch_id() | trim %}
{% set expected = var('expected_summary', none) %}
{% if not batch_id and expected is none %}
    {% set expected = {
        'total_payment_records':50, 'total_bank_records':40, 'valid_records':20,
        'invalid_payment_transactions':25, 'total_validation_errors':46,
        'duplicate_errors':10, 'missing_value_errors':3, 'currency_errors':3,
        'amount_mismatch_errors':5, 'unmatched_bank_transactions':8,
        'bank_reference_id_not_extractable_errors':2
    } %}
{% endif %}
{{ config(enabled=(expected is not none)) }}
{% set fields = ['total_payment_records','total_bank_records','valid_records',
    'invalid_payment_transactions','total_validation_errors','duplicate_errors',
    'missing_value_errors','currency_errors','amount_mismatch_errors',
    'unmatched_bank_transactions','bank_reference_id_not_extractable_errors'] %}
{% if expected is not none %}
    {% if expected is not mapping %}
        {{ exceptions.raise_compiler_error('expected_summary must be a mapping') }}
    {% endif %}
    {% for field in fields %}
        {% if expected.get(field) is not integer or expected.get(field) < 0 %}
            {{ exceptions.raise_compiler_error('expected_summary requires a non-negative integer for ' ~ field) }}
        {% endif %}
    {% endfor %}
select * from {{ ref('dbt_validation_summary') }}
where
{% for field in fields %}
    {{ field }} <> {{ expected[field] }}{% if not loop.last %} or{% endif %}
{% endfor %}
{% else %}
select * from {{ ref('dbt_validation_summary') }} where false
{% endif %}
