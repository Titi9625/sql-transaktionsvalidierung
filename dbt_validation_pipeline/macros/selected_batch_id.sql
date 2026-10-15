{% macro selected_batch_id() -%}
    {%- set value = var('batch_id', none) -%}
    {%- if value is not none -%}
        {%- if value is not string or not modules.re.fullmatch('[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', value) -%}
            {{ exceptions.raise_compiler_error('batch_id must be a UUID in 8-4-4-4-12 format') }}
        {%- endif -%}
        {{- value | lower -}}
    {%- endif -%}
{%- endmacro %}
