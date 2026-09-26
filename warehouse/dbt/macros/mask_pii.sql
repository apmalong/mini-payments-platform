{#- Module 5: masking helpers. Marts must use these for any column tagged pii: true. -#}
{% macro mask_email(column) -%}
    {{ dbt.hash("lower(trim(" ~ column ~ "))") }}
{%- endmacro %}

{% macro mask_card_last4(column) -%}
    right({{ column }}, 4)
{%- endmacro %}
