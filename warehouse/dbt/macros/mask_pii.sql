{# Module 5: masking helpers. Marts must use these for any column tagged pii: true. #}
{% macro mask_email(col) -%}
    md5(lower(trim({{ col }})))
{%- endmacro %}

{% macro mask_card_last4(col) -%}
    right({{ col }}, 4)
{%- endmacro %}
