{#- Extract a scalar from a JSON string column. Paths use dots for nesting: 'three_ds.authenticated'.
    Dispatched so the same models run on DuckDB and BigQuery. -#}
{% macro json_field(column, path) -%}
    {{ return(adapter.dispatch('json_field')(column, path)) }}
{%- endmacro %}

{% macro default__json_field(column, path) -%}
    json_extract_string({{ column }}, '$.{{ path }}')
{%- endmacro %}

{% macro bigquery__json_field(column, path) -%}
    json_value({{ column }}, '$.{{ path }}')
{%- endmacro %}
