{#-
    Simulates column-level access control on DuckDB, which has no users or grants: one schema
    per role (role_analyst, role_fraud_ops) holding views with only the columns that role may
    read, decided by each column's `classification` meta tag (default: internal).

    On BigQuery the same policy maps to policy tags on columns (column-level security) plus
    authorized views per role; this macro is the local stand-in.
-#}
{% macro column_classification(node, column_name) %}
    {%- set column = node.columns.get(column_name) or node.columns.get(column_name | lower) -%}
    {%- if column is none -%}
        {{ return('internal') }}
    {%- endif -%}
    {%- set meta = column.meta or {} -%}
    {%- if not meta and column.config is defined and column.config.meta is defined -%}
        {%- set meta = column.config.meta or {} -%}
    {%- endif -%}
    {{ return(meta.get('classification', 'internal')) }}
{% endmacro %}

{% macro create_role_views() %}
    {% if not execute %}{{ return('') }}{% endif %}
    {% set roles = var('access_roles') %}
    {% for role, policy in roles.items() %}
        {% set role_schema = 'role_' ~ role %}
        {% do run_query('drop schema if exists ' ~ role_schema ~ ' cascade') %}
        {% do run_query('create schema ' ~ role_schema) %}
        {% set created = [] %}
        {% for node in graph.nodes.values()
            if node.resource_type == 'model'
            and node.config.materialized != 'ephemeral'
            and node.fqn[1] in policy.layers %}
            {% set relation = adapter.get_relation(node.database, node.schema, node.alias) %}
            {% if relation is not none %}
                {% set allowed = [] %}
                {% for column in adapter.get_columns_in_relation(relation) %}
                    {% if column_classification(node, column.name) in policy.classifications %}
                        {% do allowed.append(adapter.quote(column.name)) %}
                    {% endif %}
                {% endfor %}
                {% if allowed %}
                    {% do run_query('create view ' ~ role_schema ~ '.' ~ node.alias ~ ' as select '
                                    ~ allowed | join(', ') ~ ' from ' ~ relation) %}
                    {% do created.append(node.alias ~ ' (' ~ allowed | length ~ ' columns)') %}
                {% endif %}
            {% endif %}
        {% endfor %}
        {% do log(role_schema ~ ': ' ~ created | join(', '), info=true) %}
    {% endfor %}
    {#- run-operation doesn't commit on its own: without this the views are rolled back when it
        exits, while the log above still reports them as created. -#}
    {% do adapter.commit() %}
{% endmacro %}
