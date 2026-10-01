{#-
    ELT ledger: an on-run-end hook (dbt_project.yml) that records every dbt invocation and every
    node it executed in the `ops` schema. observability/exporter.py turns these rows into cost,
    efficiency and SLO metrics (docs/elt-metrics.md).

    ops.dbt_invocations  one row per dbt command; wall-clock includes parse and compile, so
                         comparing it with node time shows the per-invocation overhead
    ops.dbt_node_runs    one row per model, seed, snapshot or test: status, execution time, rows
                         affected (where the adapter reports it), the relation's row count after a
                         table, incremental or snapshot build and, on BigQuery, bytes billed and slot
                         time

    Written from the same connection dbt already holds, so it needs no second DuckDB writer.
-#}
{% macro record_run_results(results) %}
    {% if not execute or not results %}{{ return('') }}{% endif %}

    {% set string, bigint, ts = dbt.type_string(), dbt.type_bigint(), dbt.type_timestamp() %}
    {% set double = 'double' if target.type == 'duckdb' else dbt.type_float() %}
    {% set ops = api.Relation.create(database=target.database, schema='ops') %}
    {% do adapter.create_schema(ops) %}
    {% do run_query(
        'create table if not exists ' ~ ops.schema ~ '.dbt_invocations ('
        ~ 'invocation_id ' ~ string ~ ', command ' ~ string ~ ', target ' ~ string ~ ', airflow_run_id ' ~ string
        ~ ', started_at ' ~ ts ~ ', finished_at ' ~ ts ~ ', nodes ' ~ bigint ~ ')') %}
    {% do run_query(
        'create table if not exists ' ~ ops.schema ~ '.dbt_node_runs ('
        ~ 'invocation_id ' ~ string ~ ', unique_id ' ~ string ~ ', name ' ~ string ~ ', resource_type ' ~ string
        ~ ', materialized ' ~ string ~ ', status ' ~ string ~ ', started_at ' ~ ts ~ ', completed_at ' ~ ts
        ~ ', execution_seconds ' ~ double ~ ', rows_affected ' ~ bigint ~ ', relation_rows ' ~ bigint ~ ', failures ' ~ bigint
        ~ ', bytes_processed ' ~ bigint ~ ', bytes_billed ' ~ bigint ~ ', slot_ms ' ~ bigint
        ~ ', message ' ~ string ~ ')') %}

    {% set fmt = '%Y-%m-%d %H:%M:%S.%f' %}
    {% set finished_at = modules.datetime.datetime.now(modules.pytz.utc) %}
    {% do run_query(
        'insert into ' ~ ops.schema ~ '.dbt_invocations values ('
        ~ ledger_value(invocation_id) ~ ', ' ~ ledger_value(flags.WHICH) ~ ', ' ~ ledger_value(target.name)
        ~ ', ' ~ ledger_value(env_var('AIRFLOW_CTX_DAG_RUN_ID', '') or none)
        ~ ', cast(' ~ ledger_value(run_started_at.strftime(fmt)) ~ ' as ' ~ ts ~ ')'
        ~ ', cast(' ~ ledger_value(finished_at.strftime(fmt)) ~ ' as ' ~ ts ~ ')'
        ~ ', ' ~ results | length ~ ')') %}

    {% set rows = [] %}
    {% for result in results %}
        {% set response = result.adapter_response or {} %}
        {% set execute_timing = result.timing | selectattr('name', 'equalto', 'execute') | list %}
        {% set started_at, completed_at = none, none %}
        {% if execute_timing and execute_timing[0].started_at %}
            {% set started_at = execute_timing[0].started_at.strftime(fmt) %}
            {% set completed_at = execute_timing[0].completed_at.strftime(fmt) %}
        {% endif %}
        {% set relation_rows = none %}
        {% if result.status | string == 'success' and result.node.config.materialized in ('table', 'incremental', 'snapshot') %}
            {% set relation = adapter.get_relation(result.node.database, result.node.schema, result.node.alias) %}
            {% if relation is not none %}
                {% set relation_rows = run_query('select count(*) from ' ~ relation).columns[0].values()[0] %}
            {% endif %}
        {% endif %}
        {% do rows.append('(' ~ [
            ledger_value(invocation_id),
            ledger_value(result.node.unique_id),
            ledger_value(result.node.name),
            ledger_value(result.node.resource_type),
            ledger_value(result.node.config.materialized),
            ledger_value(result.status | string),
            'cast(' ~ ledger_value(started_at) ~ ' as ' ~ ts ~ ')',
            'cast(' ~ ledger_value(completed_at) ~ ' as ' ~ ts ~ ')',
            ledger_value(result.execution_time),
            ledger_value(response.get('rows_affected')),
            ledger_value(relation_rows),
            ledger_value(result.failures),
            ledger_value(response.get('bytes_processed')),
            ledger_value(response.get('bytes_billed')),
            ledger_value(response.get('slot_ms')),
            ledger_value((result.message or '') | string | truncate(500, true, '...') if result.status | string in ('error', 'fail', 'warn') else none),
        ] | join(', ') ~ ')') %}
    {% endfor %}
    {% do run_query('insert into ' ~ ops.schema ~ '.dbt_node_runs values ' ~ rows | join(', ')) %}
    {{ return('') }}
{% endmacro %}

{# A SQL literal: null, a number, or a quoted string. Negative row counts mean "unknown". #}
{% macro ledger_value(value) %}
    {%- if value is none or (value is number and value < 0) -%}
        {{ return('null') }}
    {%- elif value is number -%}
        {{ return(value | string) }}
    {%- else -%}
        {{ return("'" ~ (value | string | replace("'", "''")) ~ "'") }}
    {%- endif -%}
{% endmacro %}
