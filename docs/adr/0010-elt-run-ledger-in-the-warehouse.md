# ADR-0010: An ELT run ledger in the warehouse for cost, efficiency and SLOs

- **Status:** accepted
- **Date:** 2026-09-30

## Context

The pipeline alerted on freshness but couldn't say what a run costs, where its time goes, or
whether it met a 30-day reliability target. Prometheus keeps two days of data, which is too short
for monthly error budgets. Cosmos runs every dbt model as its own process, so each
`target/run_results.json` is overwritten by the next task. DuckDB allows one writer.

## Options considered

1. Airflow's metrics (StatsD) or REST API for run duration and status.
2. Collect `run_results.json` after each Cosmos task (a Cosmos callback) and push to Prometheus.
3. A ledger in the warehouse: the sync and a dbt `on-run-end` hook each append rows to `ops.*`,
   and the exporter computes the metrics from it.
4. A cost and observability package (Elementary, dbt_artifacts).

## Decision

Option 3. The hook runs inside the dbt process that already holds the writer lock, sees every
node's status, timing and adapter response (rows, and on BigQuery bytes billed and slot time),
and works the same whether dbt runs from Cosmos, the host or CI. The ledger keeps full history,
so 30-day SLOs and cost windows are computed by the exporter, not by Prometheus.

Airflow's view (option 1) knows task state but not rows or bytes, and would need auth and StatsD
wiring for less information. Option 4 is the production path; it's heavier than needed here and
doesn't cover the sync.

## Consequences

- Cost on DuckDB is an estimate (busy time at a VM's hourly price); on BigQuery the bytes-billed
  term is real spend. The rates are configuration, not facts.
- The first reading was useful immediately: dbt writes ~51 rows per record ingested because table
  marts are rebuilt in full every run.
- SLOs count steps, not runs: with Cosmos there is no single "dbt run" to judge.
- dbt-duckdb reports no rows affected for incremental models, so their writes are unknown rather
  than estimated.
- The ledger grows by ~50 rows per run (~1.5M a year); fine for DuckDB, but it wants a retention
  job before it outgrows the laptop.
- The exporter's reads are another reader on the warehouse file; it reads every two minutes and
  closes the connection straight away to stay out of the writer's way.
