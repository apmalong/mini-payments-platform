# ADR-0001: DuckDB locally, dbt kept portable to BigQuery

- **Status:** accepted
- **Date:** 2026-09-26

## Context

The target stack is BigQuery + dbt, but this platform runs on one laptop, offline, at no cost.
The dbt project should still be credible as BigQuery code.

## Options considered

1. **BigQuery sandbox** for everything: real, but needs network and a GCP account for every run,
   and CI would need credentials.
2. **BigQuery emulator** (goccy/bigquery-emulator): partial SQL coverage, another container.
3. **DuckDB locally, with a `bigquery` dbt target**, keeping adapter-specific SQL behind macros.

## Decision

Option 3. `profiles.yml` has `duckdb` (default) and `bigquery` targets. SQL that differs by
adapter is dispatched: `json_field` (`json_extract_string` vs `json_value`) and `mask_email`
(`dbt.hash`). `fct_transactions` sets `partition_by` / `cluster_by` only when
`target.type == 'bigquery'`.

## Consequences

- Fast, free, offline builds (about 6 s for the whole project) and a CI fixture that's just a file.
- **DuckDB allows one writer per file.** This shaped the platform: Airflow runs one task at a time,
  the host can't read the warehouse while a run writes, and the exporter and agents use short
  read-only connections. On BigQuery none of this applies.
- **Timezones bit twice.** Raw timestamps are naive UTC; DuckDB's session timezone defaults to
  local time, which made freshness read −4 h and would have shifted `transaction_date`.
  Every connection now sets `TimeZone = 'UTC'` (dbt profile, exporter, agent tools).
- Contract types are DuckDB's (`varchar`, `decimal(18,2)`); the BigQuery target would need its own.
  The ML feature model uses `epoch()` windows and is DuckDB-only.
