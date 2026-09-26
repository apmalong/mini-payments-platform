# Runbook: stale data (MartDataStale, IngestionStalled, TransformStalled, SourceQuiet)

**What it means:** `analytics.fct_transactions` hasn't changed for more than 15 minutes (the
freshness SLO), so dashboards, the analyst views and the model's training data are out of date.
The stage alerts say where the pipeline stopped:

| Alert | Stalled stage | Freshness pattern |
|---|---|---|
| `SourceQuiet` | Nothing new in the source | source old, raw and mart follow it |
| `IngestionStalled` | Postgres → DuckDB sync | source fresh, raw old |
| `TransformStalled` | dbt | raw fresh, mart old |
| `FreshnessCheckFailing` | The check itself | `payments_check_success{check=...} == 0` |

Grafana's "Is the data fresh?" row shows the three ages side by side.

## Diagnose

1. **Source quiet?** Is the generator (or the real source) producing?
   `docker exec mpp-postgres psql -U payments -c "select max(updated_at), now() from transactions"`
2. **Is Airflow running and the pipeline unpaused?**
   `docker ps --filter name=mpp-airflow`, then `docker exec mpp-airflow airflow dags list`
   (`paused` column). Paused or stopped Airflow explains both stage alerts.
3. **Did a run fail?** Airflow UI → `payments_pipeline` → the red task.
   - `source_contract` failed: the source schema changed. Read the task log, update
     `governance/contracts/transactions.yml` if the change is intended, or fix the producer.
   - `ingest` failed: see the Airbyte connector log path in the task log. Common causes:
     Postgres down, Docker socket not mounted in the Airflow container.
   - a `dbt.*` task failed: a test or contract failed. Fix the data or the model;
     `dbt build --select <model>` on the host reproduces it.
   - `pii_check` failed: a mart exposes unmasked PII. Do not override; fix the model.
4. **Lock contention?** A long query holding `warehouse.duckdb` (DuckDB UI, a notebook) blocks
   the pipeline's writes. Close it.

## Fix and verify

Rerun the failed task in Airflow (Clear → downstream). The mart age on the dashboard should drop
below 15 minutes after the next successful run and the alert resolve within 5 minutes.

If the source is legitimately quiet (no traffic), `SourceQuiet` and `MartDataStale` are expected;
silence them rather than "fixing" the pipeline.
