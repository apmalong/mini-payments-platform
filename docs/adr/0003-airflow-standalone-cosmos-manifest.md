# ADR-0003: Airflow standalone, Cosmos in manifest mode, one task at a time

- **Status:** accepted
- **Date:** 2026-09-26

## Context

The pipeline (contract check → ingest → dbt → PII check → role views → publish) needs an
orchestrator with retries, backfills and data-aware triggering, within 8 GB alongside everything else.

## Options considered

1. **Official Airflow docker-compose** (Celery, Redis, six services): realistic, heavy.
2. **`airflow standalone`** in one container with the LocalExecutor and metadata in the existing Postgres.
3. **KubernetesExecutor on the kind cluster**: planned for Module 8, deferred.

## Decision

Option 2, Airflow 3.3. dbt runs through Cosmos (one task per model, tests after each) in
**manifest mode**. dbt, PyAirbyte and ML each get a virtualenv inside the image.
`max_active_tasks=1`, `max_active_runs=1`.

## Consequences

- **Manifest mode** because `dbt ls` at parse time exceeded the 30 s DAG import timeout on the
  Windows bind mount. The cost: run `dbt parse` after changing models.
- One task at a time is forced by DuckDB's single writer (ADR-0001).
- **Pausing a DAG freezes runs already in progress**, not just new ones: a paused pipeline left a
  run stuck after `ingest`. Pause only when nothing is running.
- `//var/run/docker.sock` (leading `//`) is needed on Windows, or Compose mounts an empty folder.
- `dbt run-operation` doesn't commit: from Module 5 on, the role-views macro and its Airflow task
  logged success while creating nothing, until the agent query tool found the schema empty. It now
  commits explicitly, and verification queries the schema instead of trusting the log.
- Retraining is a separate DAG triggered by the `fct_transactions` asset (ADR-0005).
