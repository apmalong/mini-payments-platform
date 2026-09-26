# Orchestration (Module 4)

One container running `airflow standalone` (API server, scheduler, DAG processor, triggerer)
with the LocalExecutor. Metadata lives in the project's Postgres, database `airflow`.

```powershell
# once: metadata database, dbt packages and manifest
docker exec mpp-postgres psql -U payments -c "create database airflow"
cd ..\warehouse\dbt; uv run dbt deps --profiles-dir .; uv run dbt parse --profiles-dir .; cd ..\..\orchestration

docker compose --env-file ../.env up -d --build     # UI: http://localhost:8088 (no login)
docker exec mpp-airflow airflow dags unpause payments_pipeline
docker exec mpp-airflow airflow dags trigger payments_pipeline
docker compose --env-file ../.env down
```

## The DAG: `payments_pipeline`

`ingest` → `dbt` task group → `publish`, every 15 minutes.

- **ingest** runs `ingestion/sync.py` in its own virtualenv inside the image.
- **dbt** is rendered by Cosmos: one run task per model/seed/snapshot, each followed by its tests.
- **publish** emits the `fct_transactions` asset for downstream DAGs (Module 7 training).
- Retries with exponential backoff; `on_failure_callback` logs and posts to `ALERT_WEBHOOK_URL` if set.
- `max_active_tasks=1` and `max_active_runs=1`: DuckDB allows one writer, so dbt models run one at a time.
  While a run is in progress, the host can't open `warehouse.duckdb` either (not even read-only).

## Things that are specific to this setup

- **Docker inside Airflow.** PyAirbyte starts the connector as a sibling container through the
  mounted Docker socket. Docker resolves bind-mount paths on its VM, not inside the Airflow
  container, so the project is mounted at `HOST_PROJECT_PATH` (the path Docker's VM uses for it,
  `/run/desktop/mnt/host/c/...` on Docker Desktop) and PyAirbyte's `TMPDIR` points inside it.
- **`//var/run/docker.sock`.** The leading `//` stops Compose on Windows from turning the socket
  path into a Windows path, which mounts an empty folder instead of the socket.
- **Manifest mode.** Cosmos renders tasks from `warehouse/dbt/target/manifest.json`. Running
  `dbt ls` at parse time exceeded the 30s DAG import timeout on the Windows bind mount.
  Run `dbt parse` on the host after changing models.
- **Port 8088**, because a local Apache uses 8080.
