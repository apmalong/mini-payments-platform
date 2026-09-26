# Mini Payments Data Platform

Local capstone project for the lesson plan in `../lesson-plan.md`: a small fictional card-payments
data platform (ingest → model → orchestrate → govern → stream → ML → AI gateway).

## Layout

| Folder | Module | Purpose |
|---|---|---|
| `generator/` | 1 | Synthetic payments data + event producer |
| `ingestion/` | 2 | PyAirbyte / Airbyte configs |
| `warehouse/dbt/` | 3 | dbt project (duckdb + bigquery targets) |
| `orchestration/` | 4 | Airflow DAGs |
| `governance/` | 5 | Data contracts, PII policy, checks |
| `streaming/` | 6 | Real-time feature consumer |
| `ml/` | 7 | Training, feature store, serving, drift monitoring |
| `k8s/` | 8 | kind cluster, Helm values, manifests |
| `observability/` | 9 | Prometheus, Grafana, alert rules |
| `gateway/` | 10A | LiteLLM AI gateway config |
| `.claude/skills/` | 10B | Agentic dev tooling |
| `docs/` | 11 | Architecture, ADRs, runbooks, roadmap |

## Module 1: source database

Postgres runs on host port **5433**, because a local PostgreSQL 13 service already uses 5432.

```powershell
# start Postgres (plain docker run: the installed Compose 2.0 beta ignores profiles)
docker run -d --name mpp-postgres -e POSTGRES_USER=payments -e POSTGRES_PASSWORD=payments `
  -e POSTGRES_DB=payments -p 5433:5432 -v mpp_pgdata:/var/lib/postgresql/data postgres:16

cd generator
uv run python generate.py --reset                 # seed + 90-day backfill (~50k txns, ~20s)
uv run python generate.py --continuous            # live inserts + status updates, Ctrl+C to stop
uv run python generate.py --help                  # all options

# look at the data
docker exec -it mpp-postgres psql -U payments
```

## Module 2: ingestion into DuckDB

PyAirbyte runs the Java `source-postgres` connector in Docker and loads the `raw` schema of
`warehouse.duckdb`. Dimensions are fully refreshed; facts sync incrementally on `updated_at`.

```powershell
cd ingestion
uv run python sync.py --check          # test the connection
uv run python sync.py --full-refresh   # first load, or after the cursor gets out of step
uv run python sync.py                  # incremental: only rows changed since the last sync
uv run python sync.py --every 60       # keep syncing every minute
```

On Windows, PyAirbyte reports a spurious `AirbyteSubprocessFailedError` (exit code 1) whenever it
stops a connector container, because Windows kills processes with exit code 1 where Linux uses -15.
`sync.py` suppresses that specific cleanup error; real failures still raise.

DuckDB allows one writer per file: stop `sync.py --every` before running another sync or dbt
against `warehouse.duckdb`.

## Quick start (all services)

Needs a current Docker Compose. Compose 2.0.0-beta ignores `profiles:` and tries to start everything.

```bash
cp .env.example .env
make up-core          # Postgres source DB
make up-streaming     # + Redpanda, Redis
make up-ml            # + MinIO, MLflow
make up-gateway       # + LiteLLM
make up-obs           # + Prometheus, Grafana
make down
```

Only bring up the profiles the current module needs.
