# Mini Payments Data Platform

Local capstone project for the lesson plan in `../lesson-plan.md`: a small fictional card-payments
data platform (ingest → model → orchestrate → govern → stream → ML → AI gateway).

## Portal

Every local UI, with live up/down status and start commands:

```powershell
uv run python -m http.server 8099 --bind 127.0.0.1 -d portal   # then open http://localhost:8099
```

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
# start Postgres (or: docker compose --env-file .env --profile core up -d)
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

## Module 3: dbt models

Stop `sync.py --every` first: DuckDB allows one writer at a time.

```powershell
cd warehouse\dbt
uv run dbt deps --profiles-dir .
uv run dbt build --profiles-dir .          # seed, snapshot, models and tests in dependency order
uv run dbt docs generate --profiles-dir .
uv run dbt docs serve --profiles-dir . --port 8081   # lineage graph at http://localhost:8081 (8080 is Apache)
```

| Layer | Models |
|---|---|
| staging (views) | `stg_transactions` (deduped, JSON payload extracted), `stg_merchants`, `stg_customers`, `stg_cards`, `stg_refunds`, `stg_chargebacks` |
| intermediate (ephemeral) | `int_transaction_adjustments`: refunds + chargebacks per transaction |
| snapshot | `snap_merchants`: SCD2 merchant history |
| marts (tables) | `fct_transactions` (incremental), `dim_merchants`, `mart_merchant_daily_volume` |

`fct_transactions` reprocesses a transaction when its own row changes *or* a refund/chargeback
lands against it later (`last_changed_at`). On the `bigquery` target it is partitioned by
`transaction_date` and clustered by `merchant_id`; `json_field` and `mask_email` are
dispatched so the same SQL runs on both.

Expected warnings: 501+ duplicate `transaction_id`s in `raw` (client retries, removed in staging),
and a few transactions refunded above their amount (generator bug the test is there to catch).

## Module 4: Airflow

`payments_pipeline` runs ingest → dbt (one task per model, tests after each) → publish every
15 minutes, in a single `airflow standalone` container. UI at http://localhost:8088.
Setup and the Windows-specific details are in [orchestration/README.md](orchestration/README.md).

## Module 5: governance

The controls, evidence and known gaps are written up for auditors in
[docs/governance.md](docs/governance.md). The pipeline now runs
`source_contract → ingest → dbt → pii_check → role_views → publish`.

```powershell
ingestion\.venv\Scripts\python governance\check_contracts.py              # source schema vs contract
warehouse\dbt\.venv\Scripts\python governance\check_pii.py --report docs\pii-lineage.md
cd warehouse\dbt; uv run dbt run-operation create_role_views --profiles-dir .
```

- **Mart contracts**: names and types enforced at build time.
- **PII lineage check**: column-level lineage through compiled SQL (sqlglot); fails on any mart
  column derived from a `pii: true` column without hashing, even if renamed.
- **Role views**: `role_analyst` / `role_fraud_ops` schemas, filtered by column classification.
- **Volume anomaly test**: last complete day vs a 14-day baseline.

## Module 6: streaming

Redpanda (Kafka API), Redis (online features) and Redpanda Console, on a shared `mpp` network:

```powershell
docker network create mpp
docker run -d --name mpp-redpanda --network mpp -p 9092:9092 -p 18081:8081 -p 9644:9644 redpandadata/redpanda:latest `
  redpanda start --mode dev-container --smp 1 `
  --kafka-addr internal://0.0.0.0:29092,external://0.0.0.0:9092 `
  --advertise-kafka-addr internal://mpp-redpanda:29092,external://localhost:9092 `
  --schema-registry-addr 0.0.0.0:8081
docker run -d --name mpp-redis --network mpp -p 6379:6379 redis:7
docker run -d --name mpp-redpanda-console --network mpp -p 8085:8080 -e KAFKA_BROKERS=mpp-redpanda:29092 redpandadata/console:latest
docker exec mpp-redpanda rpk topic create payments.transactions -p 3   # keyed by card_token

cd streaming; uv run python consumer.py            # features -> Redis + Parquet, metrics on :8000
cd generator; uv run python generate.py --continuous --stream
```

Console: http://localhost:8085. Features for a card: `docker exec mpp-redis redis-cli hgetall features:card:<card_token>`.

The consumer dedupes retries, updates card windows (count 5m, sum 1h, time since last), new-IP-country
and merchant amount z-score in one atomic Redis Lua script, routes events more than 10 minutes behind
the watermark to the offline store only, skips malformed events, and commits Kafka offsets only after
each Parquet flush to `streaming/offline/transaction_features/`.

## Module 7: MLOps

MLflow at http://localhost:5000 (`docker compose --env-file .env --profile ml up -d`, or see the
compose file for the `--allowed-hosts` it needs). Artifacts are proxied by MLflow, so no S3/MinIO.

```powershell
cd warehouse\dbt; uv run dbt build --profiles-dir . --select +ml_transaction_features   # training features
cd ml; uv run python train.py                        # train, log, register; promote if it beats champion
cd ml\serve; ..\.venv\Scripts\python -m uvicorn app:app --port 8090   # serves fraud@champion
cd streaming; uv run python bootstrap_online_store.py --reset          # load Redis from the warehouse
cd streaming; uv run python consumer.py --score-url http://127.0.0.1:8090/score
cd ml; uv run python check_skew.py --since 2026-09-26T21:00:00   # online vs offline features
```

- **One feature definition, two computations.** `ml/features.py` derives payload features for
  both training and serving; windowed features come from dbt (`ml_transaction_features`,
  point-in-time) offline and from the consumer's Lua script online. `check_skew.py` compares them.
- **Cold start is skew.** Without `bootstrap_online_store.py`, online merchant z-scores matched
  offline 1.4% of the time; with it, 85%.
- **Promotion is the deploy.** `train.py` registers each model as `challenger` and moves `champion`
  only if PR-AUC beats the current champion on the same validation set and p99 latency is under
  20 ms. The scorer polls the alias every 30 s and swaps models without a restart.
- **Retraining**: the `fraud_model_training` DAG runs on the `fct_transactions` asset and skips
  when the mature training set (labels older than 45 days) hasn't changed.
- Use `127.0.0.1`, not `localhost`, for the scorer URL: on Windows `localhost` tries IPv6 first
  and each request stalls ~2 s.

## Module 8: Kubernetes

A 3-node kind cluster running the fraud scorer from a Helm chart; MLflow and Redis stay on the
Docker host (`host.docker.internal`).

```powershell
cd k8s
kind create cluster --config kind.yaml                         # 1 control plane + 2 workers
docker build -f ..\ml\serve\Dockerfile -t fraud-scorer:local ..
kind load docker-image fraud-scorer:local --name payments
helm repo add metrics-server https://kubernetes-sigs.github.io/metrics-server/
helm upgrade --install metrics-server metrics-server/metrics-server -n kube-system --set "args={--kubelet-insecure-tls}"
helm upgrade --install fraud-scorer helm/fraud-scorer -n fraud --create-namespace --wait
..\ml\.venv\Scripts\python loadtest.py --duration 120 --concurrency 24   # http://127.0.0.1:8091
```

The chart (`k8s/helm/fraud-scorer`): Deployment with CPU/memory requests and limits, non-root and
read-only root filesystem, readiness on `/readyz` (model loaded) and liveness on `/healthz`,
`maxUnavailable: 0` rolling updates with a `preStop` sleep, a HorizontalPodAutoscaler (2-5 pods at
60% CPU), a PodDisruptionBudget, and a NodePort mapped to localhost:8091.

Docker Desktop's WSL VM defaults to half the machine's RAM (8 GB here); stop Airflow and run
MLflow with `--workers 1` while the cluster is up.

## Quick start (all services)

Needs Docker Desktop 4.x (Compose v2 with profile support); tested with 4.91 / Engine 29.8.

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
