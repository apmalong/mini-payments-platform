# Architecture

A local payments data platform: a synthetic card-payments source, batch ELT into a warehouse,
real-time fraud features and scoring, governance controls, MLOps, Kubernetes and observability.
Each section is one view; the [ADRs](adr/README.md) record why it's built this way.

## 1. System overview

What exists and how data moves. Batch flows left to right along the top, real time along the
bottom; both meet in the model.

```mermaid
flowchart LR
  subgraph source["Source"]
    GEN["generator<br/>synthetic payments"] --> PG[("Postgres<br/>transactions, refunds,<br/>chargebacks, merchants")]
  end

  subgraph batch["Batch · Airflow, 15 min"]
    PG -->|"PyAirbyte<br/>incremental on updated_at"| RAW[("DuckDB raw")]
    RAW -->|dbt| STG["staging<br/>dedupe, JSON, types"]
    STG --> MARTS[("marts<br/>fct_transactions, dims,<br/>daily volume")]
    STG --> MLF[("ml_transaction_features<br/>point-in-time")]
    MARTS --> ROLES["role views<br/>analyst / fraud_ops"]
  end

  subgraph realtime["Real time"]
    GEN -->|events| RP[["Redpanda<br/>payments.transactions"]]
    RP --> CONS["feature consumer"]
    CONS <-->|"Lua: dedupe + windows"| REDIS[("Redis<br/>online features")]
    CONS -->|"POST /score"| SCORER["fraud scorer<br/>(Kubernetes)"]
    CONS --> PARQ[("Parquet<br/>offline copy of<br/>online features")]
  end

  subgraph ml["Model lifecycle"]
    MLF --> TRAIN["train.py"] --> REG[("MLflow registry<br/>fraud@champion")]
    REG -->|"alias polled every 30 s"| SCORER
  end

  PARQ -.->|"skew check"| MLF
  MARTS -.->|"bootstrap"| REDIS
```

## 2. Deployment view

Where each piece runs, and its port on localhost. Everything is one machine: Docker Desktop
(WSL2, 8 GB) hosts the containers and the kind cluster, whose nodes are themselves containers.
Data flow is in the overview above; this view is only about placement.

```mermaid
flowchart TB
  subgraph host["Host processes"]
    direction TB
    H1["generator"]
    H2["feature consumer<br/>:8000"]
    H3["freshness + drift exporter<br/>:8001"]
    H4["portal<br/>:8099"]
    H5[("warehouse.duckdb")]
  end
  subgraph docker["Docker · network mpp"]
    direction TB
    D1[("Postgres<br/>:5433")]
    D2[["Redpanda<br/>:9092 · console :8085"]]
    D3[("Redis :6379<br/>RedisInsight :5540")]
    D4["MLflow<br/>:5000"]
    D5["Airflow<br/>:8088"]
    D6["Grafana<br/>:3000"]
  end
  subgraph kind["kind cluster · 3 nodes"]
    direction TB
    K1["fraud-scorer · 2-5 pods<br/>namespace fraud · :8091"]
    K2["Prometheus<br/>namespace monitoring · :9090"]
  end
  host ~~~ docker ~~~ kind
```

The connections that cross a boundary, and how:

| From | To | How |
|---|---|---|
| Airflow (Docker) | Postgres source | PyAirbyte starts the Java connector as a sibling container through the mounted Docker socket; the project is mounted at the path Docker's VM uses, so the connector can read its config |
| Airflow (Docker) | `warehouse.duckdb` (host disk) | bind mount; one writer at a time |
| scorer pods (kind) | MLflow, Redis (Docker) | `host.docker.internal` |
| Prometheus (kind) | consumer, exporter (host) | `host.docker.internal:8000` / `:8001` |
| localhost | scorer, Prometheus (kind) | NodePort 30080 is mapped to :8091 at cluster creation; :9090 goes through the `mpp-prometheus-port` socat container, because kind's port mappings can't be changed later |
| Grafana (Docker) | Prometheus (kind) | `host.docker.internal:9090`, via the same bridge |

## 3. Batch pipeline

The `payments_pipeline` DAG. Governance checks are gates: a failure stops the run before bad data
reaches consumers. DuckDB allows one writer, so tasks run one at a time.

```mermaid
flowchart LR
  A["source_contract<br/>schema vs YAML contract"] -->|"breaking change<br/>stops the run"| B["ingest<br/>PyAirbyte → raw"]
  B --> C["dbt (Cosmos)<br/>one task per model,<br/>tests after each"]
  C --> D["pii_check<br/>column lineage:<br/>no unmasked PII in marts"]
  D --> E["role_views<br/>analyst / fraud_ops"]
  E --> F["publish<br/>asset: fct_transactions"]
  F -.->|"asset trigger"| G["fraud_model_training<br/>skip if labels unchanged"]
```

## 4. Real-time scoring path

One card transaction, from event to decision. The scorer fails open: if it's down, the event is
still processed and its features stored, without a score.

```mermaid
sequenceDiagram
  autonumber
  participant G as generator
  participant R as Redpanda
  participant C as feature consumer
  participant X as Redis
  participant S as fraud scorer (k8s)
  participant P as Parquet

  G->>R: transaction event (keyed by card_token)
  R->>C: poll
  C->>C: late? (> 10 min behind watermark)
  alt late event
    C->>P: store, is_late = true (no online update)
  else on time
    C->>X: Lua script: dedupe key, update card window,<br/>IP countries, merchant stats (atomic)
    X-->>C: features as of this event (or "duplicate")
    C->>S: POST /score {event, features}
    S-->>C: fraud_score, decision, model_version
    C->>P: store event + features + score
  end
  C->>R: commit offsets after each Parquet flush
```

## 5. Model lifecycle

Training and serving compute the windowed features twice, from the same definitions:
point-in-time in dbt for training, incrementally in the consumer for serving. `check_skew.py`
compares the two for the same transactions. Every other feature is derived by
`ml/features.py`, which training and serving both import, so those can't drift apart.

```mermaid
flowchart LR
  F1[("ml_transaction_features<br/>dbt, point-in-time")] --> T["train.py<br/>time split, LightGBM"]
  T -->|register| V["new version<br/>alias: challenger"]
  V -->|"beats champion on the<br/>same validation set,<br/>p99 under 20 ms"| CH["alias: champion"]
  CH -->|"polled every 30 s,<br/>swapped without restart"| S["scorer pods"]
  F2[("Redis<br/>online features")] --> S
  F1 -.-|"check_skew.py:<br/>same transactions,<br/>match rate per feature"| F2
```

## 6. Observability

What is measured where, and what alerts on it. Each alert has a runbook in [runbooks/](runbooks/).

```mermaid
flowchart LR
  subgraph sources["Metric sources"]
    M1["scorer pods<br/>requests by status, latency,<br/>decisions, model version"]
    M2["feature consumer<br/>events by outcome, lag"]
    M3["exporter<br/>freshness per layer, feature PSI,<br/>ELT cost, efficiency, 30-day SLOs"]
  end
  L[("ops ledger in the warehouse<br/>sync.py + dbt on-run-end hook")] --> M3
  M1 --> P["Prometheus<br/>recording rules: 3 SLOs<br/>17 alert rules"]
  M2 --> P
  M3 --> P
  P --> G["Grafana<br/>Payments platform dashboard"]
  P --> A["alerts → runbooks<br/>stale data by stage, consumer,<br/>scorer, drift, ELT budgets and cost"]
```
