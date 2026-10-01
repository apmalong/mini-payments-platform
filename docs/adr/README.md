# Architecture decision records

Why the platform is built the way it is. Each record keeps the options that were rejected and
what the decision cost, including what went wrong while building it.

| ADR | Decision |
|---|---|
| [0001](0001-duckdb-locally-bigquery-portable-dbt.md) | DuckDB locally, dbt kept portable to BigQuery |
| [0002](0002-pyairbyte-incremental-ingestion.md) | PyAirbyte for ingestion, incremental on `updated_at` |
| [0003](0003-airflow-standalone-cosmos-manifest.md) | Airflow standalone, Cosmos in manifest mode, one task at a time |
| [0004](0004-column-lineage-pii-enforcement.md) | Enforce PII rules with column-level lineage, not column names |
| [0005](0005-dbt-and-redis-instead-of-feast.md) | dbt + Redis for features instead of Feast, with a skew check |
| [0006](0006-custom-stream-consumer.md) | A small custom consumer with Redis Lua, not a stream processor |
| [0007](0007-scorer-on-kubernetes.md) | Run the scorer on Kubernetes with explicit rollout and failure settings |
| [0008](0008-prometheus-in-cluster-psi-drift.md) | Prometheus in the cluster, and PSI for drift instead of Evidently |
| [0009](0009-ai-gateway-and-agent-guardrails.md) | An AI gateway for every LLM call; agents constrained at the data layer |
| [0010](0010-elt-run-ledger-in-the-warehouse.md) | An ELT run ledger in the warehouse for cost, efficiency and SLOs |

New records copy [0000-template.md](0000-template.md) and take the next number. A decision that
changes is superseded by a new record, not edited.
