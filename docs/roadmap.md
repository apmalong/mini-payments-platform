# Platform roadmap

Two quarters, ordered by risk to payments and to trust in the data, then by leverage for the teams
that build on the platform. Each milestone has an exit test, so "done" is observable.

## Q1: make it safe to depend on

| Milestone | Why now | Done when |
|---|---|---|
| **Keyed email hashing** (HMAC-SHA256, key in a secret manager) | Unsalted MD5 is reversible with a list of emails; it's the largest privacy gap | `customer_email_hash` recomputed with HMAC; no raw key in code or config |
| **Real access control** on BigQuery: policy tags on classified columns, authorized views per role | Role views on DuckDB are a simulation (ADR-0004) | An analyst account gets a permission error on a restricted column |
| **Scorer client resilience**: short timeouts, one retry, circuit breaker | A node failure dropped the in-flight requests (ADR-0007) | Node-kill test drops 0 scores |
| **Alert routing** (Alertmanager → on-call) | Alerts are visible but page no one (ADR-0008) | `ScorerDown` in a test reaches a phone within 2 minutes |
| **Source contracts for every fact table** | Only `transactions` has one | `refunds` and `chargebacks` gated like `transactions` |

## Q2: make it faster to build on

| Milestone | Why | Done when |
|---|---|---|
| **Warehouse on BigQuery** (dbt `bigquery` target, partitioned and clustered facts) | Removes the single-writer bottleneck that serializes the pipeline (ADR-0001) | Pipeline runs dbt models in parallel; cost per run dashboarded |
| **Runtime lineage** (OpenLineage from Airflow and dbt, into Marquez or Dataplex) | Static SQL lineage can't say which job wrote what, when | An auditor can trace a mart row to the run that produced it |
| **Feature store decision revisited** (Feast or Vertex AI Feature Store) | Two hand-kept feature implementations (ADR-0005) | One feature definition, skew check green at > 99% |
| **Self-serve sources**: the `new-dbt-source` skill plus a PR template and CI | New sources take a platform engineer | A new source reaches `stg_` with tests in one PR, reviewed not written by the platform team |
| **Model monitoring on matured labels**: weekly precision in the top 1% | Drift is visible; realized quality is not | A weekly report compares live precision with validation |

## Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Model decisions degrade silently as traffic drifts | High (already visible: PSI > 0.25) | Missed fraud or blocked customers | Drift alerts (in place); retrain on matured labels; review-rate guardrail with fraud-ops |
| Labels arrive 5-45 days late and miss ~30% of fraud | Certain | Models learn from incomplete truth | 45-day maturity rule; supplement with fraud-ops dispositions |
| A source schema change breaks ingestion | Medium | Stale data, broken marts | Contracts on all fact tables (Q1) |
| One engineer knows how the platform fits together | High | Slow incidents, blocked teams | ADRs, runbooks, architecture views, the investigate-alert skill |

## Technical debt register

| Item | Where | Cost of leaving it | Plan |
|---|---|---|---|
| Unsalted MD5 email hash | `mask_email` macro | Re-identification risk | Q1 |
| Role views simulate access control | `create_role_views` | No real enforcement locally | Q1 (BigQuery) |
| DuckDB single writer | whole pipeline | Serial tasks, host reads blocked during runs | Q2 (BigQuery) |
| Windowed features implemented twice (SQL and Lua) | dbt + consumer | Skew if one changes | Skew check in CI; Q2 decision |
| Generator's live mode uses cards ~1000x more often than its history | `generator/generate.py` | Distorts drift and review-rate in demos | Make live per-card rate match the backfill |
| Pods don't rebalance after a node recovers | Kubernetes | Next node failure can take all replicas | Descheduler |
| Names aren't redacted by the gateway | `pii_guardrail.py` | Names can reach the model provider | NER-based redaction (Presidio) |
| Sync takes ~60 s, mostly connector start-up | `ingestion/sync.py` | Floor on pipeline latency | Long-running connector or CDC |
