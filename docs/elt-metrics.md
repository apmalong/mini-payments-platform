# ELT cost, efficiency and SLOs

How the batch pipeline (sync → dbt) is measured: what each run costs, how much of that work is
useful, and whether it meets its SLOs ([slos.md](slos.md)). Shown in the Grafana row "What does
ELT cost, and is it meeting its SLOs?"; decision in
[ADR-0010](adr/0010-elt-run-ledger-in-the-warehouse.md).

## Where the numbers come from

Every run writes to the warehouse's `ops` schema, from the process that already holds the DuckDB
writer lock:

| Table | Written by | One row per |
|---|---|---|
| `ops.ingest_runs` | `ingestion/sync.py`, success or failure | sync: duration, records read, rows added, error |
| `ops.dbt_invocations` | dbt `on-run-end` hook (`macros/record_run_results.sql`) | dbt command: wall-clock including parse and compile |
| `ops.dbt_node_runs` | the same hook | model, seed, snapshot or test: status, execution time, rows, bytes billed and slot time (BigQuery) |

Both carry the Airflow run id when there is one (`AIRFLOW_CTX_DAG_RUN_ID`), so a run can be
reassembled from its steps. The exporter reads the ledger every two minutes and publishes 24-hour,
7-day and 30-day figures. Prometheus keeps two days; the ledger keeps every run, so it is the
source of truth for anything longer.

Ask the ledger directly for anything the dashboard doesn't show, for example the slowest models this week:

```sql
select n.name, count(*) as runs, round(sum(n.execution_seconds)) as total_s,
       round(avg(n.execution_seconds), 2) as avg_s
from ops.dbt_node_runs n join ops.dbt_invocations i using (invocation_id)
where i.started_at >= now() - interval 7 day and n.resource_type = 'model'
group by n.name order by total_s desc;
```

## Cost

DuckDB has no bill, so cost is **estimated at list prices** from what the pipeline consumes:

```
cost = (sync seconds + dbt invocation seconds) / 3600 × ELT_COMPUTE_USD_PER_HOUR
     + bytes billed / 2^40 × ELT_BQ_USD_PER_TIB
```

| Setting | Default | Stands for |
|---|---|---|
| `ELT_COMPUTE_USD_PER_HOUR` | 0.134 | a GCE e2-standard-4 on demand (us-central1) running the pipeline |
| `ELT_BQ_USD_PER_TIB` | 6.25 | BigQuery on-demand analysis; bytes billed are 0 on DuckDB |

On the `bigquery` target the bytes term is real spend and dominates; `payments_elt_node_bytes_billed`
shows which model spends it. Check the rates against current pricing before quoting a number.

| Metric | Meaning |
|---|---|
| `payments_elt_estimated_cost_usd{window}` | the estimate for 24h / 7d / 30d |
| `payments_elt_compute_seconds{step, window}` | busy time behind it, `ingest` or `dbt` |
| `payments_elt_bytes_billed{window}` | BigQuery bytes billed |
| `payments_elt_cost_per_million_records_usd{window}` | unit cost: the figure to watch as volume grows |

## Efficiency

| Metric | What it tells you | Lever |
|---|---|---|
| `payments_elt_busy_ratio` | share of the last 24 h the single writer was busy. Past ~70% runs overlap the 15-minute schedule | move slow models to incremental; BigQuery removes the single writer |
| `payments_elt_write_amplification` | rows dbt wrote per record ingested, last 24 h | table marts rebuild every row each run; incremental models write only what changed |
| `payments_elt_dbt_overhead_ratio` | share of dbt wall-clock spent outside node execution | Cosmos runs one dbt process per model, paying startup and parse each time; `dbt build` in one task pays it once |
| `payments_elt_node_seconds{node}` / `payments_elt_node_rows_written{node}` | where the time and writes go | start with the top of the list |
| `payments_elt_test_pass_ratio` | tests passing, last 24 h (warnings count against it) | fix or tune the warning tests so failures stand out |

Rows written are known for full table rebuilds (the table's row count after the build) and for
seeds. dbt-duckdb doesn't report rows affected for incremental models, so those are left out rather
than guessed; on BigQuery they are reported.

On the local warehouse at the time of writing, dbt wrote about 51 rows per record ingested:
`ml_transaction_features` (57k rows) and `mart_merchant_daily_volume` are rebuilt in full every
run while `fct_transactions` is already incremental. That is the first efficiency target.

## SLOs

Computed from the ledger over 30 days, targets in [slos.md](slos.md):

| Metric | Meaning |
|---|---|
| `payments_elt_slo_sli{slo, window}` | share of good events (`elt_step_success`, `ingest_duration`, `mart_delivery`) |
| `payments_elt_slo_objective{slo}` | the target |
| `payments_elt_error_budget_remaining{slo}` | of the 30-day budget: 1 untouched, 0 spent, negative overspent |
| `payments_elt_last_run_success{step}`, `payments_elt_last_run_timestamp_seconds{step}` | latest sync and dbt invocation |
| `payments_elt_ledger_age_days` | history available; 30-day figures cover less until this reaches 30 |

## Alerts

All in `observability/prometheus-values.yaml`, runbook
[runbooks/elt-cost-and-slos.md](runbooks/elt-cost-and-slos.md):

| Alert | Fires when | Severity |
|---|---|---|
| `EltErrorBudgetBurnFast` | an ELT SLO's 24-hour bad-event rate is 15× its budget (half the month's budget in a day) | page |
| `EltErrorBudgetLow` | under 25% of a 30-day budget left | ticket |
| `EltCostSpike` | last 24 h costs more than 1.5× the 7-day daily average (after a week of history) | ticket |
| `EltWriterSaturated` | writer busy over 70% of the last 24 h | ticket |
