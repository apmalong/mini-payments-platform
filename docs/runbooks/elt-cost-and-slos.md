# Runbook: ELT cost and SLOs (EltErrorBudgetBurnFast, EltErrorBudgetLow, EltCostSpike, EltWriterSaturated)

**What it means:** the batch pipeline is failing, slow or costing more than usual, measured from
the run ledger in the warehouse's `ops` schema ([elt-metrics.md](../elt-metrics.md)). Grafana's
"What does ELT cost, and is it meeting its SLOs?" row shows the budgets, cost and busiest models.

| Alert | Look at |
|---|---|
| `EltErrorBudgetBurnFast`, `EltErrorBudgetLow` with `slo="elt_step_success"` | failing steps |
| the same with `slo="ingest_duration"` | slow syncs |
| the same with `slo="mart_delivery"` | gaps between successful `fct_transactions` builds |
| `EltCostSpike` | which step or model grew |
| `EltWriterSaturated` | the most expensive models |

Open the warehouse read-only for the queries below (`duckdb -readonly warehouse.duckdb`) and close
it straight away: an open connection blocks the pipeline's writes.

## Diagnose

1. **Failing steps**: what failed in the last day, and why?
   ```sql
   select started_at, 'ingest' as step, error from ops.ingest_runs
   where status != 'success' and started_at >= now() - interval 1 day
   union all
   select n.completed_at, n.name, n.message from ops.dbt_node_runs n
   where n.status in ('error', 'fail') and n.completed_at >= now() - interval 1 day
   order by 1 desc;
   ```
   The same error repeating points to a real break: follow
   [mart-data-stale.md](mart-data-stale.md) step 3 for that task.
2. **Slow syncs**: are durations creeping up or spiking?
   ```sql
   select date_trunc('hour', started_at) as hour, count(*) as runs,
          max(epoch(finished_at - started_at)) as max_s, sum(records_read) as records
   from ops.ingest_runs where started_at >= now() - interval 2 day group by 1 order by 1;
   ```
   Rising with `records`: volume grew, which is capacity work. Rising without it: the connector or
   Docker is slow (see the Airbyte log path in the `ingest` task log).
3. **Delivery gaps**: when did `fct_transactions` go more than 15 minutes without a build?
   ```sql
   select completed_at as build_after_gap,
          round(epoch(completed_at - lag(completed_at) over (order by completed_at)) / 60) as gap_minutes
   from ops.dbt_node_runs
   where name = 'fct_transactions' and resource_type = 'model' and status = 'success'
   qualify gap_minutes > 15 order by completed_at desc limit 20;
   ```
   Paused Airflow, a stopped laptop, or failing upstream steps all show up here.
4. **Cost or writer busy**: which model grew?
   ```sql
   select n.name, date_trunc('day', i.started_at) as day, round(sum(n.execution_seconds)) as total_s,
          sum(n.relation_rows) as relation_rows, sum(n.bytes_billed) as bytes_billed
   from ops.dbt_node_runs n join ops.dbt_invocations i using (invocation_id)
   where i.started_at >= now() - interval 8 day and n.resource_type = 'model'
   group by all order by n.name, day;
   ```
   Also compare `ops.ingest_runs` durations. A full refresh (`full_refresh = true`) or a
   `dbt build --full-refresh` explains a one-off spike.

## Fix and verify

- Failures: fix the cause, then rerun the failed task in Airflow. Budgets recover as good runs
  accumulate; nothing needs resetting.
- Cost or saturation: make the top model incremental, or stop rebuilding it every run (a separate
  daily schedule). Check with `payments_elt_node_seconds` and `payments_elt_busy_ratio` a day later.
- When `EltErrorBudgetLow` fires, reliability work takes priority over features until the budget
  recovers ([slos.md](../slos.md)).
