# Service level objectives

Measured by Prometheus recording rules (`observability/prometheus-values.yaml`) and shown in the
Grafana dashboard "Payments platform".

| SLO | Objective | Indicator (recording rule) | Alert |
|---|---|---|---|
| Scorer availability | 99.9% of `/score` requests succeed (non-5xx), rolling 30 days | `slo:scorer_availability:ratio_rate5m` | `ScorerErrorRate` (> 0.1% for 5 min) |
| Scorer latency | 99% of scores complete within 50 ms | `slo:scorer_latency_under_50ms:ratio_rate5m` | `ScorerLatencyHigh` (p99 > 50 ms for 5 min) |
| Mart freshness | `fct_transactions` changed within the last 15 minutes, 99% of the time | `slo:mart_fresh:bool` | `MartDataStale` (> 15 min for 5 min) |

### ELT pipeline

Computed by the exporter from the run ledger in the warehouse (30-day windows; Prometheus keeps
only two days). Metrics, cost and efficiency: [elt-metrics.md](elt-metrics.md).

| SLO | Objective | Indicator | Alert |
|---|---|---|---|
| Step success | 99% of syncs and dbt model/seed/snapshot builds succeed, rolling 30 days | `payments_elt_slo_sli{slo="elt_step_success"}` | `EltErrorBudgetBurnFast`, `EltErrorBudgetLow` |
| Sync duration | 95% of successful syncs finish within 5 minutes | `payments_elt_slo_sli{slo="ingest_duration"}` | same |
| Mart delivery | `fct_transactions` rebuilt successfully within the last 15 minutes, 99% of minutes | `payments_elt_slo_sli{slo="mart_delivery"}` | same |

## Error budgets

- **Availability:** 0.1% of requests. At 100 scores/s that's about 260,000 failed requests a month;
  the node-failure test cost 6.
- **Freshness:** 1% of the time, about 7 hours a month. A pipeline paused for maintenance spends it.
- **Step success:** 1% of steps. A run is a sync plus about 12 dbt builds, so ~37,000 steps a month
  allow ~370 failures: one broken model failing every run for a day spends a quarter of it.
- **Sync duration:** 5% of syncs, about 140 of 2,900 a month.
- **Mart delivery:** 1% of minutes, the same 7 hours as freshness.

`payments_elt_error_budget_remaining` shows what is left of each ELT budget.

When a budget is spent, reliability work takes priority over features until it recovers.

## Why these numbers

- **50 ms**: the fraud check sits inside card authorization, which has a total budget of a few
  hundred milliseconds across the network and issuer. The scorer measures ~5 ms p99 in-process.
- **99.9%**: the consumer fails open (a transaction without a score is processed), so a scorer
  failure degrades fraud protection rather than blocking payments; 99.9% is the right tier for that.
- **15 minutes**: the pipeline runs every 15 minutes; the SLO tolerates one slow or failed run, not
  two in a row.
- **Mart delivery next to mart freshness**: freshness is what consumers see and goes stale when the
  source is quiet, which the platform can't fix. Delivery counts only whether the pipeline rebuilt
  the mart, so it measures the platform team's own reliability.
- **5-minute syncs**: the sync, dbt and the checks share one 15-minute slot and one DuckDB writer;
  a sync past 5 minutes squeezes dbt and pushes the next run late.
- **Step success, not run success**: Cosmos runs each model as its own task, so the ledger sees
  steps; a failed model counts once, and the builds it skips don't count at all.

## Not yet measured

- ELT cost has no SLO: `EltCostSpike` catches a jump, but a budget needs real billing, which comes
  with the BigQuery target.

- Consumer end-to-end latency (event time to feature available) has a histogram
  (`stream_event_latency_seconds`) but no SLO: the generator back-dates fraud bursts, so the
  distribution isn't meaningful in this environment.
- Model quality (precision in the top 1%) needs matured labels, 45 days behind; it belongs in a
  weekly report, not a real-time SLO.
