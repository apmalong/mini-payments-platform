# Service level objectives

Measured by Prometheus recording rules (`observability/prometheus-values.yaml`) and shown in the
Grafana dashboard "Payments platform".

| SLO | Objective | Indicator (recording rule) | Alert |
|---|---|---|---|
| Scorer availability | 99.9% of `/score` requests succeed (non-5xx), rolling 30 days | `slo:scorer_availability:ratio_rate5m` | `ScorerErrorRate` (> 0.1% for 5 min) |
| Scorer latency | 99% of scores complete within 50 ms | `slo:scorer_latency_under_50ms:ratio_rate5m` | `ScorerLatencyHigh` (p99 > 50 ms for 5 min) |
| Mart freshness | `fct_transactions` changed within the last 15 minutes, 99% of the time | `slo:mart_fresh:bool` | `MartDataStale` (> 15 min for 5 min) |

## Error budgets

- **Availability:** 0.1% of requests. At 100 scores/s that's about 260,000 failed requests a month;
  the node-failure test cost 6.
- **Freshness:** 1% of the time, about 7 hours a month. A pipeline paused for maintenance spends it.

When a budget is spent, reliability work takes priority over features until it recovers.

## Why these numbers

- **50 ms**: the fraud check sits inside card authorization, which has a total budget of a few
  hundred milliseconds across the network and issuer. The scorer measures ~5 ms p99 in-process.
- **99.9%**: the consumer fails open (a transaction without a score is processed), so a scorer
  failure degrades fraud protection rather than blocking payments; 99.9% is the right tier for that.
- **15 minutes**: the pipeline runs every 15 minutes; the SLO tolerates one slow or failed run, not
  two in a row.

## Not yet measured

- Consumer end-to-end latency (event time to feature available) has a histogram
  (`stream_event_latency_seconds`) but no SLO: the generator back-dates fraud bursts, so the
  distribution isn't meaningful in this environment.
- Model quality (precision in the top 1%) needs matured labels, 45 days behind; it belongs in a
  weekly report, not a real-time SLO.
