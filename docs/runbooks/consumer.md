# Runbook: feature consumer (ConsumerDown, ConsumerLagHigh)

**What it means:** `streaming/consumer.py` keeps the online features in Redis and scores every
transaction. If it's down, new transactions get no real-time fraud score and the online features
go stale (the scorer's Redis fallback then serves old values). If it's lagging, scores arrive late.

## ConsumerDown

1. Is the process running? It's a host process, not a container:
   `Get-CimInstance Win32_Process -Filter "name='python.exe'" | ? CommandLine -like '*consumer.py*'`
2. Start it: `cd streaming; uv run python consumer.py --score-url http://127.0.0.1:8091/score`
3. If it exits immediately, the usual causes are Redpanda or Redis down
   (`docker ps --filter name=mpp-redpanda --filter name=mpp-redis`).

It resumes from its last committed offset, so nothing is lost; expect a burst of catch-up work
(and `ConsumerLagHigh`) right after a restart.

## ConsumerLagHigh

1. Is lag falling? On the dashboard, "Consumer lag" should trend down after a restart. If it is,
   wait.
2. Is scoring slow? Each event waits for the scorer (2 s timeout). Check the "Scoring latency"
   panel and the scorer runbook. Scoring failures fail open (the event is processed without a
   score), so a scorer outage slows the consumer by up to 2 s per event.
3. Is Redis slow or full? `docker exec mpp-redis redis-cli info memory`.
4. Sustained lag with healthy dependencies means one consumer can't keep up: run a second
   consumer with the same `--group` (up to 3, the number of partitions).

## Verify

`sum(stream_consumer_lag)` back under 100 and `stream_events_total{outcome="processed"}` rising.
