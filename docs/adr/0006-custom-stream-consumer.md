# ADR-0006: A small custom consumer with Redis Lua, not a stream processor

- **Status:** accepted
- **Date:** 2026-09-26

## Context

Real-time features (card windows, merchant statistics) must update per transaction, tolerate
duplicate and late events, and survive restarts without double counting.

## Options considered

1. **A stream processor** (Flink, Kafka Streams, Quix Streams): windows and state built in, more to run.
2. **A small confluent-kafka consumer** with state in Redis, updated atomically by a Lua script.

## Decision

Option 2 (`streaming/consumer.py`). One Lua script per event does dedupe, window update and feature
read atomically. Offsets are committed only after each Parquet flush (at-least-once), and the
dedupe key makes Redis effectively-once. Events more than 10 minutes behind the watermark go
offline only. Redpanda stands in for Kafka (same protocol, one container).

## Consequences

- The mechanics are visible and testable, which suits a platform meant to be explained.
- Single consumer per partition; scaling beyond 3 means more partitions.
- The scorer call is synchronous per event and fails open: a scoring outage slows the consumer
  (2 s timeout) but never stops feature computation.
- **Use `127.0.0.1`, not `localhost`, on Windows:** Python tries IPv6 first and stalled about 2 s per
  request, so nothing got scored until this was found.
- Malformed events are counted and skipped (a production consumer would also dead-letter them).
