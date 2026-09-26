# ADR-0005: dbt + Redis for features instead of Feast, with a skew check

- **Status:** accepted
- **Date:** 2026-09-26

## Context

The fraud model needs the same features for training (history) and scoring (live). The usual
failure is training/serving skew: the model learns one definition and is served another.

## Options considered

1. **Feast**: one feature registry, offline (DuckDB) and online (Redis) stores.
2. **dbt for offline + the consumer for online**, one shared definition where possible, and a check
   that compares the two.

## Decision

Option 2. Windowed features are computed point-in-time in dbt (`ml_transaction_features`) and
incrementally in the consumer's Lua script. Everything else is derived by `ml/features.py`,
imported by both training and serving. `ml/check_skew.py` compares online and offline values for
the same transactions.

## Consequences

- **The check found real skew immediately.** Online merchant z-scores matched offline 1.4% of the
  time, and time-since-last-transaction 57%: cold start. The online store knew only streamed events
  (and expired card state after 24 h); the offline one had 90 days. `bootstrap_online_store.py` loads
  Redis from the warehouse, and card/merchant state now lives 90 days: 85% and 91% after the fix.
- Remaining mismatches come from a deliberate policy: late events count offline, not online.
- Two implementations of the windowed features must be kept in step by hand; the skew check is
  what makes that safe. Feast would own that problem, at the cost of another system.
- Training uses only labels older than 45 days (the chargeback window) and a time-based split.
