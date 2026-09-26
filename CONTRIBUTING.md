# Contributing

## Data PR review checklist

- [ ] Models have tests on primary keys and foreign keys
- [ ] PII columns tagged and masked before marts
- [ ] Incremental models are idempotent (safe to re-run or backfill)
- [ ] Large tables partitioned and clustered
- [ ] Docs and descriptions updated
- [ ] Contract changes are non-breaking, or versioned
