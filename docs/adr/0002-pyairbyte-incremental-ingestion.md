# ADR-0002: PyAirbyte for ingestion, incremental on `updated_at`

- **Status:** accepted
- **Date:** 2026-09-26

## Context

Postgres → warehouse ingestion should use Airbyte (the target stack), be incremental, and run as
a step in the Airflow pipeline.

## Options considered

1. **Airbyte platform** (`abctl`, a kind cluster of its own): the real UI and scheduler, ~8 GB.
2. **PyAirbyte**: the same connectors as a Python library, orchestrated by Airflow.
3. **dlt** or a hand-written loader.

## Decision

PyAirbyte with the Java `source-postgres` connector (run in Docker). Dimensions are fully
refreshed; facts sync incrementally with `updated_at` as the cursor and merge on the primary key.

## Consequences

- **A full refresh must clear state, not force a full read.** PyAirbyte's `force_full_refresh`
  saved ctid state with no cursor; every later incremental run resumed from that ctid and never
  switched to `updated_at`, silently missing changes. `--full-refresh` now deletes the fact streams'
  state and tables, then runs a normal incremental read.
- **Cursor-based incremental depends on honest timestamps.** Two generator bugs broke it: future-dated
  fraud bursts and host clock skew (Windows 5 s ahead of Docker's VM) put the cursor ahead of real
  rows. The generator now takes time from the database clock. Late-arriving rows must get a fresh
  `updated_at`, or they sit behind the cursor forever.
- Each sync takes 45-60 s mostly for connector container start-up.
- On Windows, PyAirbyte reports `terminate()` (exit 1) as a failure; `sync.py` suppresses exactly
  that case.
- Running the connector from inside Airflow needs the Docker socket and the project mounted at the
  path Docker's VM uses for it (see ADR-0003).
- Retry duplicates land in raw by design; staging removes them.
