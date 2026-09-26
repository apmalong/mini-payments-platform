# ADR-0008: Prometheus in the cluster, and PSI for drift instead of Evidently

- **Status:** accepted
- **Date:** 2026-09-26

## Context

Monitoring must cover scorer pods (which come and go), host processes (consumer, exporter) and
data freshness, and detect when live data drifts from the training data.

## Options considered

1. Prometheus on the Docker host scraping the scorer through its NodePort.
2. Prometheus inside the cluster, discovering pods; host targets via `host.docker.internal`.
3. For drift: Evidently reports, or a PSI metric computed by a small exporter.

## Decision

Prometheus in the cluster (prometheus-community chart, server only) and a Python exporter for
freshness per layer and PSI per feature. Grafana in Docker; a socat container bridges
localhost:9090 because kind's port mappings are fixed at cluster creation.

## Consequences

- Per-pod metrics are correct; through the NodePort each scrape would hit a random pod.
- Freshness per layer lets alerts name the stalled stage.
- **Rules need checking against the data, not just loading cleanly.** `raw - source` matched
  nothing without `ignoring(layer)`, so the stage alerts could never fire; `count by (model_version)`
  grouped on a label that doesn't exist. Both were caught by comparing alerts with the dashboard.
- PSI flagged real drift: the simulator uses each card every ~10 minutes live but every few days in
  history, so `seconds_since_last_card_txn` PSI exceeded 4 and the review rate ran ~7-9% instead of
  1%. Evidently would add per-feature reports; PSI in Prometheus gives alerting.
- No Alertmanager: alerts are visible in Prometheus and Grafana but don't page anyone.
