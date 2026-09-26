# ADR-0007: Run the scorer on Kubernetes with explicit rollout and failure settings

- **Status:** accepted
- **Date:** 2026-09-26

## Context

The fraud scorer sits in the payment path: it must scale with volume, update without downtime and
survive a node failure. The rest of the platform (batch, stores, tools) doesn't have that profile.

## Options considered

1. Keep the scorer as a host process (what Module 7 had).
2. A Helm chart on a kind cluster, with the defaults.
3. The same chart with settings chosen for zero-downtime rollouts and fast failover.

## Decision

Option 3 (`k8s/helm/fraud-scorer`): readiness on `/readyz` (model loaded), `maxUnavailable: 0`,
a `preStop` sleep, an HPA (2-5 pods at 60% CPU), a PodDisruptionBudget, a hard spread across
nodes (`DoNotSchedule` with `matchLabelKeys: [pod-template-hash]` and `nodeTaintsPolicy: Honor`),
and 30 s tolerations for unreachable nodes instead of 300 s. MLflow and Redis stay outside the
cluster, reached through `host.docker.internal`.

## Consequences

Measured under ~140 req/s:
- A `helm upgrade` replacing every pod: 0 of 18,744 requests failed.
- A force-killed pod: 0 of 8,634 failed (4 stale connections re-sent).
- A killed worker node carrying two-thirds of traffic: 6 of 20,241 failed (the in-flight requests);
  NotReady after 54 s, replacements serving 51 s later. Clients need timeouts and a retry.

Lessons:
- A soft spread rule put both replicas on one node during a rollout; the hard rule plus
  `matchLabelKeys` fixed it.
- Kubernetes doesn't rebalance after a node recovers; production needs a descheduler.
- The first node test reported 0 failures without checking where traffic went, so it proved
  nothing. The rerun measured per-pod traffic first.
- The load tester exhausted Windows' ephemeral ports at ~200 new connections/s; clients must reuse
  connections.
