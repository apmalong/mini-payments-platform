# Runbook: fraud scorer (ScorerDown, ScorerErrorRate, ScorerLatencyHigh, ModelVersionsDiverged)

**What it means:** the scoring service (Helm release `fraud-scorer`, namespace `fraud`) is the
real-time fraud check. SLOs: 99.9% of `/score` requests succeed, 99% complete within 50 ms.

## ScorerDown / ScorerErrorRate

1. Pods: `kubectl -n fraud get pods -o wide`. Look for `CrashLoopBackOff`, `0/1` Ready, or all
   pods on one node.
2. Not Ready usually means no model loaded: `kubectl -n fraud logs deploy/fraud-scorer | Select-String "serving|refresh failed"`.
   - `refresh failed` / MLflow errors: is MLflow up (`docker ps --filter name=mpp-mlflow`,
     http://localhost:5000/health)? A running pod keeps its loaded model if MLflow goes away;
     a new pod can't start without it.
   - `No module named ...`: the image is missing a dependency (as with `skops`); rebuild.
3. Errors with pods Ready: the error panel shows which status. 422 is a caller sending a bad
   payload (not our availability); 5xx is ours: read the pod logs for the traceback.
4. Node failure: `kubectl get nodes`. Pods move off a NotReady node after 30 s; if replacements
   are Pending, check `kubectl -n fraud describe pod` events.

Roll back a bad release: `helm -n fraud rollback fraud-scorer` (zero-downtime, same as upgrade).
Roll back a bad model: point the `champion` alias at the previous version in MLflow; pods pick
it up within 30 s.

## ScorerLatencyHigh

1. CPU-bound? `kubectl -n fraud top pods`. At the CPU limit (500m) the HPA should add pods; if it's
   at `maxReplicas` (5), raise it in `values.yaml` or the limits.
2. A new model version slower than the last? Compare the "Scoring latency" panel around the
   promotion; `train.py` gates on p99 < 20 ms offline, but production inputs can differ.

## ModelVersionsDiverged

Pods poll the champion alias every 30 s, so differences should last seconds. Ten minutes means a
pod can't reach MLflow: find it with the per-pod `model_version` metric and check its logs. Restart
it (`kubectl -n fraud delete pod <name>`) once MLflow is reachable.
