"""Module 7: fraud scoring service.

Serves models:/fraud@champion. A background thread checks the alias every 30 seconds and swaps
in a newly promoted version without a restart, so promotion in MLflow is the deploy.

POST /score takes a transaction event (the same JSON as the payments.transactions topic).
Windowed features come from the request's `features` if the caller already has them (the
streaming consumer does), otherwise from the online store in Redis.

    In the platform it runs on Kubernetes (k8s/helm/fraud-scorer) at http://127.0.0.1:8091.
    For development: uvicorn app:app --port 8090   # from ml/serve, with the ml virtualenv
"""
import os
import sys
import threading
import time
from pathlib import Path

os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")

import mlflow  # noqa: E402
import pandas as pd  # noqa: E402
import redis  # noqa: E402
from fastapi import FastAPI, HTTPException  # noqa: E402
from mlflow import MlflowClient  # noqa: E402
from prometheus_client import Counter, Gauge, Histogram, make_asgi_app  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import features  # noqa: E402

MODEL_NAME = "fraud"
REFRESH_SECONDS = 30
# Redis hash fields written by streaming/consumer.py -> feature names
ONLINE_FIELDS = {
    "txn_count_5m": "card_txn_count_5m",
    "amount_sum_1h": "card_amount_sum_1h",
    "seconds_since_last_txn": "seconds_since_last_card_txn",
    "is_new_ip_country": "is_new_ip_country",
    "merchant_amount_zscore": "merchant_amount_zscore",
}

LATENCY = Histogram("score_latency_seconds", "Time to score one transaction",
                    buckets=(0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5))
SCORES = Histogram("fraud_score", "Distribution of fraud scores", buckets=[i / 10 for i in range(11)])
DECISIONS = Counter("score_decisions_total", "Scoring decisions", ["decision", "feature_source"])
MODEL_VERSION = Gauge("model_version", "Registered version of the model being served")
REQUESTS = Counter("http_requests_total", "HTTP requests by path and status code", ["path", "status"])

mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000"))
client = MlflowClient()
store = redis.Redis(host=os.environ.get("REDIS_HOST", "localhost"), port=6379, decode_responses=True)


class Champion:
    """The model currently behind the champion alias, swapped atomically on promotion."""

    def __init__(self):
        self.current = None  # (version, model, threshold)

    def refresh(self) -> None:
        version = client.get_model_version_by_alias(MODEL_NAME, "champion")
        if self.current and self.current[0] == version.version:
            return
        model = mlflow.lightgbm.load_model(f"models:/{MODEL_NAME}/{version.version}")
        threshold = client.get_run(version.run_id).data.metrics.get("val_threshold_at_1pct", 0.5)
        self.current = (version.version, model, threshold)  # single assignment: readers see old or new
        MODEL_VERSION.set(int(version.version))
        print(f"serving {MODEL_NAME} v{version.version} (review threshold {threshold:.3f})", flush=True)

    def watch(self) -> None:
        while True:
            time.sleep(REFRESH_SECONDS)
            try:
                self.refresh()
            except Exception as error:  # keep serving the loaded model if MLflow is unreachable
                print(f"model refresh failed, keeping current: {error}", flush=True)


champion = Champion()
app = FastAPI(title="fraud-scorer")
app.mount("/metrics", make_asgi_app())


@app.middleware("http")
async def count_requests(request, call_next):
    """Every response by status, including errors, so availability can be measured (SLO)."""
    try:
        response = await call_next(request)
    except Exception:
        REQUESTS.labels(request.url.path, "500").inc()
        raise
    if not request.url.path.startswith("/metrics"):
        REQUESTS.labels(request.url.path, str(response.status_code)).inc()
    return response


@app.on_event("startup")
def load_model() -> None:
    champion.refresh()
    threading.Thread(target=champion.watch, daemon=True).start()


@app.get("/healthz")
def healthz() -> dict:
    """Liveness: the process is responsive."""
    return {"status": "ok", "model_version": champion.current[0] if champion.current else None}


@app.get("/readyz")
def readyz() -> dict:
    """Readiness: only take traffic once a model is loaded."""
    if champion.current is None:
        raise HTTPException(503, "model not loaded")
    return {"status": "ready", "model_version": champion.current[0]}


@app.post("/score")
def score(event: dict) -> dict:
    started = time.perf_counter()
    if champion.current is None:
        raise HTTPException(503, "no model loaded")
    version, model, threshold = champion.current
    try:
        row = features.flatten_event(event)
    except KeyError as missing:
        raise HTTPException(422, f"missing field {missing}") from missing

    if event.get("features"):
        row.update(event["features"])
        source = "request"
    else:
        online = store.hgetall(f"features:card:{event.get('card_token')}")
        row.update({name: online.get(field) for field, name in ONLINE_FIELDS.items()})
        if row.get("seconds_since_last_card_txn") in ("-1", -1):
            row["seconds_since_last_card_txn"] = None
        source = "redis" if online else "none"

    X = pd.DataFrame([features.build(row)], columns=features.FEATURES)
    probability = float(model.predict_proba(X)[0, 1])
    decision = "review" if probability >= threshold else "approve"
    SCORES.observe(probability)
    DECISIONS.labels(decision, source).inc()
    LATENCY.observe(time.perf_counter() - started)
    return {"transaction_id": event.get("transaction_id"), "fraud_score": round(probability, 4),
            "decision": decision, "model_version": version, "feature_source": source}
