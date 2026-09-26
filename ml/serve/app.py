"""Module 7: fraud scoring service. Loads models:/fraud@champion from MLflow."""
from fastapi import FastAPI

app = FastAPI(title="fraud-scorer")


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}


@app.post("/score")
def score(transaction: dict) -> dict:
    # TODO: fetch online features from Feast, run the champion model, expose latency metrics.
    raise NotImplementedError
