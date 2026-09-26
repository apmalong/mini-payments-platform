"""Module 7: train the fraud model, log it to MLflow, and promote it if it beats the champion.

Data: analytics.ml_transaction_features, approved transactions whose labels are mature (older
than 45 days, the chargeback window). Split by time, not at random: the model is validated on
transactions that happened after everything it trained on, as it will be used.

Promotion: the new version gets the `challenger` alias. It becomes `champion` only if it beats
the current champion's PR-AUC on the same validation set and its p99 single-row scoring latency
is under the budget. The scoring service follows the `champion` alias, so promotion is a deploy.

    python train.py
"""
import argparse
import os
import time

os.environ.setdefault("MLFLOW_DISABLE_AGENT_HINT", "1")

import duckdb  # noqa: E402
import lightgbm as lgb  # noqa: E402
import mlflow  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from mlflow import MlflowClient  # noqa: E402
from sklearn.metrics import average_precision_score, roc_auc_score  # noqa: E402

import features  # noqa: E402

MODEL_NAME = "fraud"
LATENCY_BUDGET_MS = 20.0


def load(duckdb_path: str) -> pd.DataFrame:
    connection = duckdb.connect(duckdb_path, read_only=True)
    frame = connection.execute(
        "select * from analytics.ml_transaction_features "
        "where is_approved and label_mature order by created_at").df()
    connection.close()
    return frame


def to_matrix(frame: pd.DataFrame) -> pd.DataFrame:
    rows = [features.build(row) for row in frame.to_dict("records")]
    return pd.DataFrame(rows, columns=features.FEATURES)


def evaluate(model, X: pd.DataFrame, y: np.ndarray) -> dict:
    scores = model.predict_proba(X)[:, 1] if hasattr(model, "predict_proba") else np.asarray(model.predict(X))
    top = max(1, int(len(scores) * 0.01))
    flagged = np.argsort(-scores)[:top]
    return {
        "threshold_at_1pct": float(scores[flagged].min()),  # score cutoff that flags the riskiest 1%
        "pr_auc": average_precision_score(y, scores),
        "roc_auc": roc_auc_score(y, scores),
        "precision_at_1pct": float(y[flagged].mean()),       # of the 1% riskiest, how many were fraud
        "recall_at_1pct": float(y[flagged].sum() / max(1, y.sum())),  # share of all fraud caught there
    }


def p99_latency_ms(model, X: pd.DataFrame, n: int = 300) -> float:
    timings = []
    for i in range(n):
        row = X.iloc[[i % len(X)]]
        start = time.perf_counter()
        model.predict_proba(row)
        timings.append((time.perf_counter() - start) * 1000)
    return float(np.percentile(timings, 99))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--validation-share", type=float, default=0.25)
    parser.add_argument("--skip-if-unchanged", action="store_true",
                        help="exit without training if the mature training set is the same size as last run's")
    args = parser.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    duckdb_path = os.environ.get("DUCKDB_PATH", os.path.join(root, "warehouse.duckdb"))
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000"))
    mlflow.set_experiment("fraud")

    frame = load(duckdb_path)
    if args.skip_if_unchanged:
        last = mlflow.search_runs(experiment_names=["fraud"], order_by=["start_time DESC"], max_results=1)
        if len(last):
            previous = int(last["params.train_rows"].iloc[0]) + int(last["params.valid_rows"].iloc[0])
            if previous == len(frame):
                print(f"training set unchanged ({len(frame)} mature rows); skipping")
                return
    cut = int(len(frame) * (1 - args.validation_share))
    train, valid = frame.iloc[:cut], frame.iloc[cut:]
    X_train, y_train = to_matrix(train), train["label"].astype(int).to_numpy()
    X_valid, y_valid = to_matrix(valid), valid["label"].astype(int).to_numpy()
    print(f"train {len(train)} rows ({y_train.sum()} fraud) up to {train['created_at'].max():%Y-%m-%d}; "
          f"validate {len(valid)} rows ({y_valid.sum()} fraud)")

    params = {
        "n_estimators": 300, "learning_rate": 0.05, "num_leaves": 15, "min_child_samples": 20,
        "subsample": 0.8, "subsample_freq": 1, "colsample_bytree": 0.8,
        "scale_pos_weight": float((len(y_train) - y_train.sum()) / max(1, y_train.sum())),
        "random_state": 42, "verbose": -1,
    }
    model = lgb.LGBMClassifier(**params).fit(X_train, y_train)
    metrics = evaluate(model, X_valid, y_valid)
    metrics["p99_latency_ms"] = p99_latency_ms(model, X_valid)

    with mlflow.start_run() as run:
        mlflow.log_params(params)
        mlflow.log_params({"features": ",".join(features.FEATURES), "train_rows": len(train),
                           "train_fraud": int(y_train.sum()), "valid_rows": len(valid),
                           "valid_fraud": int(y_valid.sum())})
        mlflow.log_metrics({f"val_{k}": v for k, v in metrics.items()})
        importance = dict(zip(features.FEATURES, model.booster_.feature_importance("gain").round(1)))
        mlflow.log_dict(importance, "feature_importance.json")
        info = mlflow.lightgbm.log_model(
            model, name="model", input_example=X_valid.head(3), registered_model_name=MODEL_NAME)

    client = MlflowClient()
    version = info.registered_model_version
    client.set_registered_model_alias(MODEL_NAME, "challenger", version)
    print(f"logged run {run.info.run_id}, registered {MODEL_NAME} v{version} as challenger")
    print("  " + ", ".join(f"{k}={v:.3f}" for k, v in metrics.items()))

    try:
        champion = client.get_model_version_by_alias(MODEL_NAME, "champion")
        champion_model = mlflow.lightgbm.load_model(f"models:/{MODEL_NAME}@champion")
        champion_pr_auc = evaluate(champion_model, X_valid, y_valid)["pr_auc"]
        print(f"champion v{champion.version} on the same validation set: pr_auc={champion_pr_auc:.3f}")
    except mlflow.exceptions.MlflowException:
        champion, champion_pr_auc = None, -1.0
        print("no champion yet")

    beats = metrics["pr_auc"] > champion_pr_auc
    fast = metrics["p99_latency_ms"] <= LATENCY_BUDGET_MS
    if beats and fast:
        client.set_registered_model_alias(MODEL_NAME, "champion", version)
        print(f"PROMOTED v{version} to champion")
    else:
        reasons = [r for r, failed in (("doesn't beat champion", not beats),
                                       (f"p99 latency over {LATENCY_BUDGET_MS}ms", not fast)) if failed]
        print(f"v{version} stays challenger: {', '.join(reasons)}")


if __name__ == "__main__":
    main()
