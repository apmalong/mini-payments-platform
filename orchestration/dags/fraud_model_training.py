"""Module 7: retrain the fraud model whenever payments_pipeline publishes fct_transactions.

ml/train.py trains on mature labels, logs to MLflow and promotes the new version to `champion`
only if it beats the current one; the scoring service picks up the alias within 30 seconds.
Most pipeline runs don't change the mature training set (labels mature after 45 days), so the
task skips training when the set is the same size as last time.
"""
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Asset

PROJECT = Path(os.environ.get("PROJECT_DIR", "/opt/project"))
FCT_TRANSACTIONS = Asset("duckdb://warehouse/analytics/fct_transactions")

with DAG(
    dag_id="fraud_model_training",
    schedule=[FCT_TRANSACTIONS],
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["module-7"],
) as dag:
    BashOperator(
        task_id="train_and_maybe_promote",
        bash_command="/opt/airflow/venvs/ml/bin/python train.py --skip-if-unchanged",
        cwd=str(PROJECT / "ml"),
        env={"MLFLOW_TRACKING_URI": "http://host.docker.internal:5000"},
        append_env=True,
        execution_timeout=timedelta(minutes=15),
    )
