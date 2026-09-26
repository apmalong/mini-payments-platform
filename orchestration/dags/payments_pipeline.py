"""Module 4: ingest -> dbt build -> publish, every 15 minutes.

- ingest: ingestion/sync.py (PyAirbyte, Postgres -> DuckDB raw) in its own virtualenv
- dbt: one Airflow task per model/seed/snapshot, each followed by its tests (Cosmos)
- publish: emits the fct_transactions asset, which the Module 7 training DAG will schedule on

DuckDB allows a single writer, so the DAG runs one task at a time and one run at a time.
"""
import json
import logging
import os
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Asset
from cosmos import DbtTaskGroup, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import LoadMode, TestBehavior

PROJECT = Path(os.environ.get("PROJECT_DIR", "/opt/project"))
DBT_DIR = PROJECT / "warehouse" / "dbt"
VENVS = Path("/opt/airflow/venvs")
FCT_TRANSACTIONS = Asset("duckdb://warehouse/analytics/fct_transactions")

log = logging.getLogger(__name__)


def notify_failure(context) -> None:
    """Log the failure and, if ALERT_WEBHOOK_URL is set, post it (Slack-compatible payload)."""
    ti = context["task_instance"]
    message = f"payments_pipeline: {ti.task_id} failed on try {ti.try_number}: {context.get('exception')}"
    log.error(message)
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if url:
        request = urllib.request.Request(
            url, data=json.dumps({"text": message}).encode(), headers={"Content-Type": "application/json"})
        urllib.request.urlopen(request, timeout=10)


with DAG(
    dag_id="payments_pipeline",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "on_failure_callback": notify_failure,
    },
    tags=["module-4"],
) as dag:
    ingest = BashOperator(
        task_id="ingest",
        # PyAirbyte bind-mounts TMPDIR into the connector container, so it must live under
        # the project path that Docker's VM can see (see docker-compose.yaml).
        bash_command=f"mkdir -p \"$TMPDIR\" && {VENVS}/ingest/bin/python sync.py",
        cwd=str(PROJECT / "ingestion"),
        env={"TMPDIR": str(PROJECT / ".airbyte-tmp")},
        append_env=True,
        execution_timeout=timedelta(minutes=10),
    )

    # Tasks are rendered from target/manifest.json rather than by running `dbt ls` at parse time,
    # which exceeds the DAG import timeout on a Windows bind mount. After changing models, run
    # `dbt parse` on the host to refresh the manifest.
    transform = DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(
            dbt_project_path=DBT_DIR,
            manifest_path=DBT_DIR / "target" / "manifest.json",
            install_dbt_deps=False,  # run `dbt deps` once on the host
        ),
        profile_config=ProfileConfig(
            profile_name="payments", target_name="duckdb", profiles_yml_filepath=DBT_DIR / "profiles.yml"),
        execution_config=ExecutionConfig(dbt_executable_path=str(VENVS / "dbt" / "bin" / "dbt")),
        render_config=RenderConfig(load_method=LoadMode.DBT_MANIFEST, test_behavior=TestBehavior.AFTER_EACH),
    )

    publish = EmptyOperator(task_id="publish", outlets=[FCT_TRANSACTIONS])

    ingest >> transform >> publish
