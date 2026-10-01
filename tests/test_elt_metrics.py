from datetime import datetime, timedelta

import duckdb
import pytest

from exporter import elt_metrics, error_budget_remaining, estimated_cost

NOW = datetime(2026, 9, 30, 12, 0)


def minutes_ago(minutes: float) -> datetime:
    return NOW - timedelta(minutes=minutes)


@pytest.fixture
def ledger():
    """The ops tables as ingestion/sync.py and macros/record_run_results.sql create them."""
    conn = duckdb.connect()
    conn.execute("create schema ops")
    conn.execute("create table ops.ingest_runs (run_id varchar, airflow_run_id varchar, started_at timestamp, "
                 "finished_at timestamp, status varchar, full_refresh boolean, records_read bigint, "
                 "rows_added bigint, error varchar)")
    conn.execute("create table ops.dbt_invocations (invocation_id varchar, command varchar, target varchar, "
                 "airflow_run_id varchar, started_at timestamp, finished_at timestamp, nodes bigint)")
    conn.execute("create table ops.dbt_node_runs (invocation_id varchar, unique_id varchar, name varchar, "
                 "resource_type varchar, materialized varchar, status varchar, started_at timestamp, "
                 "completed_at timestamp, execution_seconds double, rows_affected bigint, relation_rows bigint, "
                 "failures bigint, bytes_processed bigint, bytes_billed bigint, slot_ms bigint, message varchar)")
    return conn


def add_ingest(conn, started: float, seconds: float, status: str = "success", records: int = 1000) -> None:
    conn.execute("insert into ops.ingest_runs values ('r', null, ?, ?, ?, false, ?, 0, null)",
                 [minutes_ago(started), minutes_ago(started) + timedelta(seconds=seconds), status, records])


def add_dbt_run(conn, invocation: str, started: float, wall_seconds: float, nodes: list[tuple]) -> None:
    """nodes: (name, resource_type, materialized, status, execution_seconds, relation_rows, bytes_billed)"""
    begin = minutes_ago(started)
    conn.execute("insert into ops.dbt_invocations values (?, 'build', 'duckdb', null, ?, ?, ?)",
                 [invocation, begin, begin + timedelta(seconds=wall_seconds), len(nodes)])
    for name, resource_type, materialized, status, seconds, rows, billed in nodes:
        conn.execute("insert into ops.dbt_node_runs values (?, ?, ?, ?, ?, ?, ?, ?, ?, null, ?, 0, null, ?, null, null)",
                     [invocation, name, name, resource_type, materialized, status, begin,
                      begin + timedelta(seconds=wall_seconds), seconds, rows, billed])


def test_empty_warehouse_reports_nothing():
    metrics = elt_metrics(duckdb.connect(), NOW)
    assert metrics["sli"] == {} and metrics["nodes"] == [] and metrics["last"] == {}
    assert metrics["windows"]["30d"]["ingest_seconds"] == 0


def test_step_success_and_ingest_duration(ledger):
    add_ingest(ledger, 60, 120)
    add_ingest(ledger, 45, 400)                      # succeeded, but slower than 5 minutes
    add_ingest(ledger, 30, 10, status="error")
    add_dbt_run(ledger, "a", 20, 30, [
        ("fct_transactions", "model", "incremental", "success", 5, 900, None),
        ("dim_merchants", "model", "table", "error", 1, None, None),
        ("mart_merchant_daily_volume", "model", "table", "skipped", 0, None, None),   # upstream failed
        ("unique_x", "test", "test", "pass", 1, None, None),
        ("not_null_x", "test", "test", "fail", 1, None, None),
    ])
    metrics = elt_metrics(ledger, NOW)
    assert metrics["sli"]["elt_step_success", "24h"] == pytest.approx(3 / 5)   # skipped builds don't count
    assert metrics["sli"]["ingest_duration", "24h"] == pytest.approx(1 / 2)    # of successful syncs
    assert metrics["windows"]["24h"]["tests_ok"] / metrics["windows"]["24h"]["tests"] == 0.5
    assert metrics["last"]["ingest"][1] is False and metrics["last"]["dbt"][1] is False


def test_mart_delivery_counts_minutes_after_the_last_build(ledger):
    for invocation, started in (("a", 60), ("b", 45), ("c", 30)):
        add_dbt_run(ledger, invocation, started, 0, [("fct_transactions", "model", "incremental", "success", 1, 1, None)])
    # Minutes 60..15 ago are within 15 minutes of a build; the last 14 are not.
    assert elt_metrics(ledger, NOW)["sli"]["mart_delivery", "24h"] == pytest.approx(46 / 60)


def test_rows_written_counts_full_table_rebuilds_only(ledger):
    add_ingest(ledger, 20, 60, records=100)
    add_dbt_run(ledger, "a", 10, 30, [
        ("fct_transactions", "model", "incremental", "success", 5, 50_000, None),   # rows written unknown
        ("dim_merchants", "model", "table", "success", 1, 400, 2**40),
    ])
    metrics = elt_metrics(ledger, NOW)
    assert metrics["windows"]["24h"]["rows_written"] == 400
    assert metrics["windows"]["24h"]["bytes_billed"] == 2**40
    assert {row[0]: row[3] for row in metrics["nodes"]} == {"fct_transactions": None, "dim_merchants": 400}


def test_windows_exclude_older_runs(ledger):
    add_ingest(ledger, 60 * 24 * 3, 60)          # three days ago
    metrics = elt_metrics(ledger, NOW)
    assert metrics["windows"]["24h"]["ingest_runs"] == 0 and metrics["windows"]["7d"]["ingest_runs"] == 1
    assert metrics["ledger_age_days"] == pytest.approx(3, abs=0.01)


def test_cost_and_error_budget():
    assert estimated_cost(3600, 0) == pytest.approx(0.134)
    assert estimated_cost(0, 2**40) == pytest.approx(6.25)
    assert error_budget_remaining(1.0, 0.99) == 1
    assert error_budget_remaining(0.995, 0.99) == pytest.approx(0.5)
    assert error_budget_remaining(0.98, 0.99) == pytest.approx(-1)
