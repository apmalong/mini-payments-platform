"""Module 9: data freshness and feature drift, as Prometheus metrics on :8001.

Freshness, per layer, so an alert shows where the pipeline stalled, not just that it did:
    payments_freshness_seconds{layer="source"}  newest transactions.updated_at in Postgres
    payments_freshness_seconds{layer="raw"}     newest row the sync loaded into DuckDB raw
    payments_freshness_seconds{layer="mart"}    newest change in analytics.fct_transactions

Drift: population stability index of each live feature over the last hour of streamed events
(streaming Parquet) against the training data (ml_transaction_features, mature and approved).
PSI < 0.1 stable, 0.1-0.25 moderate shift, > 0.25 significant.
    payments_feature_psi{feature="..."}

ELT cost, efficiency and SLOs, from the run ledger the pipeline writes to the warehouse's ops
schema (ingestion/sync.py, dbt's on-run-end hook). Prometheus keeps two days; the ledger keeps
every run, so 7- and 30-day figures are computed here. See docs/elt-metrics.md.
    payments_elt_compute_seconds{step, window}       busy time: sync wall-clock, dbt invocations
    payments_elt_estimated_cost_usd{window}          compute time and BigQuery bytes at list price
    payments_elt_slo_sli{slo, window}                share of good events against each ELT SLO
    payments_elt_error_budget_remaining{slo}         of the 30-day budget; below 0 means spent

DuckDB allows one writer, so every read opens a short read-only connection and closes it; when
the pipeline holds the lock the check is skipped and payments_check_success reports 0.

    python exporter.py
"""
import argparse
import math
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import psycopg
from prometheus_client import Gauge, start_http_server

ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", str(ROOT / "warehouse.duckdb"))
PARQUET = (ROOT / "streaming" / "offline" / "transaction_features" / "*" / "*.parquet").as_posix()
DRIFT_FEATURES = ["amount", "card_txn_count_5m", "card_amount_sum_1h", "seconds_since_last_card_txn",
                  "merchant_amount_zscore"]

FRESHNESS = Gauge("payments_freshness_seconds", "Age of the newest data in each layer", ["layer"])
PSI = Gauge("payments_feature_psi", "Population stability index vs training data", ["feature"])
DRIFT_ROWS = Gauge("payments_drift_window_rows", "Streamed events in the drift window")
CHECK_OK = Gauge("payments_check_success", "1 if the last check could read its source", ["check"])

# ELT pricing assumptions, overridable per environment: a GCE e2-standard-4 on demand
# (us-central1) for the machine running the pipeline, BigQuery on-demand analysis for bytes billed.
COMPUTE_USD_PER_HOUR = float(os.environ.get("ELT_COMPUTE_USD_PER_HOUR", "0.134"))
BQ_USD_PER_TIB = float(os.environ.get("ELT_BQ_USD_PER_TIB", "6.25"))
ELT_WINDOWS = {"24h": 1, "7d": 7, "30d": 30}
SLO_OBJECTIVES = {
    "elt_step_success": 0.99,   # ingest syncs and dbt model/seed/snapshot builds that succeed
    "ingest_duration": 0.95,    # successful syncs that finish within INGEST_TARGET_SECONDS
    "mart_delivery": 0.99,      # minutes in which fct_transactions was rebuilt within DELIVERY_TARGET_SECONDS
}
INGEST_TARGET_SECONDS = 300
DELIVERY_TARGET_SECONDS = 900

ELT_SECONDS = Gauge("payments_elt_compute_seconds", "Pipeline busy time", ["step", "window"])
ELT_COST = Gauge("payments_elt_estimated_cost_usd", "Estimated ELT cost at list prices", ["window"])
ELT_BYTES_BILLED = Gauge("payments_elt_bytes_billed", "Warehouse bytes billed (BigQuery only)", ["window"])
ELT_RECORDS = Gauge("payments_elt_records_ingested", "Records the sync read from the source", ["window"])
ELT_UNIT_COST = Gauge("payments_elt_cost_per_million_records_usd", "Estimated cost per million records ingested",
                      ["window"])
ELT_BUSY = Gauge("payments_elt_busy_ratio", "Share of the last 24 h the single warehouse writer was busy")
ELT_OVERHEAD = Gauge("payments_elt_dbt_overhead_ratio",
                     "Share of dbt wall-clock outside node execution (startup, parse, compile), last 24 h")
ELT_AMPLIFICATION = Gauge("payments_elt_write_amplification", "Rows dbt wrote per record ingested, last 24 h")
ELT_TEST_PASS = Gauge("payments_elt_test_pass_ratio", "dbt data and unit tests that passed, last 24 h")
ELT_NODE_SECONDS = Gauge("payments_elt_node_seconds", "dbt execution time per node, last 24 h",
                         ["node", "resource_type"])
ELT_NODE_ROWS = Gauge("payments_elt_node_rows_written", "Rows written per node, last 24 h", ["node"])
ELT_NODE_BYTES = Gauge("payments_elt_node_bytes_billed", "Bytes billed per node, last 24 h (BigQuery)", ["node"])
ELT_LEDGER_AGE = Gauge("payments_elt_ledger_age_days", "Days since the ledger's first recorded run")
ELT_LAST_RUN = Gauge("payments_elt_last_run_timestamp_seconds", "When the step last finished", ["step"])
ELT_LAST_OK = Gauge("payments_elt_last_run_success", "1 if the step's last run succeeded", ["step"])
SLO_SLI = Gauge("payments_elt_slo_sli", "Share of good events for each ELT SLO", ["slo", "window"])
SLO_OBJECTIVE = Gauge("payments_elt_slo_objective", "Target share of good events", ["slo"])
SLO_BUDGET = Gauge("payments_elt_error_budget_remaining", "Share of the 30-day error budget left", ["slo"])


def load_env() -> None:
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())


def check_source() -> None:
    try:
        with psycopg.connect(
                f"host={os.environ.get('POSTGRES_HOST', 'localhost')} port={os.environ.get('POSTGRES_PORT', '5433')} "
                f"user={os.environ.get('POSTGRES_USER', 'payments')} password={os.environ.get('POSTGRES_PASSWORD', 'payments')} "
                f"dbname={os.environ.get('POSTGRES_DB', 'payments')}", connect_timeout=5) as conn:
            age = conn.execute("select extract(epoch from now() - max(updated_at)) from transactions").fetchone()[0]
        FRESHNESS.labels("source").set(float(age))
        CHECK_OK.labels("source").set(1)
    except psycopg.Error:
        CHECK_OK.labels("source").set(0)


def check_warehouse() -> None:
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    except duckdb.Error:  # the pipeline is writing
        CHECK_OK.labels("warehouse").set(0)
        return
    try:
        conn.execute("set TimeZone = 'UTC'")  # warehouse timestamps are naive UTC; don't read them as local time
        raw, mart = conn.execute("""
            select
                epoch(now()::timestamp - (select max(_airbyte_extracted_at) from raw.transactions)),
                epoch(now()::timestamp - (select max(last_changed_at) from analytics.fct_transactions))
        """).fetchone()
        FRESHNESS.labels("raw").set(raw)
        FRESHNESS.labels("mart").set(mart)
        CHECK_OK.labels("warehouse").set(1)
    finally:
        conn.close()


def reference_bins() -> dict[str, tuple[list[float], list[float]]]:
    """Decile edges and bucket shares of each feature in the training data."""
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        bins = {}
        for feature in DRIFT_FEATURES:
            values = f"coalesce(cast({feature} as double), -1)"
            edges = sorted(set(conn.execute(
                f"select quantile_cont({values}, [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]) "
                f"from analytics.ml_transaction_features where label_mature and is_approved").fetchone()[0]))
            shares = bucket_shares(conn, f"select {values} as v from analytics.ml_transaction_features "
                                         f"where label_mature and is_approved", edges)
            bins[feature] = (edges, shares)
        return bins
    finally:
        conn.close()


def bucket_shares(conn, query: str, edges: list[float]) -> list[float]:
    """Share of rows in each bucket: (-inf, e0], (e0, e1], ..., (en, inf)."""
    cases = " ".join(f"when v <= {edge} then {i}" for i, edge in enumerate(edges))
    rows = dict(conn.execute(
        f"select case {cases} else {len(edges)} end as bucket, count(*) from ({query}) group by 1").fetchall())
    total = sum(rows.values()) or 1
    return [rows.get(i, 0) / total for i in range(len(edges) + 1)]


def check_drift(reference: dict, window_minutes: int) -> None:
    conn = duckdb.connect()  # Parquet only: no warehouse lock needed
    try:
        recent = (f"select * from read_parquet('{PARQUET}', union_by_name = true) "
                  f"where not is_late and processed_at >= now() - interval {window_minutes} minute")
        count = conn.execute(f"select count(*) from ({recent})").fetchone()[0]
        DRIFT_ROWS.set(count)
        if count < 100:  # too few events for a stable estimate
            return
        for feature, (edges, expected) in reference.items():
            actual = bucket_shares(conn, f"select coalesce(cast({feature} as double), -1) as v from ({recent})", edges)
            psi = sum((a - e) * math.log((a + 1e-4) / (e + 1e-4)) for a, e in zip(actual, expected, strict=True))
            PSI.labels(feature).set(round(psi, 4))
        CHECK_OK.labels("drift").set(1)
    except duckdb.Error:
        CHECK_OK.labels("drift").set(0)
    finally:
        conn.close()


BUILDS = "resource_type in ('model', 'seed', 'snapshot')"
TESTS = "resource_type in ('test', 'unit_test')"
# dbt-duckdb doesn't report rows affected; a table rebuild writes every row it ends with.
ROWS_WRITTEN = "coalesce(rows_affected, case when materialized = 'table' then relation_rows end)"


def error_budget_remaining(sli: float, objective: float) -> float:
    """1 = untouched, 0 = spent, negative = overspent."""
    return 1 - (1 - sli) / (1 - objective)


def estimated_cost(compute_seconds: float, bytes_billed: float) -> float:
    return compute_seconds / 3600 * COMPUTE_USD_PER_HOUR + bytes_billed / 2**40 * BQ_USD_PER_TIB


def ledger_tables(conn) -> set[str]:
    return {name for (name,) in conn.execute(
        "select table_name from information_schema.tables where table_schema = 'ops'").fetchall()}


def elt_window(conn, tables: set[str], since: datetime) -> dict:
    """Ledger totals for runs that started at or after `since`."""
    totals = dict.fromkeys(["ingest_runs", "ingest_ok", "ingest_fast", "ingest_seconds", "records", "dbt_seconds",
                            "builds", "builds_ok", "tests", "tests_ok", "node_seconds", "rows_written",
                            "bytes_billed"], 0)
    if "ingest_runs" in tables:
        row = conn.execute(f"""
            select count(*), count(*) filter (where status = 'success'),
                   count(*) filter (where status = 'success'
                                    and epoch(finished_at - started_at) <= {INGEST_TARGET_SECONDS}),
                   coalesce(sum(epoch(finished_at - started_at)), 0), coalesce(sum(records_read), 0)
            from ops.ingest_runs where started_at >= ?""", [since]).fetchone()
        totals.update(zip(["ingest_runs", "ingest_ok", "ingest_fast", "ingest_seconds", "records"], row, strict=True))
    if {"dbt_invocations", "dbt_node_runs"} <= tables:
        row = conn.execute(f"""
            with invocations as (select * from ops.dbt_invocations where started_at >= ?)
            select (select coalesce(sum(epoch(finished_at - started_at)), 0) from invocations),
                   count(*) filter (where {BUILDS} and status != 'skipped'),
                   count(*) filter (where {BUILDS} and status = 'success'),
                   count(*) filter (where {TESTS} and status != 'skipped'),
                   count(*) filter (where {TESTS} and status = 'pass'),
                   coalesce(sum(execution_seconds), 0), coalesce(sum({ROWS_WRITTEN}), 0),
                   coalesce(sum(bytes_billed), 0)
            from ops.dbt_node_runs where invocation_id in (select invocation_id from invocations)""",
                           [since]).fetchone()
        totals.update(zip(["dbt_seconds", "builds", "builds_ok", "tests", "tests_ok", "node_seconds",
                           "rows_written", "bytes_billed"], row, strict=True))
    return {key: float(value) for key, value in totals.items()}


def delivery_sli(conn, since: datetime, now: datetime) -> float | None:
    """Share of minutes since the first recorded build in which fct_transactions had been rebuilt
    within DELIVERY_TARGET_SECONDS. Unlike payments_freshness_seconds{layer="mart"}, a quiet source
    doesn't count against it: this measures the pipeline, not the traffic."""
    return conn.execute(f"""
        with builds as (
            select completed_at from ops.dbt_node_runs
            where name = 'fct_transactions' and resource_type = 'model' and status = 'success'),
        minutes as (
            select unnest(range(greatest(?::timestamp, min(completed_at)), ?::timestamp, interval 1 minute)) as t
            from builds having count(*) > 0)
        select avg(case when b.completed_at >= m.t - interval {DELIVERY_TARGET_SECONDS} second then 1 else 0 end)
        from minutes m asof left join builds b on m.t >= b.completed_at""", [since, now]).fetchone()[0]


def elt_metrics(conn, now: datetime) -> dict:
    """Cost, efficiency and SLO figures from the ops ledger, as of `now` (naive UTC)."""
    tables = ledger_tables(conn)
    windows = {name: elt_window(conn, tables, now - timedelta(days=days)) for name, days in ELT_WINDOWS.items()}
    sli = {}
    for name, days in ELT_WINDOWS.items():
        w = windows[name]
        if w["ingest_runs"] + w["builds"]:
            sli["elt_step_success", name] = (w["ingest_ok"] + w["builds_ok"]) / (w["ingest_runs"] + w["builds"])
        if w["ingest_ok"]:
            sli["ingest_duration", name] = w["ingest_fast"] / w["ingest_ok"]
        if "dbt_node_runs" in tables:
            value = delivery_sli(conn, now - timedelta(days=days), now)
            if value is not None:
                sli["mart_delivery", name] = float(value)

    firsts = [conn.execute(f"select min(started_at) from ops.{table}").fetchone()[0]
              for table in ("ingest_runs", "dbt_invocations") if table in tables]
    firsts = [first for first in firsts if first is not None]
    ledger_age_days = (now - min(firsts)).total_seconds() / 86400 if firsts else 0.0

    nodes, last = [], {}
    if {"dbt_invocations", "dbt_node_runs"} <= tables:
        nodes = conn.execute(f"""
            select n.name, n.resource_type, sum(n.execution_seconds), sum({ROWS_WRITTEN}), sum(n.bytes_billed)
            from ops.dbt_node_runs n join ops.dbt_invocations i using (invocation_id)
            where i.started_at >= ? and {BUILDS}
            group by all""", [now - timedelta(days=1)]).fetchall()
        row = conn.execute("""
            select epoch(i.finished_at), not exists (
                select 1 from ops.dbt_node_runs n
                where n.invocation_id = i.invocation_id and n.status in ('error', 'fail'))
            from ops.dbt_invocations i order by i.finished_at desc limit 1""").fetchone()
        if row:
            last["dbt"] = row
    if "ingest_runs" in tables:
        row = conn.execute("select epoch(finished_at), status = 'success' from ops.ingest_runs "
                           "order by finished_at desc limit 1").fetchone()
        if row:
            last["ingest"] = row
    return {"windows": windows, "sli": sli, "nodes": nodes, "last": last, "ledger_age_days": ledger_age_days}


def publish_elt(metrics: dict) -> None:
    for name, w in metrics["windows"].items():
        ELT_SECONDS.labels("ingest", name).set(w["ingest_seconds"])
        ELT_SECONDS.labels("dbt", name).set(w["dbt_seconds"])
        cost = estimated_cost(w["ingest_seconds"] + w["dbt_seconds"], w["bytes_billed"])
        ELT_COST.labels(name).set(cost)
        ELT_BYTES_BILLED.labels(name).set(w["bytes_billed"])
        ELT_RECORDS.labels(name).set(w["records"])
        if w["records"]:
            ELT_UNIT_COST.labels(name).set(cost / w["records"] * 1e6)

    ELT_LEDGER_AGE.set(metrics["ledger_age_days"])
    day = metrics["windows"]["24h"]
    ELT_BUSY.set((day["ingest_seconds"] + day["dbt_seconds"]) / 86400)
    if day["dbt_seconds"]:
        ELT_OVERHEAD.set(max(0.0, 1 - day["node_seconds"] / day["dbt_seconds"]))  # threads can overlap nodes
    if day["records"]:
        ELT_AMPLIFICATION.set(day["rows_written"] / day["records"])
    if day["tests"]:
        ELT_TEST_PASS.set(day["tests_ok"] / day["tests"])

    for gauge in (ELT_NODE_SECONDS, ELT_NODE_ROWS, ELT_NODE_BYTES):
        gauge.clear()  # drop nodes that no longer run
    for node, resource_type, seconds, rows, bytes_billed in metrics["nodes"]:
        ELT_NODE_SECONDS.labels(node, resource_type).set(seconds or 0)
        if rows is not None:
            ELT_NODE_ROWS.labels(node).set(rows)
        if bytes_billed is not None:
            ELT_NODE_BYTES.labels(node).set(bytes_billed)

    for step, (finished, ok) in metrics["last"].items():
        ELT_LAST_RUN.labels(step).set(finished)
        ELT_LAST_OK.labels(step).set(int(ok))

    for slo, objective in SLO_OBJECTIVES.items():
        SLO_OBJECTIVE.labels(slo).set(objective)
    for (slo, window), value in metrics["sli"].items():
        SLO_SLI.labels(slo, window).set(value)
        if window == "30d":
            SLO_BUDGET.labels(slo).set(error_budget_remaining(value, SLO_OBJECTIVES[slo]))


def check_elt() -> None:
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    except duckdb.Error:  # the pipeline is writing
        CHECK_OK.labels("elt").set(0)
        return
    try:
        metrics = elt_metrics(conn, datetime.now(UTC).replace(tzinfo=None))
    except duckdb.Error:
        CHECK_OK.labels("elt").set(0)
        return
    finally:
        conn.close()
    publish_elt(metrics)
    CHECK_OK.labels("elt").set(1)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--interval", type=int, default=30, help="seconds between freshness checks")
    parser.add_argument("--drift-window", type=int, default=60, help="minutes of streamed events to compare")
    parser.add_argument("--elt-interval", type=int, default=120, help="seconds between ELT ledger checks")
    args = parser.parse_args()

    load_env()
    start_http_server(args.port)
    print(f"exporter on :{args.port}/metrics", flush=True)
    reference, reference_at, drift_at, elt_at = None, 0.0, 0.0, 0.0
    while True:
        check_source()
        check_warehouse()
        now = time.time()
        if reference is None or now - reference_at > 3600:
            try:
                reference, reference_at = reference_bins(), now
            except duckdb.Error:
                pass  # warehouse busy; retry next round
        if reference and now - drift_at > 300:
            check_drift(reference, args.drift_window)
            drift_at = now
        if now - elt_at > args.elt_interval:
            check_elt()
            elt_at = now
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
