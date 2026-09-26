"""Module 9: data freshness and feature drift, as Prometheus metrics on :8001.

Freshness, per layer, so an alert shows where the pipeline stalled, not just that it did:
    payments_freshness_seconds{layer="source"}  newest transactions.updated_at in Postgres
    payments_freshness_seconds{layer="raw"}     newest row the sync loaded into DuckDB raw
    payments_freshness_seconds{layer="mart"}    newest change in analytics.fct_transactions

Drift: population stability index of each live feature over the last hour of streamed events
(streaming Parquet) against the training data (ml_transaction_features, mature and approved).
PSI < 0.1 stable, 0.1-0.25 moderate shift, > 0.25 significant.
    payments_feature_psi{feature="..."}

DuckDB allows one writer, so every read opens a short read-only connection and closes it; when
the pipeline holds the lock the check is skipped and payments_check_success reports 0.

    python exporter.py
"""
import argparse
import math
import os
import time
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--interval", type=int, default=30, help="seconds between freshness checks")
    parser.add_argument("--drift-window", type=int, default=60, help="minutes of streamed events to compare")
    args = parser.parse_args()

    load_env()
    start_http_server(args.port)
    print(f"exporter on :{args.port}/metrics", flush=True)
    reference, reference_at, drift_at = None, 0.0, 0.0
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
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
