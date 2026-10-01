"""Module 2: ingest Postgres -> DuckDB raw layer with PyAirbyte.

- Small dimension tables (merchants, customers, cards): full refresh, replacing the raw copy.
- Fact tables (transactions, refunds, chargebacks): incremental on updated_at, merged on the
  Postgres primary key, so status changes overwrite the earlier version of the row.
- Raw data is loaded as-is (ELT). Duplicates from client retries share a transaction_id but have
  different row ids, so they survive into raw on purpose; dbt removes them in Module 3.
- Every sync, failed or not, appends a row to ops.ingest_runs (duration, records read, rows
  added): the ingest half of the ELT ledger behind the cost and SLO metrics (docs/elt-metrics.md).

The connector is Java-based, so PyAirbyte runs it in Docker. The container reaches the host's
Postgres through host.docker.internal.
"""
import argparse
import os
import sys
import time
import uuid
import warnings
from datetime import UTC, datetime
from pathlib import Path

os.environ.setdefault("DO_NOT_TRACK", "1")  # turn off PyAirbyte telemetry

import airbyte as ab  # noqa: E402
from airbyte.caches import DuckDBCache  # noqa: E402
from airbyte.exceptions import AirbyteSubprocessFailedError  # noqa: E402
from sqlalchemy import text  # noqa: E402


def _ignore_windows_terminate(unraisable) -> None:
    """PyAirbyte stops reading once it has the message it needs and calls process.terminate().
    On Linux that exits with -15, which PyAirbyte accepts; on Windows TerminateProcess exits
    with 1, which it reports as a failure from a generator's cleanup ("Exception ignored in").
    Real failures are raised normally, not through this hook, so they still surface."""
    exc = unraisable.exc_value
    if isinstance(exc, AirbyteSubprocessFailedError) and exc.exit_code == 1:
        return
    sys.__unraisablehook__(unraisable)


if sys.platform == "win32":
    sys.unraisablehook = _ignore_windows_terminate
    # Same cause: Windows can't delete the config temp file while the killed container holds it.
    warnings.filterwarnings("ignore", message="Failed to remove temporary file")

ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = ROOT / "warehouse.duckdb"

DIMENSIONS = ["merchants", "customers", "cards"]
FACTS = ["transactions", "refunds", "chargebacks"]


def load_env() -> None:
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())


def get_source() -> ab.Source:
    config = {
        "host": os.environ.get("AIRBYTE_POSTGRES_HOST", "host.docker.internal"),
        "port": int(os.environ.get("POSTGRES_PORT", "5433")),
        "database": os.environ.get("POSTGRES_DB", "payments"),
        "username": os.environ.get("POSTGRES_USER", "payments"),
        "password": os.environ.get("POSTGRES_PASSWORD", "payments"),
        "schemas": ["public"],
        "ssl_mode": {"mode": "disable"},
        "tunnel_method": {"tunnel_method": "NO_TUNNEL"},
        "replication_method": {"method": "Standard"},  # cursor-based; CDC would need wal_level=logical
    }
    source = ab.get_source("source-postgres", config=config, docker_image=True)
    source.set_cursor_keys(**{name: "updated_at" for name in FACTS})
    return source


def row_counts(cache: DuckDBCache) -> dict[str, int]:
    counts = {}
    with cache.get_sql_engine().connect() as conn:
        for name in DIMENSIONS + FACTS:
            try:
                counts[name] = conn.execute(text(f"select count(*) from raw.{name}")).scalar()
            except Exception:
                counts[name] = 0
    return counts


def reset_facts(cache: DuckDBCache) -> None:
    """Forget the fact tables' sync state and data, so the next read starts from scratch.

    Don't use PyAirbyte's force_full_refresh for incremental streams: it saves full-refresh
    state (a ctid position with no cursor value), and later incremental reads resume from that
    ctid position and never switch to the updated_at cursor.
    """
    with cache.get_sql_engine().begin() as conn:
        has_state = conn.execute(text(
            "select count(*) from information_schema.tables "
            "where table_schema = 'raw' and table_name = '_airbyte_state'")).scalar()
        if has_state:
            names = ", ".join(f"'{name}'" for name in FACTS)
            conn.execute(text(f"delete from raw._airbyte_state where stream_name in ({names})"))
        for name in FACTS:
            conn.execute(text(f"drop table if exists raw.{name}"))


def record_run(cache: DuckDBCache, run: dict) -> None:
    """Append one row to ops.ingest_runs. A failure to record never hides the sync's own outcome."""
    try:
        with cache.get_sql_engine().begin() as conn:
            conn.execute(text("create schema if not exists ops"))
            conn.execute(text(
                "create table if not exists ops.ingest_runs (run_id varchar, airflow_run_id varchar, "
                "started_at timestamp, finished_at timestamp, status varchar, full_refresh boolean, "
                "records_read bigint, rows_added bigint, error varchar)"))
            conn.execute(text(
                "insert into ops.ingest_runs values (:run_id, :airflow_run_id, :started_at, :finished_at, "
                ":status, :full_refresh, :records_read, :rows_added, :error)"), run)
    except Exception as exc:
        print(f"Could not record the run in ops.ingest_runs: {exc}", file=sys.stderr)


def sync(source: ab.Source, cache: DuckDBCache, full_refresh: bool) -> None:
    run = {"run_id": str(uuid.uuid4()), "airflow_run_id": os.environ.get("AIRFLOW_CTX_DAG_RUN_ID"),
           "started_at": datetime.now(UTC).replace(tzinfo=None), "status": "error",
           "full_refresh": full_refresh, "records_read": None, "rows_added": None, "error": None}
    started = time.time()
    try:
        before = row_counts(cache)
        if full_refresh:
            reset_facts(cache)
        source.read(cache, streams=DIMENSIONS, force_full_refresh=True, write_strategy="replace")
        result = source.read(cache, streams=FACTS, write_strategy="merge")
        after = row_counts(cache)
        run.update(status="success", records_read=result.processed_records,
                   rows_added=sum(after[name] - before[name] for name in FACTS))
    except Exception as exc:
        run["error"] = f"{type(exc).__name__}: {exc}"[:500]
        raise
    finally:
        run["finished_at"] = datetime.now(UTC).replace(tzinfo=None)
        record_run(cache, run)

    print(f"\nSync finished in {time.time() - started:.0f}s ({result.processed_records} fact records read)")
    print(f"{'table':<14}{'before':>10}{'after':>10}{'change':>10}")
    for name in DIMENSIONS + FACTS:
        print(f"{name:<14}{before[name]:>10}{after[name]:>10}{after[name] - before[name]:>+10}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Postgres -> DuckDB raw layer (Module 2)")
    parser.add_argument("--full-refresh", action="store_true", help="reload the fact tables from scratch")
    parser.add_argument("--check", action="store_true", help="only test the source connection")
    parser.add_argument("--every", type=int, default=0, help="repeat the sync every N seconds")
    args = parser.parse_args()

    load_env()
    source = get_source()
    if args.check:
        source.check()
        print("Connection OK. Streams:", ", ".join(source.get_available_streams()))
        return

    cache = DuckDBCache(db_path=DUCKDB_PATH, schema_name="raw")
    while True:
        sync(source, cache, args.full_refresh)
        if not args.every:
            break
        args.full_refresh = False
        time.sleep(args.every)


if __name__ == "__main__":
    main()
