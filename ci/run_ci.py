"""Module 11: the CI pipeline, runnable locally and in GitHub Actions (.github/workflows/ci.yml).

    python ci/run_ci.py                                   # local: Postgres on :5433
    python ci/run_ci.py --postgres-host localhost --postgres-port 5432   # GitHub service container

Steps, stopping at the first failure:
  1. lint (ruff), unit tests (pytest) and the portal's browser tests (e2e/, Playwright + Chromium)
  2. source fixture: a fresh payments_ci database filled by the real generator (small backfill)
  3. source contract check against it
  4. warehouse fixture: raw tables copied from payments_ci into ci/warehouse_ci.duckdb with DuckDB's
     postgres extension, shaped as PyAirbyte lands them (naive UTC timestamps, JSON as text)
  5. dbt build (models, contracts, tests) and the PII lineage check

Everything writes to ci/ (warehouse file, dbt target), never to the working warehouse or manifest.
"""
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import psycopg

ROOT = Path(__file__).resolve().parent.parent
CI = ROOT / "ci"
DBT_DIR = ROOT / "warehouse" / "dbt"
WAREHOUSE = CI / "warehouse_ci.duckdb"
TARGET = CI / "target"
SOURCE_DB = "payments_ci"
TABLES = ["merchants", "customers", "cards", "transactions", "refunds", "chargebacks"]


def step(name: str, command: list[str], env: dict | None = None, cwd: Path = ROOT) -> None:
    print(f"\n=== {name}", flush=True)
    started = time.time()
    result = subprocess.run(command, cwd=cwd, env={**os.environ, **(env or {})})
    if result.returncode != 0:
        sys.exit(f"FAILED: {name} (exit {result.returncode})")
    print(f"--- ok in {time.time() - started:.0f}s", flush=True)


def tool(name: str) -> str:
    folder = Path(sys.executable).parent
    return str(folder / (f"{name}.exe" if os.name == "nt" else name))


def create_source_db(pg: dict) -> None:
    print(f"\n=== source fixture: fresh {SOURCE_DB} database", flush=True)
    with psycopg.connect(dbname=pg["POSTGRES_DB_ADMIN"], host=pg["POSTGRES_HOST"], port=pg["POSTGRES_PORT"],
                         user=pg["POSTGRES_USER"], password=pg["POSTGRES_PASSWORD"], autocommit=True) as conn:
        conn.execute(f"drop database if exists {SOURCE_DB} with (force)")
        conn.execute(f"create database {SOURCE_DB}")


def build_warehouse_fixture(pg: dict) -> None:
    print("\n=== warehouse fixture: raw tables from the source, shaped like PyAirbyte output", flush=True)
    WAREHOUSE.unlink(missing_ok=True)
    conn = duckdb.connect(str(WAREHOUSE))
    conn.execute("set TimeZone = 'UTC'")
    conn.execute("install postgres; load postgres")
    conn.execute(f"attach 'dbname={SOURCE_DB} host={pg['POSTGRES_HOST']} port={pg['POSTGRES_PORT']} "
                 f"user={pg['POSTGRES_USER']} password={pg['POSTGRES_PASSWORD']}' as src (type postgres, read_only)")
    conn.execute("create schema raw")
    for table in TABLES:
        columns = conn.execute(
            "select column_name, data_type from information_schema.columns "
            "where table_catalog = 'src' and table_schema = 'public' and table_name = ? order by ordinal_position",
            [table]).fetchall()
        expressions = []
        for column, data_type in columns:
            if data_type == "TIMESTAMP WITH TIME ZONE":
                expressions.append(f"timezone('UTC', {column}) as {column}")   # PyAirbyte lands naive UTC
            elif data_type in ("JSON", "JSONB") or column == "raw_payload":
                expressions.append(f"cast({column} as varchar) as {column}")
            else:
                expressions.append(column)
        conn.execute(f"create table raw.{table} as select {', '.join(expressions)}, "
                     f"now()::timestamp as _airbyte_extracted_at from src.public.{table}")
        print(f"  raw.{table}: {conn.execute(f'select count(*) from raw.{table}').fetchone()[0]} rows")
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--postgres-host", default="localhost")
    parser.add_argument("--postgres-port", default="5433")
    parser.add_argument("--skip-lint", action="store_true")
    args = parser.parse_args()

    pg = {"POSTGRES_HOST": args.postgres_host, "POSTGRES_PORT": args.postgres_port,
          "POSTGRES_USER": os.environ.get("POSTGRES_USER", "payments"),
          "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "payments"),
          "POSTGRES_DB_ADMIN": os.environ.get("POSTGRES_DB", "payments")}
    source_env = {k: v for k, v in pg.items() if k != "POSTGRES_DB_ADMIN"} | {"POSTGRES_DB": SOURCE_DB}
    dbt_env = {"DUCKDB_PATH": str(WAREHOUSE), "DBT_TARGET_PATH": str(TARGET), "DBT_SEND_ANONYMOUS_USAGE_STATS": "false"}
    started = time.time()

    if not args.skip_lint:
        step("lint", [tool("ruff"), "check", "."])
    step("unit tests", [sys.executable, "-m", "pytest", "tests", "-q"])
    step("portal browser tests", [sys.executable, "-m", "pytest", "e2e", "-q"])
    create_source_db(pg)
    step("generate source data", [sys.executable, "generator/generate.py", "--reset", "--merchants", "20",
                                  "--customers", "500", "--transactions", "3000", "--days", "90"], env=source_env)
    step("source contract", [sys.executable, "governance/check_contracts.py"], env=source_env)
    build_warehouse_fixture(pg)
    dbt = [tool("dbt"), "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    step("dbt deps", dbt[:1] + ["deps"] + dbt[1:], env=dbt_env)
    step("dbt build (models, contracts, tests)", dbt[:1] + ["build"] + dbt[1:], env=dbt_env)
    step("PII lineage check", [sys.executable, "governance/check_pii.py"], env=dbt_env)
    print(f"\nCI passed in {time.time() - started:.0f}s")


if __name__ == "__main__":
    main()
