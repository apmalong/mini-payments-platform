"""Module 5: check source tables against their data contracts before ingesting.

    python governance/check_contracts.py            # all contracts in governance/contracts/
    python governance/check_contracts.py --contracts-dir some/dir

Exit code 1 on a breaking change, so the Airflow DAG stops before ingestion. Run it with the
ingestion virtualenv's Python (it needs psycopg and pyyaml).
"""
import argparse
import os
import sys
from pathlib import Path

import psycopg
import yaml

ROOT = Path(__file__).resolve().parent.parent
TYPE_ALIASES = {"timestamp with time zone": "timestamptz", "timestamp without time zone": "timestamp",
                "character varying": "varchar", "integer": "int"}


def load_env() -> None:
    path = ROOT / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())


def normalize(data_type: str, precision, scale) -> str:
    data_type = TYPE_ALIASES.get(data_type, data_type)
    if data_type == "numeric" and precision is not None:
        return f"numeric({precision},{scale})"
    return data_type


def live_columns(conn: psycopg.Connection, table: str) -> dict[str, dict]:
    schema, name = table.split(".")
    rows = conn.execute(
        "select column_name, data_type, numeric_precision, numeric_scale, is_nullable "
        "from information_schema.columns where table_schema = %s and table_name = %s",
        (schema, name)).fetchall()
    return {r[0]: {"type": normalize(r[1], r[2], r[3]), "nullable": r[4] == "YES"} for r in rows}


def check(contract: dict, live: dict[str, dict]) -> tuple[list[str], list[str]]:
    breaking, warnings = [], []
    if not live:
        return [f"table {contract['table']} not found"], []
    declared = {c["name"]: c for c in contract["columns"]}
    for name, spec in declared.items():
        actual = live.get(name)
        if actual is None:
            breaking.append(f"column {name} is missing")
            continue
        if actual["type"] != spec["type"].replace(" ", ""):
            breaking.append(f"column {name} changed type: contract {spec['type']}, source {actual['type']}")
        if not spec.get("nullable", True) and actual["nullable"]:
            breaking.append(f"column {name} must be not null but the source now allows nulls")
    for name in live.keys() - declared.keys():
        warnings.append(f"new column {name} ({live[name]['type']}) is not in the contract yet")
    return breaking, warnings


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--contracts-dir", type=Path, default=Path(__file__).resolve().parent / "contracts")
    args = parser.parse_args()

    load_env()
    conninfo = (f"host={os.environ.get('POSTGRES_HOST', 'localhost')} port={os.environ.get('POSTGRES_PORT', '5433')} "
                f"user={os.environ.get('POSTGRES_USER', 'payments')} password={os.environ.get('POSTGRES_PASSWORD', 'payments')} "
                f"dbname={os.environ.get('POSTGRES_DB', 'payments')}")
    failed = False
    with psycopg.connect(conninfo) as conn:
        for path in sorted(args.contracts_dir.glob("*.yml")):
            contract = yaml.safe_load(path.read_text(encoding="utf-8"))
            breaking, warnings = check(contract, live_columns(conn, contract["table"]))
            status = "BREAKING" if breaking else "ok"
            print(f"{status:<8} {contract['table']} ({path.name}, owner {contract.get('owner', '?')})")
            for message in breaking:
                print(f"  breaking: {message}")
            for message in warnings:
                print(f"  warning:  {message}")
            failed = failed or bool(breaking)
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
