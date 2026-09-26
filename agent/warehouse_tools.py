"""Module 10: what an AI agent may read from the warehouse, enforced in one place.

Used by the MCP server (agent/mcp_server.py) and the ask-the-warehouse app (gateway/ask.py).
Agents get the analyst role: only views in the role_analyst schema, which exclude confidential
and restricted columns (Module 5). A query must parse as a single SELECT whose every table is in
that schema; it runs on a read-only connection with a row cap. Rejections raise QueryRejected
with a reason the agent can act on.
"""
import os
from pathlib import Path

import duckdb
import sqlglot
from sqlglot import exp

ROOT = Path(__file__).resolve().parent.parent
DUCKDB_PATH = os.environ.get("DUCKDB_PATH", str(ROOT / "warehouse.duckdb"))
AGENT_SCHEMA = "role_analyst"
MAX_ROWS = 200


class QueryRejected(ValueError):
    pass


def connect() -> duckdb.DuckDBPyConnection:
    try:
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    except duckdb.Error as error:
        raise QueryRejected(f"warehouse is busy (the pipeline may be writing); retry shortly: {error}")
    conn.execute("set TimeZone = 'UTC'")
    return conn


def schema() -> dict[str, list[tuple[str, str]]]:
    """Tables the agent may query, with their columns and types."""
    conn = connect()
    try:
        rows = conn.execute(
            "select table_name, column_name, data_type from information_schema.columns "
            "where table_schema = ? order by table_name, ordinal_position", [AGENT_SCHEMA]).fetchall()
    finally:
        conn.close()
    tables: dict[str, list[tuple[str, str]]] = {}
    for table, column, data_type in rows:
        tables.setdefault(table, []).append((column, data_type))
    return tables


def check(sql: str) -> exp.Expression:
    """Parse and validate; returns the statement with a row cap applied."""
    try:
        statements = [s for s in sqlglot.parse(sql, dialect="duckdb") if s is not None]
    except sqlglot.errors.ParseError as error:
        raise QueryRejected(f"could not parse the SQL: {error}")
    if len(statements) != 1:
        raise QueryRejected("send exactly one statement")
    statement = statements[0]
    if not isinstance(statement, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
        raise QueryRejected(f"only SELECT queries are allowed, not {statement.key.upper()}")
    if any(statement.find_all(exp.Insert, exp.Update, exp.Delete, exp.Create, exp.Drop, exp.Command)):
        raise QueryRejected("the query contains a write or DDL statement")

    cte_names = {cte.alias_or_name.lower() for cte in statement.find_all(exp.CTE)}
    allowed = set(schema())
    for table in statement.find_all(exp.Table):
        if not isinstance(table.this, exp.Identifier):  # read_parquet(...), read_csv(...), ...
            raise QueryRejected("table functions (read_csv, read_parquet, ...) are not allowed")
        name, db = table.name.lower(), (table.db or "").lower()
        if not db and name in cte_names:
            continue
        if db not in ("", AGENT_SCHEMA) or name not in allowed:
            raise QueryRejected(f"table {table.sql()} is not available; use {AGENT_SCHEMA}: {', '.join(sorted(allowed))}")
        table.set("db", exp.to_identifier(AGENT_SCHEMA))
    return statement.limit(MAX_ROWS) if not statement.args.get("limit") else statement


def run(sql: str) -> tuple[list[str], list[tuple]]:
    statement = check(sql)
    conn = connect()
    try:
        cursor = conn.execute(statement.sql(dialect="duckdb"))
        columns = [d[0] for d in cursor.description]
        rows = cursor.fetchmany(MAX_ROWS)
    except duckdb.Error as error:
        raise QueryRejected(f"query failed: {error}")
    finally:
        conn.close()
    return columns, rows


def as_markdown(columns: list[str], rows: list[tuple]) -> str:
    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    lines += ["| " + " | ".join("" if v is None else str(v) for v in row) + " |" for row in rows]
    return "\n".join(lines) + f"\n\n{len(rows)} row(s){' (capped)' if len(rows) == MAX_ROWS else ''}"
