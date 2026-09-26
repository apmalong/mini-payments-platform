"""Module 10: ask the warehouse a question in plain English.

The question and the analyst schema go to the AI gateway (team key, PII guardrail, logging); the
model answers with SQL; the SQL runs through warehouse_tools, the same guard the MCP server uses,
so the model can only read the analyst views, however it phrases the query.

    python ask.py "Which merchant categories had the most fraud chargebacks?"
    python ask.py "..." --mock-sql "select ..."   # no provider key: the gateway returns this SQL

The --mock-sql path still goes through the gateway (auth, guardrail, logging); only the call to the
model provider is replaced by the given answer.
"""
import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request
from pathlib import Path

import warehouse_tools

GATEWAY = os.environ.get("GATEWAY_URL", "http://127.0.0.1:4000")
KEYS_FILE = Path(__file__).resolve().parent.parent / "gateway" / ".keys.json"

SYSTEM = """You write one DuckDB SQL SELECT that answers the user's question about a payments \
warehouse. Use only these tables and columns:

{schema}

Amounts ending in _cad are in Canadian dollars. has_fraud_chargeback marks confirmed fraud.
Reply with the SQL in a ```sql block and nothing else."""


def ask_gateway(question: str, key: str, mock_sql: str | None) -> str:
    tables = warehouse_tools.schema()
    schema = "\n".join(f"{t}({', '.join(f'{c} {ty}' for c, ty in cols)})" for t, cols in tables.items())
    body = {"model": "default", "temperature": 0,
            "messages": [{"role": "system", "content": SYSTEM.format(schema=schema)},
                         {"role": "user", "content": question}]}
    if mock_sql:
        body["mock_response"] = f"```sql\n{mock_sql}\n```"
    request = urllib.request.Request(f"{GATEWAY}/v1/chat/completions", data=json.dumps(body).encode(),
                                     headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.loads(response.read())["choices"][0]["message"]["content"]
    except urllib.error.HTTPError as error:
        sys.exit(f"gateway refused the request: HTTP {error.code} {error.read().decode()[:300]}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("question")
    parser.add_argument("--team", default="analytics", help="whose gateway key to use (local-test for --mock-sql)")
    parser.add_argument("--mock-sql", help="skip the model provider; the gateway answers with this SQL")
    args = parser.parse_args()

    keys = json.loads(KEYS_FILE.read_text()) if KEYS_FILE.exists() else {}
    if args.team not in keys:
        sys.exit("no gateway key for that team: run python gateway/setup_keys.py")
    answer = ask_gateway(args.question, keys[args.team], args.mock_sql)
    match = re.search(r"```sql\s*(.*?)```", answer, re.S)
    sql = (match.group(1) if match else answer).strip()
    print(f"SQL:\n{sql}\n")
    try:
        columns, rows = warehouse_tools.run(sql)
    except warehouse_tools.QueryRejected as error:
        sys.exit(f"Rejected: {error}")
    print(warehouse_tools.as_markdown(columns, rows))


if __name__ == "__main__":
    main()
