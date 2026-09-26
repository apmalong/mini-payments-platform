"""Module 10: MCP server giving an AI agent (e.g. Claude Code) read-only access to the platform.

Tools:
    list_tables       tables the agent may query (the analyst role's views) and their columns
    query_warehouse   run a single read-only SELECT against those views (warehouse_tools.py)
    model_lineage     a dbt model's upstream/downstream, tests, contract and column classes
    data_freshness    age of the newest data in the source, raw and mart layers
    active_alerts     pending and firing Prometheus alerts, with their runbooks
    read_runbook      a runbook from docs/runbooks

Everything is read-only by design: tools an agent can call without asking should not be able to
change the platform. Writes (running dbt, retraining) stay with the agent's normal tools, which go
through the user's approval.

Registered for Claude Code in .mcp.json; runs over stdio.
"""
import json
import os
import urllib.request
from pathlib import Path

from mcp.server.mcpserver import MCPServer
from mcp.types import ToolAnnotations

import warehouse_tools

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "warehouse" / "dbt" / "target" / "manifest.json"
RUNBOOKS = ROOT / "docs" / "runbooks"
PROMETHEUS = os.environ.get("PROMETHEUS_URL", "http://127.0.0.1:9090")
EXPORTER = os.environ.get("EXPORTER_URL", "http://127.0.0.1:8001/metrics")

READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, openWorldHint=False)

mcp = MCPServer(
    "payments-platform",
    instructions=(
        "Read-only access to a local payments data platform. Start with list_tables before "
        "query_warehouse. For an incident, call active_alerts, then read_runbook for each alert, then "
        "data_freshness or model_lineage as the runbook directs. The warehouse is DuckDB SQL."
    ),
)


def fetch(url: str) -> str:
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read().decode()


@mcp.tool(annotations=READ_ONLY)
def list_tables() -> str:
    """List the warehouse tables you may query, with column names and types. These are the
    analyst role's views: personal and confidential columns are not included."""
    tables = warehouse_tools.schema()
    return "\n\n".join(f"{table}\n" + "\n".join(f"  {column}: {data_type}" for column, data_type in columns)
                       for table, columns in tables.items())


@mcp.tool(annotations=READ_ONLY)
def query_warehouse(sql: str) -> str:
    """Run one read-only SELECT (DuckDB SQL) against the tables from list_tables. Results are
    capped at 200 rows. Writes, other schemas and file-reading functions are rejected."""
    try:
        columns, rows = warehouse_tools.run(sql)
    except warehouse_tools.QueryRejected as error:
        return f"Rejected: {error}"
    return warehouse_tools.as_markdown(columns, rows)


@mcp.tool(annotations=READ_ONLY)
def model_lineage(model: str) -> str:
    """Describe a dbt model: what it reads from, what reads it, its tests, whether its contract is
    enforced, and each column's classification. `model` is a name like fct_transactions."""
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    nodes = {**manifest["nodes"], **manifest["sources"]}
    matches = [key for key, node in nodes.items() if node.get("name") == model
               and node["resource_type"] in ("model", "snapshot", "seed", "source")]
    if not matches:
        return f"No model named {model}. Models: " + ", ".join(sorted(
            n["name"] for n in manifest["nodes"].values() if n["resource_type"] == "model"))
    key = matches[0]
    node = nodes[key]
    name = lambda k: nodes[k]["name"] if k in nodes else k.split(".")[-1]  # noqa: E731
    children = manifest["child_map"].get(key, [])
    tests = sorted(nodes[c]["name"] for c in children if nodes.get(c, {}).get("resource_type") == "test")
    downstream = sorted(name(c) for c in children if nodes.get(c, {}).get("resource_type") != "test")
    lines = [
        f"{model} ({node['resource_type']}, {node.get('config', {}).get('materialized', '')})",
        f"description: {node.get('description') or '-'}",
        f"reads from: {', '.join(sorted(name(p) for p in manifest['parent_map'].get(key, []))) or '-'}",
        f"read by: {', '.join(downstream) or '-'}",
        f"contract enforced: {node.get('config', {}).get('contract', {}).get('enforced', False)}",
        f"tests ({len(tests)}): {', '.join(tests) or '-'}",
        "columns:",
    ]
    for column, spec in node.get("columns", {}).items():
        meta = spec.get("meta") or spec.get("config", {}).get("meta") or {}
        classification = meta.get("classification", "internal")
        lines.append(f"  {column}: {spec.get('data_type') or ''} [{classification}{', PII' if meta.get('pii') else ''}]")
    return "\n".join(lines)


@mcp.tool(annotations=READ_ONLY)
def data_freshness() -> str:
    """Age of the newest data in each layer: source (Postgres), raw (after the sync) and mart
    (fct_transactions). The freshness SLO is 15 minutes for the mart."""
    try:
        metrics = fetch(EXPORTER)
    except OSError as error:
        return f"The freshness exporter isn't reachable ({error}). Start it: cd observability; uv run python exporter.py"
    lines = []
    for line in metrics.splitlines():
        if line.startswith("payments_freshness_seconds{"):
            layer = line.split('layer="')[1].split('"')[0]
            seconds = float(line.rsplit(" ", 1)[1])
            status = "within SLO" if seconds <= 900 else "STALE"
            lines.append(f"{layer:<7} {seconds / 60:8.1f} min  {status if layer == 'mart' else ''}")
    return "\n".join(lines) or "No freshness metrics yet."


@mcp.tool(annotations=READ_ONLY)
def active_alerts() -> str:
    """Pending and firing alerts from Prometheus, with their summary and runbook."""
    try:
        alerts = json.loads(fetch(f"{PROMETHEUS}/api/v1/alerts"))["data"]["alerts"]
    except OSError as error:
        return f"Prometheus isn't reachable at {PROMETHEUS} ({error})."
    if not alerts:
        return "No pending or firing alerts."
    return "\n".join(
        f"[{a['state']}] {a['labels']['alertname']} ({a['labels'].get('severity', '-')}): "
        f"{a['annotations'].get('summary', '')}  runbook: {a['annotations'].get('runbook', '-')}"
        for a in sorted(alerts, key=lambda a: (a["state"] != "firing", a["labels"]["alertname"])))


@mcp.tool(annotations=READ_ONLY)
def read_runbook(name: str) -> str:
    """Read a runbook, e.g. 'scorer', 'consumer', 'mart-data-stale', 'model-drift' (the runbook
    paths from active_alerts also work)."""
    stem = Path(name).stem
    path = RUNBOOKS / f"{stem}.md"
    if not path.is_file():
        return "Runbooks: " + ", ".join(p.stem for p in sorted(RUNBOOKS.glob("*.md")))
    return path.read_text(encoding="utf-8")


if __name__ == "__main__":
    mcp.run()
