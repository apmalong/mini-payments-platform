"""Module 5: block personal data from reaching marts unmasked.

Traces every column of every mart back to the columns it is computed from, through the compiled
SQL of each model on the way (column-level lineage with sqlglot). A mart column fails when its
lineage reaches a column tagged `pii: true` and no hashing function sits on that path.

    python governance/check_pii.py                                # compile, check, exit 1 on leaks
    python governance/check_pii.py --report ../docs/pii-lineage.md  # also write the lineage report

Run it with the dbt virtualenv's Python (it needs dbt, sqlglot, duckdb and pyyaml).
"""
import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import duckdb
import yaml
from sqlglot import exp
from sqlglot.lineage import lineage

ROOT = Path(__file__).resolve().parent.parent
DBT_DIR = ROOT / "warehouse" / "dbt"
MANIFEST = DBT_DIR / "target" / "manifest.json"
POLICY = Path(__file__).resolve().parent / "pii_policy.yml"
MASKING_FUNCTIONS = (exp.MD5, exp.SHA, exp.SHA2)


@dataclass(frozen=True)
class Flow:
    """One path from a mart column back to a PII column."""
    pii_column: str   # relation.column
    path: tuple[str, ...]
    masked: bool


def relation_key(schema: str, table: str) -> str:
    return f"{schema}.{table}".lower()


def column_meta(column: dict) -> dict:
    return column.get("meta") or (column.get("config") or {}).get("meta") or {}


class Lineage:
    def __init__(self, manifest: dict, schema: dict):
        self.schema = schema
        self.nodes = {}   # relation key -> manifest node with SQL (models, snapshots)
        self.pii = set()  # relation key.column
        for node in [*manifest["nodes"].values(), *manifest["sources"].values()]:
            if node["resource_type"] not in ("model", "snapshot", "source", "seed"):
                continue
            key = relation_key(node["schema"], node.get("alias") or node.get("identifier") or node["name"])
            if node["resource_type"] in ("model", "snapshot") and node.get("compiled_code"):
                self.nodes[key] = node
            for name, column in node.get("columns", {}).items():
                if column_meta(column).get("pii"):
                    self.pii.add(f"{key}.{name.lower()}")
        self._cache: dict[str, list[Flow]] = {}

    def trace(self, relation: str, column: str) -> list[Flow]:
        """All PII columns that `relation.column` is derived from, with whether each is masked."""
        here = f"{relation}.{column.lower()}"
        if here in self.pii:
            return [Flow(here, (here,), False)]
        if here in self._cache:
            return self._cache[here]
        self._cache[here] = []  # guards against cycles
        node = self.nodes.get(relation)
        if node is None:
            return []  # source or seed without PII tags on this column
        if node["resource_type"] == "snapshot" and column.lower().startswith("dbt_"):
            return []  # snapshot bookkeeping (dbt_valid_from, ...), added by dbt, not the SQL

        flows = []
        root = lineage(column, node["compiled_code"], schema=self.schema, dialect="duckdb")
        stack = [(root, False)]
        while stack:
            current, masked = stack.pop()
            masked = masked or any(current.expression.find_all(*MASKING_FUNCTIONS))
            if current.downstream:
                stack.extend((child, masked) for child in current.downstream)
            elif isinstance(current.source, exp.Table):
                upstream = relation_key(current.source.db, current.source.name)
                for flow in self.trace(upstream, current.name.split(".")[-1]):
                    flows.append(Flow(flow.pii_column, (here, *flow.path), masked or flow.masked))
        self._cache[here] = flows
        return flows


def compile_project() -> None:
    # A separate process, not dbtRunner: dbt-duckdb keeps its connection open in-process, and
    # DuckDB refuses a second connection to the same file with a different (read-only) config.
    dbt = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
    result = subprocess.run(
        [str(dbt), "compile", "--quiet", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)])
    if result.returncode != 0:
        sys.exit(f"dbt compile failed with exit code {result.returncode}")


def load_schema() -> dict:
    """Column names of every relation in the warehouse, so sqlglot can expand `select *`."""
    path = os.environ.get("DUCKDB_PATH", str(ROOT / "warehouse.duckdb"))
    connection = duckdb.connect(path, read_only=True)
    schema: dict = {}
    for catalog, schema_name, table, column, data_type in connection.execute(
            "select table_catalog, table_schema, table_name, column_name, data_type "
            "from information_schema.columns").fetchall():
        schema.setdefault(catalog, {}).setdefault(schema_name, {}).setdefault(table, {})[column] = data_type
    connection.close()
    return schema


def write_report(path: Path, results: dict[str, list[Flow]], pii: set[str]) -> None:
    lines = [
        "# PII lineage report",
        "",
        "Generated by `governance/check_pii.py` from the compiled dbt SQL. Every mart column",
        "derived from a column tagged `pii: true` is listed with the path it takes.",
        "",
        "| PII source column | Mart column | Path | Masked |",
        "|---|---|---|---|",
    ]
    reached = set()
    for mart_column, flows in sorted(results.items()):
        for flow in sorted(set(flows), key=lambda f: f.pii_column):
            reached.add(flow.pii_column)
            steps = " ← ".join(f"`{step}`" for step in flow.path)
            lines.append(f"| `{flow.pii_column}` | `{mart_column}` | {steps} | "
                         f"{'yes' if flow.masked else '**NO**'} |")
    contained = sorted(pii - reached)
    lines += ["", "PII columns that reach no mart:", ""]
    lines += [f"- `{column}`" for column in contained] or ["- none"]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--report", type=Path, help="write a markdown lineage report here")
    parser.add_argument("--skip-compile", action="store_true", help="use the existing manifest")
    args = parser.parse_args()

    policy = yaml.safe_load(POLICY.read_text(encoding="utf-8"))
    published_layers = policy["published_layers"]

    if not args.skip_compile:
        compile_project()
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    graph = Lineage(manifest, load_schema())

    results: dict[str, list[Flow]] = {}
    for key, node in graph.nodes.items():
        if node["resource_type"] != "model" or node["fqn"][1] not in published_layers:
            continue
        database, schema_name = node["database"], node["schema"]
        for column in graph.schema[database][schema_name].get(node["alias"], {}):
            flows = graph.trace(key, column)
            if flows:
                results[f"{key}.{column}"] = flows

    leaks = {column: flows for column, flows in results.items() if any(not f.masked for f in flows)}
    for column, flows in sorted(results.items()):
        status = "LEAK" if column in leaks else "ok  "
        sources = ", ".join(sorted({f.pii_column for f in flows}))
        print(f"{status} {column} <- {sources}")
    if args.report:
        write_report(args.report, results, graph.pii)
        print(f"Report written to {args.report}")

    print(f"{len(graph.pii)} PII columns tagged, {len(results)} mart columns derived from them, "
          f"{len(leaks)} unmasked.")
    sys.exit(1 if leaks else 0)


if __name__ == "__main__":
    main()
