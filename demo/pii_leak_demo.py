"""Demo: a mart that leaks customer emails, disguised, gets blocked by the PII lineage check.

    python demo/pii_leak_demo.py

Adds a mart that renames stg_customers.email to `contact` through a CTE (a name-based check would
miss it), builds it, runs governance/check_pii.py (expect exit 1, "LEAK"), then removes the model
and its table whatever happens.

Runs against the CI warehouse (ci/warehouse_ci.duckdb, build it with ci/run_ci.py) so it can't
collide with the live pipeline, which holds the working warehouse's single write lock while it runs.
"""
import os
import subprocess
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
DBT_DIR = ROOT / "warehouse" / "dbt"
LEAKY_MODEL = DBT_DIR / "models" / "marts" / "mart_customer_contacts.sql"
WAREHOUSE = ROOT / "ci" / "warehouse_ci.duckdb"
ENV = {**os.environ, "DUCKDB_PATH": str(WAREHOUSE), "DBT_TARGET_PATH": str(ROOT / "ci" / "target"),
       "DBT_SEND_ANONYMOUS_USAGE_STATS": "false", "PYTHONIOENCODING": "utf-8"}
BIN = Path(sys.executable).parent


def main() -> None:
    sys.stdout.reconfigure(line_buffering=True)  # narration interleaves correctly with dbt's output
    if not WAREHOUSE.exists():
        sys.exit("No CI warehouse yet: run python ci/run_ci.py once first.")
    print("1. A new mart, written the way a leak really happens: renamed, via a CTE\n")
    LEAKY_MODEL.write_text(
        "with c as (select customer_id, lower(email) as contact_value, country from {{ ref('stg_customers') }})\n"
        "select customer_id, contact_value as contact, country from c\n", encoding="utf-8")
    print("   " + LEAKY_MODEL.read_text(encoding="utf-8").replace("\n", "\n   "))
    try:
        print("2. It builds fine: dbt has no reason to object\n")
        subprocess.run([str(BIN / "dbt"), "run", "--quiet", "--select", "mart_customer_contacts",
                        "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)], env=ENV, check=True)
        print("   built analytics.mart_customer_contacts\n")
        print("3. The PII lineage check traces every mart column back through the compiled SQL\n")
        result = subprocess.run([str(BIN / "python"), str(ROOT / "governance" / "check_pii.py")], env=ENV)
        print(f"\n   exit code {result.returncode}: "
              f"{'BLOCKED - in CI the PR fails; in Airflow the run stops before publish' if result.returncode else 'not blocked?!'}")
    finally:
        LEAKY_MODEL.unlink(missing_ok=True)
        conn = duckdb.connect(str(WAREHOUSE))
        conn.execute("drop table if exists analytics.mart_customer_contacts")
        conn.close()
        print("\n4. Cleaned up: model file and table removed")


if __name__ == "__main__":
    main()
