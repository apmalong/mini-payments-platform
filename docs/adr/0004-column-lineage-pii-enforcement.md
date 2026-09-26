# ADR-0004: Enforce PII rules with column-level lineage, not column names

- **Status:** accepted
- **Date:** 2026-09-26

## Context

Personal data must never reach a published mart unmasked, and this has to be provable to an auditor.
Checks based on column names (`email`, `phone`) miss a rename or a derived column.

## Options considered

1. **Name matching** on mart columns: simple, trivially bypassed by `select email as contact`.
2. **dbt meta tags only**, trusting authors: no enforcement.
3. **Column-level lineage from the compiled SQL** (sqlglot), from each mart column back to
   `pii: true` source columns, failing when no hashing function is on the path.
4. **OpenLineage + Marquez** for runtime lineage.

## Decision

Option 3 (`governance/check_pii.py`), run in CI and as an Airflow gate before `publish`. PII is
declared once, as dbt column meta. Access is by role views built from column classifications.

## Consequences

- Catches the disguised case: a test mart renaming `email` to `contact` through a CTE was blocked.
- Produces the auditor report (`docs/pii-lineage.md`) from the same analysis.
- dbt must be compiled first, and in a subprocess: in-process, dbt-duckdb keeps its connection and
  DuckDB refuses the checker's read-only one.
- Static lineage says where data *can* flow, not which jobs ran when; runtime lineage (Marquez)
  remains a gap.
- Role views on DuckDB are a simulation (no users or grants); on BigQuery they map to policy tags
  and authorized views. The same views bound AI agents (ADR-0009).
- The email hash is unsalted MD5, reversible with a list of candidate emails: a known gap, to be
  replaced by keyed HMAC.
