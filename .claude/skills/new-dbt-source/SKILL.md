---
name: new-dbt-source
description: Add a new raw table to the dbt project following this repo's conventions - source entry, deduplicated staging model, tests, PII classification, and a passing PII lineage check. Use when a new table lands in the DuckDB raw schema (or a new stream is added to ingestion/sync.py).
---

# New dbt source

Work in `warehouse/dbt`. Given a raw table name `<table>`:

## 1. Inspect before writing

- Columns and types: `uv run python -c "import duckdb; c=duckdb.connect('../../warehouse.duckdb', read_only=True); print(c.execute(\"describe raw.<table>\").fetchall())"`
- Find the primary key and check it's unique in raw. Retries can duplicate rows (see
  `stg_transactions`); if the key isn't unique, the staging model must dedupe.
- Note which columns hold personal data: names, emails, phones, addresses, card data.
  A full card number must never be in raw: stop and raise it if you see one.

## 2. Source entry

Add the table under `raw` in `models/staging/_sources.yml`. If it has `updated_at`, add freshness:

```yaml
      - name: <table>
        config:
          loaded_at_field: updated_at
          freshness:
            warn_after: {count: 15, period: minute}
            error_after: {count: 60, period: minute}
```

## 3. Staging model `models/staging/stg_<table>.sql`

- Select columns explicitly (no `select *` in the final select), rename to snake_case, cast types
  (`cast(amount as decimal(18, 2))`).
- Dedupe when the key isn't unique:
  `qualify row_number() over (partition by <key> order by updated_at desc, id desc) = 1`.
- Extract JSON with the dispatched macro `{{ json_field('col', 'path.to.field') }}`, never
  adapter-specific functions (the project also targets BigQuery).

## 4. Tests and classification in `models/staging/_stg_models.yml`

dbt 1.12 syntax: `data_tests:` and `arguments:` (not `tests:` or bare arguments).

```yaml
  - name: stg_<table>
    columns:
      - name: <key>
        data_tests: [unique, not_null]
      - name: <foreign_key>
        data_tests:
          - relationships:
              arguments: {to: ref('stg_<parent>'), field: <parent_key>}
      - name: email
        config:
          meta: {pii: true, classification: restricted}
```

Classifications (`governance/pii_policy.yml`): `restricted` for direct identifiers,
`confidential` for hashed or pseudonymous ones, otherwise leave unclassified (internal).

## 5. Verify - all must pass before you're done

```powershell
uv run dbt build --profiles-dir . --select stg_<table>
cd ..\..; warehouse\dbt\.venv\Scripts\python governance\check_pii.py   # exits 0: no unmasked PII in marts
cd warehouse\dbt; uv run dbt parse --profiles-dir .                   # refresh the manifest Airflow renders from
```

If a mart should expose a personal column, it must go through a masking macro
(`{{ mask_email('col') }}`) and be classified `confidential`; `check_pii.py` enforces this.

## 6. Finish

Work on a branch, never `main`. Report what you added, the test results, and any PII you found.
