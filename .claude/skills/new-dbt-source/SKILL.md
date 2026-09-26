---
name: new-dbt-source
description: Scaffold a new raw source in the dbt project (sources.yml entry, staging model, tests, PII tags, docs) following this repo's conventions.
---

# New dbt source

Given a source table name:

1. Inspect the table's columns in the DuckDB `raw` schema.
2. Add it to `warehouse/dbt/models/staging/_sources.yml`, with a freshness check if it has `updated_at`.
3. Create `stg_<table>.sql`: rename to snake_case, cast types, dedupe on the primary key.
4. Add tests in `_stg_models.yml`: `unique` + `not_null` on the primary key, `relationships` for foreign keys.
5. Tag PII columns (email, name, phone, address, card) with `meta: {pii: true, classification: restricted}`.
6. Run `dbt build --select stg_<table>` and `python governance/check_pii.py`. Fix any failures.
7. Work on a branch. Never commit to main.
