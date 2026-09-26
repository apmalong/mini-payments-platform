# Data Governance

For auditors, compliance and security reviewers: where personal and card data lives in the
payments data platform, how it is protected, and how each control can be verified.

## 1. Data inventory

| Data | Where it exists | Classification |
|---|---|---|
| Card numbers (PAN) | **Nowhere.** Tokenized in the source system before storage. Only `card_token`, BIN (first 6) and last 4 digits exist. | n/a |
| Customer name, email, phone | Source Postgres (`customers`, `transactions.customer_email`), warehouse `raw` schema, `staging` views | restricted |
| Hashed customer email | `analytics.fct_transactions.customer_email_hash` | confidential |
| Transactions, merchants, refunds, chargebacks (no personal fields) | All layers | internal |

The authoritative list of personal-data columns is the set of columns tagged `pii: true` in the
dbt project (`warehouse/dbt/models/**/_*.yml`). [pii-lineage.md](pii-lineage.md) lists where each
one flows.

## 2. Classification

Defined in [governance/pii_policy.yml](../governance/pii_policy.yml):

- **internal**: no personal information. Any employee.
- **confidential**: pseudonymous personal data (hashed identifiers). Named roles only.
- **restricted**: direct identifiers. Never in a published layer (marts).

## 3. Controls

| # | Control | How it is enforced | When it runs |
|---|---|---|---|
| C1 | No PAN in the platform | Source system stores tokens only; the raw contract declares `card_token` and no PAN column | Every pipeline run (contract check) |
| C2 | Restricted data never reaches a mart unmasked | `governance/check_pii.py` traces every mart column through the compiled SQL (column-level lineage) and fails if it derives from a `pii: true` column without a hashing function on the path. Renames and CTEs don't hide a leak. | CI on every pull request; Airflow `pii_check` task gates publishing on every run |
| C3 | Role-based column access | `dbt run-operation create_role_views` builds one schema of views per role from the column classifications (`vars.access_roles` in `dbt_project.yml`). `analyst`: marts, internal columns only. `fraud_ops`: marts and staging, all classifications. | Airflow `role_views` task, every run |
| C4 | Source changes can't silently corrupt data | `governance/check_contracts.py` compares the live source schema with [contracts/transactions.yml](../governance/contracts/transactions.yml); a missing column, type change or new nullability stops the run before ingestion | Airflow `source_contract` task, every run |
| C5 | Published tables keep their shape | dbt model contracts (`contract: {enforced: true}`) on all marts: names and types are checked at build time | Every dbt build |
| C6 | Data quality | 30+ dbt tests (uniqueness, referential integrity, accepted values and ranges, over-refunds) and a volume anomaly test on `fct_transactions` | After each model, in the pipeline |

## 4. Evidence

| To show that... | Look at |
|---|---|
| Which personal data reaches published tables, and how it is masked | [pii-lineage.md](pii-lineage.md), regenerated with `python governance/check_pii.py --report docs/pii-lineage.md` |
| C2 blocks a leak | Adding a mart that selects `stg_customers.email` (even renamed) makes `check_pii.py` exit 1 and the Airflow run stop before `publish` |
| Controls ran on a given date | Airflow run history for `payments_pipeline` (task logs for `source_contract`, `pii_check`, `role_views`) |
| What a role can see | `select table_name, column_name from information_schema.columns where table_schema = 'role_analyst'` |
| No PAN is stored | Source contract (C1) plus `stg_cards` columns: `card_token`, `bin`, `last4` |

## 5. Known gaps

Stated plainly so they can be tracked, not discovered.

1. **The email hash is unsalted MD5.** Anyone with a list of candidate emails can hash them and
   match. Fix: keyed HMAC-SHA256 with the key in a secret manager, so hashes are only computable
   by the platform. Until then, `customer_email_hash` is classified confidential, not internal.
2. **Role views are a simulation.** DuckDB has no users or grants, so nothing stops a process
   with file access from reading `staging`. On BigQuery the same policy becomes policy tags on
   columns (column-level security) and authorized views per role, enforced by IAM.
3. **No access audit log.** Reads are not recorded. BigQuery provides this through Cloud Audit
   Logs (Data Access).
4. **No retention or deletion process.** Customer data is kept indefinitely; there is no
   right-to-erasure workflow. Needed before any real personal data is processed.
5. **One source contract.** Only `transactions` has a contract; `customers`, `refunds` and
   `chargebacks` should get one each.
6. **Runtime lineage is not captured.** Column lineage is derived statically from SQL. Run-level
   lineage (which job read and wrote what, when) would need OpenLineage and a store such as Marquez.
