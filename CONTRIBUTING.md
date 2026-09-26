# Contributing

## Before you open a pull request

```powershell
cd mini-payments-platform
ci\.venv\Scripts\python ci\run_ci.py        # lint, tests, source fixture, dbt build, PII check (~80 s)
```

First time: `uv venv ci\.venv --python 3.12; uv pip install -r ci\requirements.txt --python ci\.venv`.
CI runs the same script on every pull request; if it passes locally it passes there.

Changed dbt models? Also run `uv run dbt parse --profiles-dir .` in `warehouse\dbt`: Airflow
renders its tasks from the manifest.

Adding a raw table? Use the `new-dbt-source` skill in Claude Code, or follow its steps by hand.

## What reviewers check on a data PR

**Correctness**
- [ ] Primary keys tested `unique` + `not_null`; foreign keys tested with `relationships`
- [ ] Duplicates handled where the source can duplicate (retries, CDC replays)
- [ ] Incremental models are idempotent: rerunning or backfilling gives the same result as a full refresh
- [ ] Late-arriving data is picked up (think refunds and chargebacks that land weeks later)
- [ ] Timestamps are UTC end to end

**Governance**
- [ ] Every new personal column is tagged `pii: true` with a classification
- [ ] Marts expose personal data only through a masking macro; `check_pii.py` passes
- [ ] Changes to a mart's columns or types are intentional (the contract will fail otherwise) and
      consumers are told
- [ ] Source contract updated if the producer's schema changed on purpose

**Operability**
- [ ] New failure modes have an alert and a runbook, or the PR says why not
- [ ] A significant choice (new tool, new pattern, dropped option) has an ADR in `docs/adr/`

**Reviewing well**
- Ask for the evidence, not the claim: the test output, the query result, the screenshot.
- Prefer a question to a rewrite: "what happens when this runs twice?" teaches more than a diff.
- Approve when it's safe and clear, not when it's how you'd have written it.
