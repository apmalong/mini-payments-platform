---
name: investigate-alert
description: Investigate an alert or "something's wrong" report on the payments platform using the read-only payments-platform MCP tools - find what's firing, follow its runbook, narrow the failing stage, and report a diagnosis with evidence. Use when asked why data is stale, why the scorer is failing, why drift or the review rate is high, or "what's broken".
---

# Investigate an alert

Use the `payments-platform` MCP server (`.mcp.json`). Its tools are read-only; any fix that
changes the platform (restarting, rerunning, retraining) is proposed to the user, not done.

## 1. What's wrong

Call `active_alerts`. Order by `firing` before `pending`, then `page` before `ticket` severity.
If several alerts share a cause (`MartDataStale` plus `TransformStalled`), treat them as one incident.

## 2. Follow the runbook

For each distinct alert, `read_runbook` with the path from the alert. Work its **Diagnose**
steps in order; they tell you which tool to use next.

## 3. Narrow it with evidence

- **Stale data:** `data_freshness`. The stage whose age jumps is the one that stalled
  (source fresh but raw old: ingestion; raw fresh but mart old: dbt).
- **A model or table question:** `model_lineage` for what it reads, what reads it, its tests
  and contract.
- **Numbers:** `query_warehouse` (call `list_tables` first). For example, compare today's volume
  with the daily average when a volume or drift alert fires:
  `select transaction_date, sum(transaction_count) from mart_merchant_daily_volume group by 1 order by 1 desc limit 8`

Some checks need the shell rather than the MCP server (pod status, container logs); run them only
after saying why, and prefer read-only commands (`kubectl get`, `docker ps`, `docker logs`).

## 4. Report

- **What's happening**, in one sentence, with the alert names.
- **Where**: the failing stage or component.
- **Evidence**: the tool outputs that show it (ages, counts, query results).
- **Likely cause** and your confidence.
- **Proposed fix**, from the runbook, as commands for the user to approve.

Don't guess past the evidence. If the tools can't narrow it down, say which check is missing.
Known expected state in this local environment: `ConsumerDown` and `SourceQuiet` fire whenever the
generator and consumer aren't running, and `FeatureDrift` / `ReviewRateHigh` reflect simulator drift
explained in the model-drift runbook.
