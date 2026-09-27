# Demo script: 10 minutes

A live walk-through of the platform for an interview, mapped to the Senior Data Platform Engineer
role: pipelines, governance, real-time ML, Kubernetes, AI tooling, and the judgment behind them.

Every step has what to **show**, what to **say**, and a **fallback** if it misbehaves. Rehearse it
once end to end; the first run of the day is the one that finds a stopped container.

## 15 minutes before

Memory is tight (Docker has 8 GB), so start things in this order, from
`C:\projects\test\mini-payments-platform`:

```powershell
docker start mpp-airflow                                   # ~1.3 GB; everything else is usually up
docker exec mpp-airflow airflow dags unpause payments_pipeline
docker exec mpp-airflow airflow dags trigger payments_pipeline   # so the mart is fresh on screen
```

Two terminals that stay running:

```powershell
cd streaming; uv run python consumer.py --score-url http://127.0.0.1:8091/score   # terminal 1
cd generator; uv run python generate.py --continuous --stream                    # terminal 2
```

Then `uv run python demo/preflight.py` until it says **READY**. A third terminal stays open in the
project folder for the demo commands.

Open these tabs, in this order:

1. Portal, Architecture tab: http://localhost:8099/#architecture
2. Airflow, `payments_pipeline` graph: http://localhost:8088/dags/payments_pipeline
3. Grafana dashboard: http://localhost:3000/d/payments-platform
4. Redpanda Console topic: http://localhost:8085/topics/payments.transactions
5. Portal, Docs tab: http://localhost:8099/#docs/governance.md

## The ten minutes

### 0:00 - 1:00 · What this is

**Show:** portal, Architecture tab, the system overview.

**Say:** "This is a small payments data platform I built to work through the problems in this
role. Everything runs on one laptop, but in production shapes: a source system, batch ELT into a
warehouse with dbt and Airflow, a real-time path that scores every transaction for fraud, and the
governance, MLOps and observability around it. Batch runs along the top, real time along the bottom,
and they meet in the model."

**Fallback:** the same diagrams are in `docs/architecture.md`.

### 1:00 - 3:00 · A pipeline you can trust

**Show:** Airflow graph. Point at the order: `source_contract` → `ingest` → one task per dbt model with
tests after each → `pii_check` → `role_views` → `publish`. Then Grafana's "Is the data fresh?" row.

**Say:**
- "The gates are the point. A breaking source schema change stops the run before ingestion; a
  failed test or an unmasked personal column stops it before anything is published."
- "Ingestion is incremental on `updated_at`. Getting that right took three fixes: a clock-skew bug,
  future-dated rows, and a PyAirbyte full refresh that silently broke every later incremental run.
  I only found the last one because I checked row counts against the source instead of trusting
  the sync's success message."
- "Freshness is measured per layer, so the alert says which stage stalled, not just that the mart is old."

**Fallback:** if the run is mid-flight, that's fine; show the running tasks. If Airflow is down,
show the batch pipeline diagram and `docs/runbooks/mart-data-stale.md`.

### 3:00 - 5:00 · Governance that holds up to an auditor

**Show:** terminal: `uv run python demo/pii_leak_demo.py` (about 30 s). Then the portal's Data
Governance doc, the controls table and the "Known gaps" section.

**Say:**
- "This is how a leak really happens: nobody writes `select email`. The email goes through a CTE
  and comes out named `contact`. A name-based check misses it. This one traces every mart column back
  through the compiled SQL, so it's blocked: in CI the pull request fails, in Airflow the run stops
  before publishing."
- "The governance doc is written for an auditor: what's controlled, how, and how to verify each one.
  And it lists the gaps, like the email hash being unsalted MD5. I'd rather show that than have it found."

**Fallback:** `docs/pii-lineage.md` shows the lineage report the check produces.

### 5:00 - 7:00 · Real-time fraud scoring

**Show:** terminal: `cd generator; uv run python ../demo/fraud_burst.py`. While it waits, flip to
Redpanda Console to show the events arriving. When the table fills, point at the score column.
Then Grafana's "Decisions per second": the review spike.

**Say:**
- "That's a card-testing attack: tiny charges on one card, then a big one. The score is low for
  the first few, then the card's velocity and a new IP country push it over the line partway through."
- "Features update in Redis in one atomic Lua script: dedupe, windows and the read, so a redelivered
  event can't be counted twice."
- "The subtle part is training/serving skew. The model trains on features computed in dbt and is
  served features computed in the stream. A check comparing the two found online merchant statistics
  matching offline only 1.4% of the time: the online store started cold. Bootstrapping it from the
  warehouse took that to 85%."
- "The scorer runs on Kubernetes. I tested a rolling upgrade under load, zero dropped requests out of
  18,744, and killing a node, which cost the six requests that were in flight on it."

**Fallback:** if scores don't appear, the consumer isn't running with `--score-url`; show the result
table from ADR-0007 and the model lifecycle diagram instead.

### 7:00 - 9:00 · AI tooling with guardrails

**Show:** terminal: `uv run python demo/guardrail_demo.py`. Then, in Claude Code opened in this
folder: *"Use the investigate-alert skill: what's wrong with the platform right now?"*

**Say:**
- "Every LLM call goes through one gateway: per-team keys, budgets, rate limits, an audit log, and
  personal data redacted before anything leaves. That's the prompt I typed, and that's what left."
- "Agents get the platform through an MCP server with read-only tools. The access rules are enforced
  below the model: a query for customer emails from the staging table is refused no matter who or
  what wrote it. The control doesn't depend on the model behaving."
- "The skill encodes how we investigate: alerts, then the runbook, then evidence, then a proposed
  fix for a human to approve."

**Fallback:** if the agent is slow, show `docs/runbooks/` and the MCP tool list in `agent/mcp_server.py`.

### 9:00 - 10:00 · A decision and what I'd change

**Show:** portal Docs → ADR-0005 (features) or ADR-0007 (Kubernetes), then the roadmap.

**Say:** pick one decision, the option rejected, and what it cost. Then the roadmap: "At Helcim's
scale, the first things I'd change are real access control with BigQuery policy tags, keyed hashing
for identifiers, and client-side retries for the scorer, because those are the gaps with the most
risk. I keep a debt register so those are visible, not folklore."

## Questions to expect

| Question | Short answer |
|---|---|
| Why DuckDB, not BigQuery? | Free, offline, fast to rebuild. The dbt project keeps a BigQuery target, with dispatched macros for the SQL that differs. The single-writer limit shaped the pipeline; BigQuery removes it (ADR-0001). |
| How do you know incremental loads are correct? | Rebuild from scratch and compare a fingerprint of every row with the incremental result; they matched when I built the model. Also, `fct_transactions` reprocesses rows when late refunds or chargebacks arrive, not just when the transaction changes. |
| How would you roll this out to a team? | CI that runs the whole pipeline on every PR, a review checklist, runbooks per alert, ADRs for decisions, and skills that encode the conventions so new sources are self-serve. |
| What if the fraud model is wrong? | It fails open in the stream (a missing score never blocks a payment), promotion is gated on beating the current model on the same data, and drift and review-rate alerts say when live traffic no longer looks like training. |
| Labels arrive late: how do you train? | Only on transactions older than the 45-day chargeback window, split by time. About 30% of fraud is never disputed, so labels are noisy; that's on the roadmap. |
| Why not Feast? | Two feature implementations plus a skew check was less to run here. It needs discipline; a feature store is on the roadmap once more models share features (ADR-0005). |

## Afterwards

Free the memory: stop the two terminals (Ctrl+C), then
`docker exec mpp-airflow airflow dags pause payments_pipeline` once no run is active, and
`docker stop mpp-airflow`.
