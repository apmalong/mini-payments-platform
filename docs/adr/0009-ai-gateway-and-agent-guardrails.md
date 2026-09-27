# ADR-0009: An AI gateway for every LLM call; agents constrained at the data layer

- **Status:** accepted
- **Date:** 2026-09-26

## Context

Teams and agents will call LLMs and query the warehouse. Cost, rate, logging and PII exposure must
be controlled centrally, and an agent must not be able to read personal data however it is prompted.

## Options considered

1. Each app holds a provider key and applies its own rules.
2. **LiteLLM gateway**: virtual keys per team, budgets, rate limits, logging, guardrails, fallback.
3. For agents: rely on prompts ("don't query PII"), or enforce access in code below the agent.

## Decision

All LLM traffic goes through LiteLLM with per-team keys and a pre-call guardrail that redacts
emails, phones and Luhn-valid card numbers. Agents (the MCP server, `ask.py`) read the warehouse
only through `agent/warehouse_tools.py`: one SELECT, `role_analyst` views only, read-only, capped.
MCP tools are read-only and annotated as such; writes stay behind the user's approval.

## Consequences

- The guardrail was verified in the audit log: the stored prompt had all three kinds of PII redacted.
- **Client-supplied mock responses are stripped** unless a key allows them; team keys can't fake model
  output. Only test keys (`local-test`, `ratelimit-demo`) may.
- **The rate limiter counts rejected requests** (sliding window): a client retrying in a loop stays
  locked out. Clients must back off on 429.
- Access control doesn't depend on the model behaving: SQL reading customer emails from staging is
  rejected whatever produced it (tested with the SQL supplied through mock mode, since no provider
  key was configured; the guard never sees who wrote the query).
- Names aren't redacted (needs NER, e.g. Presidio); the agent guard inherits the role-views
  simulation of ADR-0004.
- Verified with real model calls: asked to repeat an email, card and phone number, Haiku answered that it
  "can't repeat back those redacted placeholders", confirming from the model's side that the values never
  left. A real question produced correct SQL through the guard; asked for emails and pointed at staging,
  the real model stayed within the analyst tables, so the guard is the backstop, not the usual path.
  Spend is tracked per key (fractions of a cent per call).
