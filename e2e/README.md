# e2e: browser tests and walkthrough screenshots

Playwright (Python, Chromium) for two jobs: testing the portal, and capturing the screenshots
its Walkthrough tab shows.

```powershell
cd e2e; uv venv; uv pip install -r requirements.txt; .venv\Scripts\python -m playwright install chromium
```

## Tests

| Tier | Command | Needs |
|---|---|---|
| Portal | `.venv\Scripts\python -m pytest` | nothing running; starts its own portal. Internet for the page's CDN scripts |
| Smoke | `.venv\Scripts\python -m pytest -m smoke` | the platform running |

- **Portal** (runs in CI through `ci/run_ci.py`): `walkthrough.json` only names services, docs and
  tabs that exist; every step renders in order from the first to "Done"; deep links and arrow keys
  work; service links resolve through `services.json`; each screenshot shows or says how to capture
  it; doc links open in the portal; the other tabs render; no browser console errors.
- **Smoke**: every UI the walkthrough screenshots answers and shows the text its screenshot waits
  for. Point it at another environment with `E2E_SERVICES=path\to\services.json`.

## Screenshots

```powershell
.venv\Scripts\python capture.py                     # every step whose service is reachable
.venv\Scripts\python capture.py --only pipeline     # one use case, or pipeline/airflow for one step
```

Each step's `screenshot` spec in `portal/walkthrough.json` names the service and path to open, the
text or selector to wait for, text to scroll to, and selectors to `mask` (keys, emails). Images go
to `portal/walkthrough/<use case>/<step>.png` at 1440×900, and `portal/walkthrough/index.json`
lists them, so the page only shows images that exist. Unreachable services are skipped and listed.

For another environment, pass `--services` with that environment's service list, and
`--storage-state` with a saved Playwright login if its UIs sit behind sign-in (IAP on GCP).
