"""Smoke tier: every UI the walkthrough screenshots is up and shows what the screenshot expects.
Needs the platform running: `pytest -m smoke` (set E2E_SERVICES to check another environment)."""
from urllib.parse import urljoin

import pytest
from conftest import SERVICE_URLS, STEPS

UI_STEPS = [(case, step) for case, step in STEPS
            if step.get("screenshot") and step["screenshot"]["service"] != "portal"]


@pytest.mark.smoke
@pytest.mark.parametrize(("case", "step"), UI_STEPS, ids=[f"{c['id']}/{s['id']}" for c, s in UI_STEPS])
def test_ui_shows_expected_content(page, case, step):
    spec = step["screenshot"]
    page.goto(urljoin(SERVICE_URLS[spec["service"]], spec.get("path", "")), wait_until="domcontentloaded")
    if spec.get("wait_for_selector"):
        page.locator(spec["wait_for_selector"]).first.wait_for(timeout=20_000)
    if spec.get("wait_for_text"):
        page.get_by_text(spec["wait_for_text"]).filter(visible=True).first.wait_for(timeout=20_000)
