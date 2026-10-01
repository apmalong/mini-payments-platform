"""The portal in a real browser: the walkthrough from start to finish, deep links, and the other tabs.
Needs internet: the page loads React, marked and mermaid from unpkg."""
from urllib.parse import urljoin

from conftest import ROOT, SERVICE_URLS, STEPS, WALKTHROUGH
from playwright.sync_api import expect


def step_id(case, step) -> str:
    return f"{case['id']}/{step['id']}"


def test_walkthrough_is_the_landing_page(page, portal_url, console_errors):
    page.goto(portal_url)
    _, first_step = STEPS[0]
    expect(page.locator("article.step h3")).to_have_text(first_step["title"])
    expect(page.locator(".walk-list a")).to_have_count(len(WALKTHROUGH["use_cases"]))
    assert console_errors == []


def test_next_walks_every_step_in_order(page, portal_url, console_errors):
    page.goto(f"{portal_url}/#walkthrough")
    for case, step in STEPS:
        expect(page.locator("article.step")).to_have_attribute("data-step", step_id(case, step))
        expect(page.locator("article.step h3")).to_have_text(step["title"])
        next_link = page.locator(".step-nav a.primary")
        if next_link.count():
            next_link.click()
    expect(page.locator(".step-nav")).to_contain_text("Done")
    assert console_errors == []


def test_deep_link_and_arrow_keys(page, portal_url):
    page.goto(f"{portal_url}/#walkthrough/pipeline/airflow")
    expect(page.locator(".step-count")).to_have_text(f"Use case 2 of {len(WALKTHROUGH['use_cases'])} · step 2 of 5")
    page.keyboard.press("ArrowRight")
    expect(page).to_have_url(f"{portal_url}/#walkthrough/pipeline/lineage")
    page.keyboard.press("ArrowLeft")
    page.keyboard.press("ArrowLeft")
    expect(page).to_have_url(f"{portal_url}/#walkthrough/pipeline/source")


def test_service_links_point_at_services_json(page, portal_url):
    for case, step in STEPS:
        service_links = [link for link in step.get("links", []) if "service" in link]
        if not service_links:
            continue
        page.goto(f"{portal_url}/#walkthrough/{step_id(case, step)}")
        for link in service_links:
            base = SERVICE_URLS[link["service"]]
            href = urljoin(base, link["path"]) if link.get("path") else base
            anchor = page.locator(".step-link", has_text=link["label"]).first
            expect(anchor).to_have_attribute("href", href)
            expect(anchor).to_have_attribute("target", "_blank")


def test_screenshots_show_or_say_how_to_capture(page, portal_url):
    for case, step in STEPS:
        if not step.get("screenshot"):
            continue
        page.goto(f"{portal_url}/#walkthrough/{step_id(case, step)}")
        image = ROOT / "portal" / "walkthrough" / case["id"] / f"{step['id']}.png"
        if image.exists():
            expect(page.locator(".shot img")).to_be_visible()
            assert page.locator(".shot img").evaluate("img => img.naturalWidth") > 0, step_id(case, step)
        else:
            expect(page.locator(".shot-missing")).to_contain_text("capture.py")


def test_doc_links_open_in_the_portal(page, portal_url, console_errors):
    page.goto(f"{portal_url}/#walkthrough/pipeline/breaks")
    page.locator(".step-link", has_text="Runbook: stale data").click()
    expect(page).to_have_url(f"{portal_url}/#docs/runbooks/mart-data-stale.md")
    expect(page.locator("article.doc h1")).to_contain_text("Runbook: stale data")
    assert console_errors == []


def test_other_tabs_render(page, portal_url, console_errors):
    page.goto(f"{portal_url}/#services")
    expect(page.locator(".card").first).to_be_visible()
    page.goto(f"{portal_url}/#architecture")
    expect(page.locator(".doc .mermaid svg").first).to_be_visible(timeout=15_000)
    page.goto(f"{portal_url}/#status")
    expect(page.locator(".status-controls")).to_be_visible()
    assert console_errors == []
