"""Capture the walkthrough's screenshots into portal/walkthrough/<use case>/<step>.png.

Each step's `screenshot` spec in portal/walkthrough.json says which service and path to open, what
to wait for and what to mask; URLs come from services.json, so pointing --services at another
environment's file (GCP, say) re-captures every image from there.

    e2e/.venv/Scripts/python e2e/capture.py                      # every step whose service is up
    e2e/.venv/Scripts/python e2e/capture.py --only pipeline      # one use case (or pipeline/airflow)
    e2e/.venv/Scripts/python e2e/capture.py --storage-state auth.json   # UIs behind a login (IAP)

A service that isn't reachable is skipped and listed; existing images are left as they are.
"""
import argparse
import contextlib
import json
import sys
from pathlib import Path
from urllib.parse import urljoin

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright
from portal_server import portal, reachable

ROOT = Path(__file__).resolve().parent.parent
PORTAL_DIR = ROOT / "portal"


def load_steps(only: str | None) -> list[tuple[str, str, dict]]:
    walkthrough = json.loads((PORTAL_DIR / "walkthrough.json").read_text(encoding="utf-8"))
    steps = [(case["id"], step["id"], step["screenshot"]) for case in walkthrough["use_cases"]
             for step in case["steps"] if step.get("screenshot")]
    if only:
        steps = [s for s in steps if f"{s[0]}/{s[1]}" == only or s[0] == only]
    return steps


def service_urls(path: Path) -> dict[str, str]:
    config = json.loads(path.read_text(encoding="utf-8"))
    return {s["name"]: s["url"] for g in config["groups"] for s in g["services"] if s.get("url")}


def capture(page, url: str, spec: dict, target: Path) -> None:
    page.goto(url, wait_until="domcontentloaded", timeout=30_000)
    if spec.get("wait_for_selector"):
        page.locator(spec["wait_for_selector"]).first.wait_for(timeout=20_000)
    if spec.get("wait_for_text"):
        page.get_by_text(spec["wait_for_text"]).first.wait_for(timeout=20_000)
    if spec.get("scroll_to_text"):
        page.get_by_text(spec["scroll_to_text"]).first.evaluate("e => e.scrollIntoView({block: 'start'})")
    page.wait_for_timeout(spec.get("delay_ms", 500))
    masks = [page.locator(selector) for selector in spec.get("mask", [])]
    target.parent.mkdir(parents=True, exist_ok=True)
    page.screenshot(path=str(target), mask=masks)


def write_manifest() -> list[str]:
    """portal/walkthrough/index.json: every screenshot on disk, as <use case>/<step>."""
    folder = PORTAL_DIR / "walkthrough"
    screenshots = sorted(p.relative_to(folder).with_suffix("").as_posix() for p in folder.glob("*/*.png"))
    folder.mkdir(exist_ok=True)
    (folder / "index.json").write_text(json.dumps({"screenshots": screenshots}, indent=2) + "\n", encoding="utf-8")
    return screenshots


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--only", help="a use case id, or <use case>/<step>")
    parser.add_argument("--services", type=Path, default=PORTAL_DIR / "services.json")
    parser.add_argument("--portal-url", default="http://localhost:8099",
                        help="a running portal; if it isn't up, a temporary one is started")
    parser.add_argument("--storage-state", help="Playwright storage state with logged-in sessions")
    parser.add_argument("--width", type=int, default=1440)
    parser.add_argument("--height", type=int, default=900)
    args = parser.parse_args()

    steps = load_steps(args.only)
    urls = service_urls(args.services)
    captured, skipped = [], []
    with contextlib.ExitStack() as stack:
        portal_url = args.portal_url
        if any(spec["service"] == "portal" for _, _, spec in steps) and not reachable(portal_url):
            portal_url = stack.enter_context(portal(collector=True))
            print(f"portal not running at {args.portal_url}; started one at {portal_url}")
        playwright = stack.enter_context(sync_playwright())
        browser = playwright.chromium.launch()
        context = browser.new_context(viewport={"width": args.width, "height": args.height}, color_scheme="light",
                                      device_scale_factor=1, storage_state=args.storage_state)
        page = context.new_page()
        for case_id, step_id, spec in steps:
            name = f"{case_id}/{step_id}"
            base = portal_url if spec["service"] == "portal" else urls.get(spec["service"])
            if not base:
                skipped.append((name, f"no service named {spec['service']!r} in {args.services.name}"))
                continue
            url = urljoin(base, spec.get("path", ""))
            if not reachable(url.split("#")[0]):
                skipped.append((name, f"{spec['service']} not reachable at {url}"))
                continue
            try:
                capture(page, url, spec, PORTAL_DIR / "walkthrough" / case_id / f"{step_id}.png")
                captured.append(name)
                print(f"captured {name}")
            except PlaywrightError as exc:
                skipped.append((name, str(exc).splitlines()[0]))
        browser.close()

    total = len(write_manifest())
    print(f"\n{len(captured)} captured, {len(skipped)} skipped; {total} screenshots in portal/walkthrough/index.json")
    for name, reason in skipped:
        print(f"  skipped {name}: {reason}")
    sys.exit(1 if not captured and skipped else 0)


if __name__ == "__main__":
    main()
