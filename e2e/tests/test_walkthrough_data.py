"""portal/walkthrough.json only names services, docs and tabs that exist."""
import json
import re

from conftest import ROOT, SERVICE_URLS, STEPS, WALKTHROUGH

SLUG = re.compile(r"^[a-z0-9-]+$")
TABS = {"#walkthrough", "#services", "#status", "#architecture", "#docs"}


def test_ids_are_unique_slugs():
    case_ids = [case["id"] for case in WALKTHROUGH["use_cases"]]
    assert len(case_ids) == len(set(case_ids))
    for case in WALKTHROUGH["use_cases"]:
        step_ids = [step["id"] for step in case["steps"]]
        assert len(step_ids) == len(set(step_ids)), case["id"]
        assert all(SLUG.match(i) for i in [case["id"], *step_ids]), case["id"]


def test_every_step_says_what_to_do():
    for case, step in STEPS:
        where = f"{case['id']}/{step['id']}"
        assert step.get("title") and step.get("body"), where
        assert step.get("run") or step.get("links") or step.get("screenshot"), f"{where} has nothing to act on"


def test_links_resolve():
    for case, step in STEPS:
        for link in step.get("links", []):
            where = f"{case['id']}/{step['id']}: {link}"
            assert link.get("label"), where
            if "service" in link:
                assert link["service"] in SERVICE_URLS, where
            elif "doc" in link:
                assert (ROOT / "docs" / link["doc"]).is_file(), where
            else:
                assert link.get("portal", "").split("/")[0] in TABS, where


def test_screenshot_manifest_matches_the_files():
    """The page shows only what index.json lists; e2e/capture.py rewrites it after every capture."""
    folder = ROOT / "portal" / "walkthrough"
    on_disk = sorted(p.relative_to(folder).with_suffix("").as_posix() for p in folder.glob("*/*.png"))
    assert json.loads((folder / "index.json").read_text(encoding="utf-8"))["screenshots"] == on_disk


def test_screenshot_specs_name_a_service_and_something_to_wait_for():
    for case, step in STEPS:
        spec = step.get("screenshot")
        if spec:
            where = f"{case['id']}/{step['id']}"
            assert spec["service"] == "portal" or spec["service"] in SERVICE_URLS, where
            assert spec.get("wait_for_text") or spec.get("wait_for_selector"), where
            assert spec.get("alt"), where
