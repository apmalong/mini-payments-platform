import json
import os
import sys
from pathlib import Path

import pytest

E2E = Path(__file__).resolve().parent.parent
ROOT = E2E.parent
sys.path.insert(0, str(E2E))

from portal_server import portal  # noqa: E402


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


WALKTHROUGH = load_json(ROOT / "portal" / "walkthrough.json")
# The smoke tier can point at another environment's service list (E2E_SERVICES=path/to/services.json).
SERVICES = load_json(Path(os.environ.get("E2E_SERVICES", ROOT / "portal" / "services.json")))
SERVICE_URLS = {s["name"]: s["url"] for g in SERVICES["groups"] for s in g["services"] if s.get("url")}
STEPS = [(case, step) for case in WALKTHROUGH["use_cases"] for step in case["steps"]]


@pytest.fixture(scope="session")
def portal_url():
    with portal() as url:
        yield url


@pytest.fixture
def console_errors(page):
    """Errors the page logs or throws; tests assert it's empty at the end."""
    errors = []
    page.on("console", lambda message: message.type == "error" and errors.append(message.text))
    page.on("pageerror", lambda error: errors.append(str(error)))
    return errors
