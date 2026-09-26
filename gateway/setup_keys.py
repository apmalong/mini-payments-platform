"""Module 10: create per-team virtual keys on the AI gateway. Standard library only.

Each team gets its own key with a budget and a rate limit, so spend and abuse are attributable
and capped per team rather than shared behind one provider key. Keys are saved to
gateway/.keys.json (gitignored); rerunning reuses keys that still exist.

    python gateway/setup_keys.py
"""
import json
import os
import urllib.error
import urllib.request
from pathlib import Path

GATEWAY = os.environ.get("GATEWAY_URL", "http://127.0.0.1:4000")
MASTER_KEY = os.environ.get("LITELLM_MASTER_KEY", "sk-local-master")
KEYS_FILE = Path(__file__).resolve().parent / ".keys.json"

TEAMS = {
    # alias: budget (USD), requests per minute, models
    "analytics": {"max_budget": 5.0, "rpm_limit": 20, "models": ["default", "fast"]},
    "fraud-ops": {"max_budget": 20.0, "rpm_limit": 60, "models": ["default", "fast"]},
    # Test keys may send "mock_response" (the gateway answers without calling the model provider).
    # The gateway strips client mocks unless the key allows them, so team keys can't fake answers.
    "local-test": {"max_budget": 1.0, "rpm_limit": 30, "models": ["default", "fast"],
                   "metadata": {"allow_client_mock_response": True}},
    "ratelimit-demo": {"max_budget": 1.0, "rpm_limit": 3, "models": ["fast"],
                       "metadata": {"allow_client_mock_response": True}},
}


def call(path: str, body: dict | None = None, method: str = "POST") -> dict:
    request = urllib.request.Request(
        f"{GATEWAY}{path}", method=method, data=json.dumps(body).encode() if body is not None else None,
        headers={"Authorization": f"Bearer {MASTER_KEY}", "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read())


def key_exists(key: str) -> bool:
    try:
        call(f"/key/info?key={key}", method="GET")
        return True
    except urllib.error.HTTPError:
        return False


def main() -> None:
    keys = json.loads(KEYS_FILE.read_text()) if KEYS_FILE.exists() else {}
    for alias, settings in TEAMS.items():
        limits = {k: v for k, v in settings.items() if k != "metadata"}
        metadata = {"team": alias, **settings.get("metadata", {})}
        if alias in keys and key_exists(keys[alias]):
            call("/key/update", {"key": keys[alias], "metadata": metadata, **limits})  # keep in sync
            print(f"{alias:<15} exists, limits synced")
            continue
        created = call("/key/generate", {"key_alias": alias, "metadata": metadata, **limits})
        keys[alias] = created["key"]
        print(f"{alias:<15} created: budget ${limits['max_budget']}, {limits['rpm_limit']} req/min, "
              f"models {', '.join(limits['models'])}")
    KEYS_FILE.write_text(json.dumps(keys, indent=2))
    print(f"keys saved to {KEYS_FILE}")


if __name__ == "__main__":
    main()
