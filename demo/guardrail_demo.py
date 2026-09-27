"""Demo: personal data in a prompt never leaves through the AI gateway. Standard library only.

    python demo/guardrail_demo.py

Sends a prompt containing an email, a card number and a phone number through the gateway with the
local-test key (mock response, so no provider key is needed), then reads the gateway's audit log
for that request and shows what it recorded as sent: the redacted prompt.
"""
import json
import time
import urllib.request
from pathlib import Path

GATEWAY = "http://127.0.0.1:4000"
MASTER_KEY = "sk-local-master"
PROMPT = "Why was jane.doe@example.com's card 4111 1111 1111 1111 declined? Call her back at 403-555-0199."


def request(path: str, key: str, body: dict | None = None):
    req = urllib.request.Request(f"{GATEWAY}{path}", data=json.dumps(body).encode() if body else None,
                                 headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.load(response)


def main() -> None:
    keys = json.loads((Path(__file__).resolve().parent.parent / "gateway" / ".keys.json").read_text())
    print(f"You send:\n  {PROMPT}\n")
    reply = request("/v1/chat/completions", keys["local-test"],
                    {"model": "default", "mock_response": "Looking into it.",
                     "messages": [{"role": "user", "content": PROMPT}]})
    request_id = reply["id"]
    for _ in range(15):  # the gateway writes spend logs in batches
        logs = request("/spend/logs?summarize=false", MASTER_KEY)
        entry = next((e for e in logs if e.get("request_id") == request_id), None)
        if entry:
            break
        time.sleep(2)
    else:
        raise SystemExit("request not in the audit log yet; try again in a few seconds")
    sent = entry["proxy_server_request"]["messages"][0]["content"]
    print(f"The gateway recorded as sent to the model provider:\n  {sent}\n")
    print(f"Team key: {entry['metadata'].get('user_api_key_alias')}  |  model: {entry.get('model_group')}  |  "
          f"audit log id: {request_id}")


if __name__ == "__main__":
    main()
