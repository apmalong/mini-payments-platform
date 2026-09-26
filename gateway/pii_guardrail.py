"""Module 10: LiteLLM guardrail that redacts personal data before a prompt leaves the network.

Runs on every request (pre_call, default on). The same classes of data the warehouse treats as
PII (governance/pii_policy.yml) are replaced with placeholders: email addresses, phone numbers and
card numbers. Card numbers are only redacted when they pass the Luhn check, so order ids and
amounts with many digits go through untouched.

Names are not caught: they need an NER model (e.g. Presidio), which is the production upgrade.
"""
import re

from litellm.integrations.custom_guardrail import CustomGuardrail

EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
CARD = re.compile(r"\b(?:\d[ -]?){12,18}\d\b")
PHONE = re.compile(r"(?<!\w)(?:\+?1[ .-]?)?\(?\d{3}\)?[ .-]?\d{3}[ .-]?\d{4}(?!\w)")


def luhn_valid(digits: str) -> bool:
    total, parity = 0, len(digits) % 2
    for i, char in enumerate(digits):
        d = int(char)
        if i % 2 == parity:
            d = d * 2 - 9 if d > 4 else d * 2
        total += d
    return total % 10 == 0


def redact(text: str) -> tuple[str, int]:
    count = 0

    def card(match: re.Match) -> str:
        nonlocal count
        digits = re.sub(r"\D", "", match.group())
        if 13 <= len(digits) <= 19 and luhn_valid(digits):
            count += 1
            return "[REDACTED_CARD]"
        return match.group()

    text = CARD.sub(card, text)
    text, n = EMAIL.subn("[REDACTED_EMAIL]", text)
    count += n
    text, n = PHONE.subn("[REDACTED_PHONE]", text)
    return text, count + n


class PiiRedaction(CustomGuardrail):
    async def async_pre_call_hook(self, user_api_key_dict, cache, data: dict, call_type):
        redacted = 0
        for message in data.get("messages", []):
            content = message.get("content")
            if isinstance(content, str):
                message["content"], n = redact(content)
                redacted += n
            elif isinstance(content, list):  # multi-part content
                for part in content:
                    if part.get("type") == "text":
                        part["text"], n = redact(part["text"])
                        redacted += n
        if redacted:
            data.setdefault("metadata", {})["pii_redactions"] = redacted
        return data
