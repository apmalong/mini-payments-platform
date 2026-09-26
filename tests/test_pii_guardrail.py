import pytest

from pii_guardrail import luhn_valid, redact


@pytest.mark.parametrize("number", ["4111111111111111", "5500000000000004", "340000000000009"])
def test_luhn_accepts_test_cards(number):
    assert luhn_valid(number)


def test_luhn_rejects_order_ids():
    assert not luhn_valid("1234567890123456")


def test_redacts_email_card_and_phone():
    text, count = redact("jane.doe@example.com paid with 4111 1111 1111 1111, call 403-555-0199")
    assert text == "[REDACTED_EMAIL] paid with [REDACTED_CARD], call [REDACTED_PHONE]"
    assert count == 3


def test_keeps_ids_amounts_and_long_non_card_numbers():
    original = "order 1234567890123456 for merchant mer_4f2a9c1b0e77, amount 1650.00 on 2026-09-26"
    assert redact(original) == (original, 0)
