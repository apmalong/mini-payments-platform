"""Model features: one definition shared by training (ml/train.py) and serving (ml/serve/app.py).

The windowed features (card_*, is_new_ip_country, merchant_amount_zscore) are computed online by
streaming/consumer.py and offline by the dbt model ml_transaction_features; ml/check_skew.py
compares the two. Everything else is derived here from raw transaction fields, by this same code
on both sides, so it can't drift.
"""
import csv
import math
from pathlib import Path

FX_RATES = {
    row["currency"]: float(row["rate_to_cad"])
    for row in csv.DictReader(
        (Path(__file__).resolve().parent.parent / "warehouse" / "dbt" / "seeds" / "fx_rates.csv").open())
}

FEATURES = [
    "amount_cad",
    "network_risk_score",
    "is_card_not_present",
    "avs_mismatch",
    "cvv_mismatch",
    "three_ds_authenticated",
    "card_txn_count_5m",
    "card_amount_sum_1h",
    "seconds_since_last_card_txn",
    "is_new_ip_country",
    "merchant_amount_zscore",
]

WINDOWED = ["card_txn_count_5m", "card_amount_sum_1h", "seconds_since_last_card_txn",
            "is_new_ip_country", "merchant_amount_zscore"]


def _number(value) -> float:
    """Missing -> NaN (LightGBM treats NaN as missing); booleans -> 0/1."""
    if value is None or value == "":
        return math.nan
    if isinstance(value, str) and value.lower() in ("true", "false"):
        return float(value.lower() == "true")
    return float(value)


def flatten_event(event: dict) -> dict:
    """A streamed transaction event (raw_payload nested) as the flat row shape dbt produces."""
    payload = event.get("raw_payload") or {}
    return {
        "amount": event["amount"],
        "currency": event["currency"],
        "entry_mode": event["entry_mode"],
        "avs_result": payload.get("avs_result"),
        "cvv_result": payload.get("cvv_result"),
        "three_ds_authenticated": (payload.get("three_ds") or {}).get("authenticated"),
        "network_risk_score": payload.get("network_risk_score"),
    }


def build(row: dict) -> list[float]:
    """Feature vector, in FEATURES order, from a flat transaction row plus its windowed features."""
    derived = {
        "amount_cad": round(float(row["amount"]) * FX_RATES[row["currency"]], 2),
        "network_risk_score": _number(row.get("network_risk_score")),
        "is_card_not_present": float(row["entry_mode"] == "ecommerce"),
        "avs_mismatch": float(row.get("avs_result") in ("N", "Z")),
        "cvv_mismatch": float(row.get("cvv_result") == "N"),
        "three_ds_authenticated": _number(row.get("three_ds_authenticated")),
    }
    derived.update({name: _number(row.get(name)) for name in WINDOWED})
    return [derived[name] for name in FEATURES]
