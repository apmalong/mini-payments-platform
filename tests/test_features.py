import math

import features


def event(**payload):
    return {"amount": "100.00", "currency": "USD", "entry_mode": "ecommerce", "raw_payload": payload}


def test_flatten_event_reads_nested_payload():
    row = features.flatten_event(event(avs_result="N", three_ds={"authenticated": True}, network_risk_score=70))
    assert row["avs_result"] == "N" and row["three_ds_authenticated"] is True and row["network_risk_score"] == 70


def test_build_orders_features_and_converts_currency():
    row = features.flatten_event(event(avs_result="Z", cvv_result="N"))
    row.update(card_txn_count_5m=3, card_amount_sum_1h=250.0, seconds_since_last_card_txn=None,
               is_new_ip_country=True, merchant_amount_zscore=1.5)
    vector = dict(zip(features.FEATURES, features.build(row), strict=True))
    assert vector["amount_cad"] == 137.0            # 100 USD at the seed rate 1.37
    assert vector["avs_mismatch"] == 1.0 and vector["cvv_mismatch"] == 1.0
    assert vector["is_card_not_present"] == 1.0
    assert math.isnan(vector["seconds_since_last_card_txn"])   # missing -> NaN for LightGBM
    assert vector["is_new_ip_country"] == 1.0
    assert len(vector) == len(features.FEATURES)


def test_card_present_has_no_ecommerce_signals():
    row = features.flatten_event({"amount": "5", "currency": "CAD", "entry_mode": "chip", "raw_payload": {}})
    vector = dict(zip(features.FEATURES, features.build(row), strict=True))
    assert vector["is_card_not_present"] == 0.0 and vector["avs_mismatch"] == 0.0
    assert math.isnan(vector["three_ds_authenticated"])
