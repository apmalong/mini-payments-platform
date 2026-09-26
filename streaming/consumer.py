"""Module 6: consume payments.transactions and compute rolling fraud features.

TODO:
- windowed features: txn count per card (5 min), amount z-score vs merchant baseline, new-country flag
- write features to Redis (online store) and a sink table (offline store)
- call the scoring service (ml/serve) per transaction
- handle duplicates and late events (dedupe key, watermark)
"""
