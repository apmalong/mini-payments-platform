# Runbook: model drift (FeatureDrift, ReviewRateHigh)

**What it means:** live transactions no longer look like the data the fraud model was trained on
(`FeatureDrift`: a feature's PSI over the last hour of streamed events is above 0.25), or the model
is sending far more transactions to review than the ~1% it was tuned for (`ReviewRateHigh`). Either
way the model's decisions are less trustworthy than its validation metrics suggest.

These are **ticket** alerts: they need investigation, not a 3 a.m. page.

## Diagnose

1. Which feature? The "Feature drift" panel lists PSI per feature.
2. **Is it the traffic or the pipeline?**
   - Pipeline (skew): if the online feature differs from its offline twin for the same
     transactions, it's a bug, not drift. Run `cd ml; uv run python check_skew.py --since <time>`.
     A cold online store is the classic cause: `cd streaming; uv run python bootstrap_online_store.py`.
   - Traffic (real drift): offline and online agree, but both differ from the training period.
3. **Is fraud actually up?** A review-rate jump with a real fraud attack is the model doing its
   job. Compare with chargebacks arriving over the next days
   (`analytics.fct_transactions.has_fraud_chargeback`).

## Known case in this project

The generator's continuous mode uses each card every ~10 minutes, while the 90-day backfill uses
it every few days, so `seconds_since_last_card_txn` (PSI > 4) and `card_txn_count_5m` drift heavily
between training and live traffic. That is genuine drift introduced by the simulator, and it's
why the live review rate runs far above 1%.

## Act

- Real, lasting change: retrain on recent data once labels mature (`fraud_model_training`
  DAG, or `cd ml; uv run python train.py`). The promotion gate keeps a worse model out.
- Temporary (a campaign, a holiday): raise the review threshold only with fraud-ops sign-off, and
  record the decision.
- Never retrain on immature labels to "fix" drift quickly: recent transactions look legitimate
  only because their chargebacks haven't arrived yet.
