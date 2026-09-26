"""Module 7: train the fraud model and register it in MLflow.

TODO:
- read training features from the dbt marts via Feast (offline store)
- train LightGBM, log params/metrics/artifacts to MLflow
- register as `fraud`; promote @challenger -> @champion only if it beats champion
  on holdout AND passes latency checks
"""
