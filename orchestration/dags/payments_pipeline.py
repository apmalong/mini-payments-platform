"""Module 4: ingest -> dbt build -> publish.

TODO:
- ingest task calling ingestion/sync.py
- dbt models as individual tasks via astronomer-cosmos DbtTaskGroup
- emit Dataset("duckdb://marts/fct_transactions") so the ML training DAG triggers on it
- retries with exponential backoff, on_failure_callback to a local webhook stub
"""
