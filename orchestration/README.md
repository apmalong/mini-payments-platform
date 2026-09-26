# Orchestration (Module 4)

Airflow runs from the official compose file, kept separate from the root compose:

```bash
cd orchestration
curl -LfO https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml
mkdir -p logs plugins config
docker compose up airflow-init && docker compose up -d
```

Mount `../warehouse/dbt` into the containers and add `astronomer-cosmos` + `dbt-duckdb`
to `_PIP_ADDITIONAL_REQUIREMENTS`.
