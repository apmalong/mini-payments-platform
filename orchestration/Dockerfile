FROM apache/airflow:3.3.2-python3.12

# Docker CLI only (no daemon): PyAirbyte launches the source-postgres connector as a sibling
# container through the host's Docker socket.
USER root
COPY --from=docker:27-cli /usr/local/bin/docker /usr/local/bin/docker
USER airflow

# Cosmos renders the dbt project as Airflow tasks. Pin Airflow so pip doesn't move it.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "astronomer-cosmos==1.15.1"

# dbt and PyAirbyte get their own virtualenvs to keep their dependencies away from Airflow's.
# duckdb is pinned to the host version: the warehouse file is shared with the host.
RUN python -m venv /opt/airflow/venvs/dbt \
 && /opt/airflow/venvs/dbt/bin/pip install --no-cache-dir "dbt-core==1.12.5" "dbt-duckdb==1.11.0" "duckdb==1.4.3" \
      "sqlglot==30.19.0" pyyaml
RUN python -m venv /opt/airflow/venvs/ingest \
 && /opt/airflow/venvs/ingest/bin/pip install --no-cache-dir "airbyte==0.71.0" "duckdb==1.4.3"
