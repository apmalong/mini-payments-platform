# Build from the project root: docker build -f ml/serve/Dockerfile -t fraud-scorer:local .
FROM python:3.12-slim

# LightGBM needs OpenMP at runtime.
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY ml/serve/requirements.txt ml/serve/requirements.txt
RUN pip install --no-cache-dir -r ml/serve/requirements.txt

# Same relative layout as the repo: features.py reads warehouse/dbt/seeds/fx_rates.csv.
COPY warehouse/dbt/seeds/fx_rates.csv warehouse/dbt/seeds/fx_rates.csv
COPY ml/features.py ml/features.py
COPY ml/serve/app.py ml/serve/app.py

RUN useradd --uid 10001 --create-home scorer
USER 10001
WORKDIR /app/ml/serve
EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
