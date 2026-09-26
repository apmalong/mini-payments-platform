# Build from the ml/ folder: docker build -f serve/Dockerfile -t fraud-scorer:local .
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY serve/app.py .
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
