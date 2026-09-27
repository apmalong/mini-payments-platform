"""Demo pre-flight: is everything the 10-minute demo touches up? Standard library only.

    python demo/preflight.py

Prints GO / NO-GO per item with the command that fixes each NO-GO. Run it 10 minutes before the
demo, start what's missing, and run it again.
"""
import json
import subprocess
import sys
import urllib.parse
import urllib.request

ROOT_CMD = r"cd C:\projects\test\mini-payments-platform"

HTTP = [
    ("Portal", "http://127.0.0.1:8099/", "uv run python portal/serve.py"),
    ("Airflow", "http://127.0.0.1:8088/api/v2/monitor/health", "docker start mpp-airflow"),
    ("Redpanda Console", "http://127.0.0.1:8085/", "docker start mpp-redpanda mpp-redpanda-console"),
    ("RedisInsight", "http://127.0.0.1:5540/", "docker start mpp-redis mpp-redisinsight"),
    ("MLflow", "http://127.0.0.1:5000/health", "docker start mpp-mlflow"),
    ("Fraud scorer (k8s)", "http://127.0.0.1:8091/readyz", "docker start payments-control-plane payments-worker payments-worker2"),
    ("Prometheus", "http://127.0.0.1:9090/-/ready", "docker start mpp-prometheus-port"),
    ("Grafana", "http://127.0.0.1:3000/api/health", "docker start mpp-grafana"),
    ("LiteLLM gateway", "http://127.0.0.1:4000/health/liveliness", "docker start mpp-litellm"),
    ("Freshness exporter", "http://127.0.0.1:8001/metrics", "cd observability; uv run python exporter.py"),
    ("Feature consumer", "http://127.0.0.1:8000/metrics",
     "cd streaming; uv run python consumer.py --score-url http://127.0.0.1:8091/score"),
]


def ok(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=4) as response:
            return response.status < 500
    except OSError:
        return False


def check(label: str, passed: bool, fix: str = "", detail: str = "") -> bool:
    print(f"  {'GO   ' if passed else 'NO-GO'}  {label:<34}{detail}")
    if not passed and fix:
        print(f"         fix: {fix}")
    return passed


def prometheus(query: str) -> list:
    url = "http://127.0.0.1:9090/api/v1/query?" + urllib.parse.urlencode({"query": query})
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)["data"]["result"]


def main() -> None:
    print(f"Run fixes from the project folder ({ROOT_CMD}).\n\nServices")
    results = [check(name, ok(url), fix) for name, url, fix in HTTP]

    print("\nState")
    try:
        paused = subprocess.run(["docker", "exec", "mpp-airflow", "airflow", "dags", "list", "-o", "json"],
                                capture_output=True, text=True, timeout=60).stdout
        dags = {d["dag_id"]: d for d in json.loads(paused[paused.index("["):])}
        unpaused = str(dags.get("payments_pipeline", {}).get("is_paused", "True")).lower() == "false"
    except (OSError, ValueError, subprocess.TimeoutExpired):
        unpaused = False
    results.append(check("payments_pipeline unpaused", unpaused,
                         "docker exec mpp-airflow airflow dags unpause payments_pipeline"))
    try:
        ages = {r["metric"]["layer"]: float(r["value"][1]) for r in prometheus("payments_freshness_seconds")}
        results.append(check("Source receiving transactions", ages.get("source", 1e9) < 120,
                             "cd generator; uv run python generate.py --continuous --stream",
                             f"source {ages.get('source', 0) / 60:.1f} min old"))
        results.append(check("Mart fresh (SLO 15 min)", ages.get("mart", 1e9) < 900,
                             "trigger: docker exec mpp-airflow airflow dags trigger payments_pipeline",
                             f"mart {ages.get('mart', 0) / 60:.1f} min old"))
        pods = prometheus('sum(up{app_kubernetes_io_name="fraud-scorer"})')
        results.append(check("Scorer pods up", bool(pods) and float(pods[0]["value"][1]) >= 2,
                             "kubectl -n fraud get pods", f"{pods[0]['value'][1] if pods else 0} pods"))
    except (OSError, KeyError, ValueError):
        results.append(check("Prometheus queries", False, "start Prometheus first"))
    try:
        keys = json.load(open("gateway/.keys.json"))
        results.append(check("Gateway test key", "local-test" in keys, "uv run python gateway/setup_keys.py"))
    except OSError:
        results.append(check("Gateway test key", False, "uv run python gateway/setup_keys.py"))

    ready = all(results)
    print(f"\n{'READY: all go.' if ready else 'NOT READY: fix the NO-GO items above, then run this again.'}")
    sys.exit(0 if ready else 1)


if __name__ == "__main__":
    main()
