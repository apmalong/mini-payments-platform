"""Service status collector for the portal: checks every service in services.json on a cadence and
keeps the history in SQLite, so the portal can show uptime bars. Standard library only.

Runs as a background thread of portal/serve.py (so it collects whenever the portal is up), or on its
own: `python portal/status.py --once` prints one round of checks.

Deliberately independent of Prometheus: that runs inside the kind cluster, which is itself one of
the things being monitored, and a status page shouldn't go dark with the system it watches.
"""
import argparse
import json
import socket
import sqlite3
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

PORTAL = Path(__file__).resolve().parent
ROOT = PORTAL.parent
DB_PATH = PORTAL / "data" / "status.db"
INTERVAL_SECONDS = 30
TIMEOUT_SECONDS = 3
RETENTION_DAYS = 7
RANGES = {  # range -> (seconds covered, number of bars)
    "1h": (3600, 60),
    "24h": (86400, 48),
    "7d": (7 * 86400, 84),
}


def load_services() -> list[dict]:
    config = json.loads((PORTAL / "services.json").read_text(encoding="utf-8"))
    return [service for group in config["groups"] for service in group["services"] if service.get("check")]


def local(url_or_host: str) -> str:
    # On Windows "localhost" tries IPv6 first and stalls ~2 s per call when a service only listens
    # on IPv4, which would read as slow or down.
    return url_or_host.replace("localhost", "127.0.0.1")


def check(service: dict) -> tuple[str, bool, float, str]:
    spec = service["check"]
    started = time.perf_counter()
    try:
        if spec["type"] == "http":
            try:
                with urllib.request.urlopen(local(spec["url"]), timeout=TIMEOUT_SECONDS):
                    pass
            except urllib.error.HTTPError as error:  # an HTTP answer means the service is up...
                if error.code >= 500:                # ...unless it reports a server error
                    raise
        elif spec["type"] == "tcp":
            with socket.create_connection((local(spec["host"]), spec["port"]), timeout=TIMEOUT_SECONDS):
                pass
        elif spec["type"] == "file":
            if not (ROOT / spec["path"]).is_file():
                raise FileNotFoundError(spec["path"])
        return service["name"], True, (time.perf_counter() - started) * 1000, ""
    except Exception as error:  # any failure is "down"; keep a readable reason for the tooltip
        return service["name"], False, (time.perf_counter() - started) * 1000, reason(error)


def reason(error: Exception) -> str:
    """A short, human-readable failure reason."""
    if isinstance(error, urllib.error.HTTPError):
        return f"HTTP {error.code} {error.reason}"
    cause = getattr(error, "reason", error)  # URLError wraps the socket error
    if isinstance(cause, ConnectionRefusedError) or "10061" in str(cause):
        return "connection refused (not running)"
    if isinstance(cause, (TimeoutError, socket.timeout)) or "timed out" in str(cause):
        return f"no answer within {TIMEOUT_SECONDS} s"
    if isinstance(error, FileNotFoundError):
        return f"file not found: {error}"
    return f"{type(cause).__name__}: {cause}"[:160]


def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("create table if not exists checks (ts integer not null, service text not null, "
                 "up integer not null, latency_ms real, error text)")
    conn.execute("create index if not exists checks_service_ts on checks (service, ts)")
    return conn


def run_round() -> list[tuple]:
    now = int(time.time())
    with ThreadPoolExecutor(max_workers=16) as pool:
        results = list(pool.map(check, load_services()))
    with connect() as conn:
        conn.executemany("insert into checks values (?, ?, ?, ?, ?)",
                         [(now, name, int(up), round(ms, 1), error) for name, up, ms, error in results])
        conn.execute("delete from checks where ts < ?", (now - RETENTION_DAYS * 86400,))
    return results


def collect_forever() -> None:
    while True:
        started = time.time()
        try:
            run_round()
        except Exception as error:  # never let one bad round kill the collector
            print(f"status collector: {error}", flush=True)
        time.sleep(max(1.0, INTERVAL_SECONDS - (time.time() - started)))


def start_collector() -> threading.Thread:
    thread = threading.Thread(target=collect_forever, name="status-collector", daemon=True)
    thread.start()
    return thread


def current() -> dict:
    """Latest check per service."""
    with connect() as conn:
        rows = conn.execute("""
            select service, ts, up, latency_ms, error from checks c
            where ts = (select max(ts) from checks where service = c.service)""").fetchall()
    return {"interval_seconds": INTERVAL_SECONDS,
            "services": {s: {"ts": ts, "up": bool(up), "latency_ms": ms, "error": err} for s, ts, up, ms, err in rows}}


def history(range_key: str) -> dict:
    """Per service: one bucket per bar with the share of checks that were up (None = no data)."""
    seconds, bars = RANGES.get(range_key, RANGES["24h"])
    bucket = seconds // bars
    end = int(time.time())
    start = end - seconds
    with connect() as conn:
        rows = conn.execute("""
            select service, (ts - ?) / ? as bucket, count(*), sum(up), max(case when up = 0 then error end)
            from checks where ts >= ? group by service, bucket""", (start, bucket, start)).fetchall()
    services: dict[str, dict] = {}
    for service, index, total, up, error in rows:
        entry = services.setdefault(service, {"buckets": [None] * bars, "checks": 0, "up": 0})
        if 0 <= index < bars:
            entry["buckets"][index] = {"checks": total, "up": up, "error": error}
            entry["checks"] += total
            entry["up"] += up
    for entry in services.values():
        entry["uptime"] = entry["up"] / entry["checks"] if entry["checks"] else None
    return {"range": range_key, "start": start, "end": end, "bucket_seconds": bucket, "bars": bars,
            "services": services}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--once", action="store_true", help="run one round, print it, and exit")
    args = parser.parse_args()
    if args.once:
        for name, up, ms, error in run_round():
            print(f"{'up  ' if up else 'DOWN'} {ms:7.0f} ms  {name}  {error}")
    else:
        collect_forever()
