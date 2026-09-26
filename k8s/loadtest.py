"""Module 8: load test for the fraud scorer. Standard library only.

    python loadtest.py --duration 120 --concurrency 24
    python loadtest.py --duration 90 --concurrency 8      # while running `helm upgrade`

Every request is checked, so the summary shows exactly how many failed, if any, and why.
Use 127.0.0.1, not localhost: on Windows localhost tries IPv6 first and stalls ~2s per request.
"""
import argparse
import collections
import http.client
import json
import random
import threading
import time
import urllib.parse

EVENTS = [
    # ordinary card-present purchase
    {"amount": "42.50", "currency": "CAD", "entry_mode": "chip", "raw_payload": {"network_risk_score": 18},
     "features": {"card_txn_count_5m": 1, "card_amount_sum_1h": 42.5, "seconds_since_last_card_txn": 86000,
                  "is_new_ip_country": False, "merchant_amount_zscore": 0.1}},
    # end of a card-testing burst
    {"amount": "1650.00", "currency": "USD", "entry_mode": "ecommerce",
     "raw_payload": {"avs_result": "N", "cvv_result": "N", "three_ds": {"authenticated": False}, "network_risk_score": 71},
     "features": {"card_txn_count_5m": 9, "card_amount_sum_1h": 1672.4, "seconds_since_last_card_txn": 45,
                  "is_new_ip_country": True, "merchant_amount_zscore": 6.2}},
]


def worker(url: str, stop_at: float, results: list, lock: threading.Lock, stale: collections.Counter) -> None:
    """One keep-alive connection per thread. A new connection per request would exhaust Windows'
    ephemeral ports (16k, each held ~2 min in TIME_WAIT) at around 200 req/s."""
    target = urllib.parse.urlsplit(url)
    conn = http.client.HTTPConnection(target.hostname, target.port, timeout=5)
    while time.time() < stop_at:
        body = json.dumps({**random.choice(EVENTS), "transaction_id": f"load-{random.getrandbits(40):x}",
                           "card_token": "tok_load"}).encode()
        started = time.perf_counter()
        for attempt in (1, 2):
            try:
                conn.request("POST", target.path, body, {"Content-Type": "application/json"})
                response = conn.getresponse()
                payload = response.read()
                outcome = ("ok", json.loads(payload)["model_version"]) if response.status == 200 \
                    else ("error", f"HTTP {response.status}")
                break
            # ConnectionAbortedError is how Windows reports writing to a connection the peer closed.
            # Re-sending is safe here as well because scoring is read-only.
            except (http.client.RemoteDisconnected, ConnectionResetError, ConnectionAbortedError,
                    BrokenPipeError) as error:
                # The server closed this keep-alive connection before reading the request (a pod
                # shutting down closes its idle connections). Reconnect and resend once, as HTTP
                # clients do for stale connections; counted separately so it stays visible.
                conn.close()
                conn = http.client.HTTPConnection(target.hostname, target.port, timeout=5)
                if attempt == 2:
                    outcome = ("error", type(error).__name__)
                else:
                    with lock:
                        stale["reconnects"] += 1
            except Exception as error:  # refused, timeout
                conn.close()
                conn = http.client.HTTPConnection(target.hostname, target.port, timeout=5)
                outcome = ("error", type(error).__name__)
                break
        with lock:
            results.append((time.time(), (time.perf_counter() - started) * 1000, *outcome))
    conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--url", default="http://127.0.0.1:8091/score")
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--concurrency", type=int, default=8)
    args = parser.parse_args()

    results, lock, stale = [], threading.Lock(), collections.Counter()
    stop_at = time.time() + args.duration
    threads = [threading.Thread(target=worker, args=(args.url, stop_at, results, lock, stale), daemon=True)
               for _ in range(args.concurrency)]
    for thread in threads:
        thread.start()
    started = time.time()
    while time.time() < stop_at:
        time.sleep(10)
        with lock:
            recent = [r for r in results if r[0] > time.time() - 10]
        failed = sum(1 for r in recent if r[2] == "error")
        print(f"{time.time() - started:5.0f}s  {len(recent) / 10:6.1f} req/s  {failed} failed in last 10s", flush=True)
    for thread in threads:
        thread.join()

    ok = [r for r in results if r[2] == "ok"]
    errors = collections.Counter(r[3] for r in results if r[2] == "error")
    latencies = sorted(r[1] for r in ok) or [0]
    versions = collections.Counter(r[3] for r in ok)
    print(f"\n{len(results)} requests, {len(ok)} ok, {sum(errors.values())} failed "
          f"({100 * sum(errors.values()) / max(1, len(results)):.2f}%)")
    print(f"latency p50 {latencies[len(latencies) // 2]:.0f} ms, p99 {latencies[int(len(latencies) * 0.99)]:.0f} ms")
    print(f"served by model versions: {dict(versions)}")
    print(f"stale keep-alive connections re-sent: {stale['reconnects']}")
    if errors:
        print(f"errors: {dict(errors)}")


if __name__ == "__main__":
    main()
