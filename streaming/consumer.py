"""Module 6: consume payments.transactions and maintain real-time fraud features.

For each transaction event:
- drop duplicates (client retries publish the same transaction_id twice)
- update per-card and per-merchant state in Redis (the online feature store) and read back the
  features as of this event, in one atomic Lua script
- write the event and its features to Parquet (the offline store, for training in Module 7)

Features (all as of the event, including it):
    card_txn_count_5m          transactions on this card in the last 5 minutes (event time)
    card_amount_sum_1h         total amount on this card in the last hour
    seconds_since_last_card_txn
    is_new_ip_country          card has history and this IP country hasn't been seen on it
    merchant_amount_zscore     amount vs this merchant's running mean/stddev (before this event)

Delivery: at-least-once from Kafka (offsets are committed only after the Parquet flush), made
effectively-once for Redis by the dedupe key inside the same Lua script as the updates.

Late events: an event more than --allowed-lateness behind the newest event time seen is written
offline with is_late=true but doesn't touch the online state, which serves real-time decisions
and is rebuilt offline anyway.

    python consumer.py                 # run until Ctrl+C
    python consumer.py --from-start    # new consumer group, replay the topic from the beginning
"""
import argparse
import json
import os
import signal
import time
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import redis
from confluent_kafka import Consumer, KafkaError
from prometheus_client import Counter, Gauge, Histogram, start_http_server

TOPIC = "payments.transactions"
OFFLINE_DIR = Path(__file__).resolve().parent / "offline" / "transaction_features"
# Card and merchant state must outlive the gaps between a card's transactions, or online features
# (time since last transaction, new IP country, merchant stats) drift from the offline ones,
# which see the full history. 90 days matches the warehouse history.
STATE_TTL_SECONDS = 90 * 24 * 3600
DEDUPE_TTL_SECONDS = 24 * 3600

EVENTS = Counter("stream_events_total", "Events consumed", ["outcome"])  # processed | duplicate | late | invalid
REQUIRED_FIELDS = ("transaction_id", "card_token", "merchant_id", "amount", "currency", "entry_mode", "created_at")
LAG = Gauge("stream_consumer_lag", "Messages behind the end of the topic", ["partition"])
SCORED = Counter("stream_scored_total", "Events scored", ["decision"])  # approve | review | error
LATENCY = Histogram("stream_event_latency_seconds", "Event time to feature availability",
                    buckets=(0.1, 0.5, 1, 2, 5, 10, 30, 60, 300))
WATERMARK = Gauge("stream_watermark_seconds", "Newest event time seen (unix seconds)")

# KEYS: dedupe, card window zset, card ip-country set, merchant stats hash, card features hash
# ARGV: txn_id, event_ts, amount, ip_country ('' if none), dedupe_ttl, state_ttl
UPDATE_FEATURES = """
if redis.call('SET', KEYS[1], 1, 'NX', 'EX', ARGV[5]) == false then
    return {0}
end
local ts, amount, ip = tonumber(ARGV[2]), tonumber(ARGV[3]), ARGV[4]
local ttl = tonumber(ARGV[6])

local last = redis.call('HGET', KEYS[5], 'last_txn_ts')
local since_last = -1
if last then since_last = ts - tonumber(last) end

redis.call('ZADD', KEYS[2], ts, ARGV[1] .. ':' .. ARGV[3])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', ts - 3600)
local count_5m = redis.call('ZCOUNT', KEYS[2], ts - 300, ts)
local sum_1h = 0
for _, member in ipairs(redis.call('ZRANGEBYSCORE', KEYS[2], ts - 3600, ts)) do
    sum_1h = sum_1h + tonumber(string.match(member, ':([^:]+)$'))
end

local new_country = 0
if ip ~= '' then
    if redis.call('SCARD', KEYS[3]) > 0 and redis.call('SISMEMBER', KEYS[3], ip) == 0 then
        new_country = 1
    end
    redis.call('SADD', KEYS[3], ip)
end

-- Welford running mean/variance per merchant; z-score uses the stats before this event.
local n = tonumber(redis.call('HGET', KEYS[4], 'n') or 0)
local mean = tonumber(redis.call('HGET', KEYS[4], 'mean') or 0)
local m2 = tonumber(redis.call('HGET', KEYS[4], 'm2') or 0)
local zscore = 0
if n >= 30 and m2 > 0 then zscore = (amount - mean) / math.sqrt(m2 / (n - 1)) end
n = n + 1
local delta = amount - mean
mean = mean + delta / n
m2 = m2 + delta * (amount - mean)
redis.call('HSET', KEYS[4], 'n', n, 'mean', mean, 'm2', m2)

if last == false or ts > tonumber(last) then
    redis.call('HSET', KEYS[5], 'last_txn_ts', ts)
end
redis.call('HSET', KEYS[5], 'txn_count_5m', count_5m, 'amount_sum_1h', sum_1h,
           'seconds_since_last_txn', since_last, 'is_new_ip_country', new_country,
           'merchant_amount_zscore', zscore, 'updated_at', ts)
redis.call('EXPIRE', KEYS[2], 3600)  -- the window only needs the last hour
for i = 3, 5 do redis.call('EXPIRE', KEYS[i], ttl) end

return {1, count_5m, tostring(sum_1h), tostring(since_last), new_country, tostring(zscore)}
"""


def load_env() -> None:
    path = Path(__file__).resolve().parent.parent / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())


def parse_time(value: str) -> float:
    return datetime.fromisoformat(value).timestamp()


class FeatureStream:
    def __init__(self, r: redis.Redis, allowed_lateness: float, score_url: str | None = None):
        self.update = r.register_script(UPDATE_FEATURES)
        self.allowed_lateness = allowed_lateness
        self.score_url = score_url
        self.score_errors = 0
        self.watermark = 0.0
        self.buffer: list[dict] = []

    def process(self, raw: bytes) -> None:
        # A malformed event is counted and skipped, not allowed to stop the stream.
        # (A production consumer would also route it to a dead-letter topic.)
        try:
            event = json.loads(raw)
            missing = [f for f in REQUIRED_FIELDS if event.get(f) in (None, "")]
            if missing:
                raise ValueError(f"missing {missing}")
            event_ts = parse_time(event["created_at"])
            float(event["amount"])
        except (ValueError, TypeError, AttributeError) as error:
            EVENTS.labels("invalid").inc()
            print(f"skipping invalid event: {error}: {raw[:120]!r}", flush=True)
            return
        is_late = event_ts < self.watermark - self.allowed_lateness
        self.watermark = max(self.watermark, event_ts)
        WATERMARK.set(self.watermark)

        row = {
            "transaction_id": event["transaction_id"],
            "card_token": event["card_token"],
            "merchant_id": event["merchant_id"],
            "event_time": datetime.fromtimestamp(event_ts, UTC),
            "amount": float(event["amount"]),
            "currency": event["currency"],
            "entry_mode": event["entry_mode"],
            "is_late": is_late,
            "processed_at": datetime.now(UTC),
        }
        if is_late:
            EVENTS.labels("late").inc()
        else:
            card, merchant = event["card_token"], event["merchant_id"]
            result = self.update(
                keys=[f"seen:{event['transaction_id']}", f"card:{card}:window", f"card:{card}:ip_countries",
                      f"merchant:{merchant}:amount_stats", f"features:card:{card}"],
                args=[event["transaction_id"], event_ts, row["amount"], event.get("ip_country") or "",
                      DEDUPE_TTL_SECONDS, STATE_TTL_SECONDS])
            if result[0] == 0:
                EVENTS.labels("duplicate").inc()
                return
            row.update({
                "card_txn_count_5m": int(result[1]),
                "card_amount_sum_1h": float(result[2]),
                "seconds_since_last_card_txn": None if float(result[3]) < 0 else float(result[3]),
                "is_new_ip_country": bool(result[4]),
                "merchant_amount_zscore": float(result[5]),
            })
            EVENTS.labels("processed").inc()
            LATENCY.observe(max(0.0, time.time() - event_ts))
            if self.score_url:
                row.update(self.score(event, row))
        self.buffer.append(row)

    def score(self, event: dict, row: dict) -> dict:
        """Ask the scoring service, passing the features just computed (no Redis race).
        Fails open: a scoring outage must not stop feature computation."""
        body = {**event, "features": {name: row[name] for name in WINDOWED_FEATURES}}
        request = urllib.request.Request(self.score_url, data=json.dumps(body, default=str).encode(),
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=2) as response:
                result = json.loads(response.read())
            SCORED.labels(result["decision"]).inc()
            return {"fraud_score": result["fraud_score"], "model_version": str(result["model_version"])}
        except (OSError, ValueError, KeyError) as error:
            SCORED.labels("error").inc()
            self.score_errors += 1
            if self.score_errors <= 3:  # don't flood the log during an outage
                print(f"scoring failed (continuing without a score): {error}", flush=True)
            return {}

    def flush(self) -> int:
        if not self.buffer:
            return 0
        table = pa.Table.from_pylist(self.buffer, schema=OFFLINE_SCHEMA)
        day = self.buffer[0]["processed_at"].strftime("%Y-%m-%d")
        path = OFFLINE_DIR / f"date={day}" / f"part-{int(time.time() * 1000)}-{uuid.uuid4().hex[:6]}.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, path)
        count = len(self.buffer)
        self.buffer = []
        return count


OFFLINE_SCHEMA = pa.schema([
    ("transaction_id", pa.string()), ("card_token", pa.string()), ("merchant_id", pa.string()),
    ("event_time", pa.timestamp("us", tz="UTC")), ("amount", pa.float64()), ("currency", pa.string()),
    ("entry_mode", pa.string()), ("is_late", pa.bool_()), ("processed_at", pa.timestamp("us", tz="UTC")),
    ("card_txn_count_5m", pa.int64()), ("card_amount_sum_1h", pa.float64()),
    ("seconds_since_last_card_txn", pa.float64()), ("is_new_ip_country", pa.bool_()),
    ("merchant_amount_zscore", pa.float64()),
    ("fraud_score", pa.float64()), ("model_version", pa.string()),
])
WINDOWED_FEATURES = ["card_txn_count_5m", "card_amount_sum_1h", "seconds_since_last_card_txn",
                     "is_new_ip_country", "merchant_amount_zscore"]


def update_lag(consumer: Consumer) -> None:
    for tp in consumer.assignment():
        low, high = consumer.get_watermark_offsets(tp, timeout=5, cached=False)
        position = consumer.position([tp])[0].offset
        LAG.labels(str(tp.partition)).set(max(0, high - (position if position >= 0 else low)))


def main() -> None:
    parser = argparse.ArgumentParser(description="Real-time fraud features (Module 6)")
    parser.add_argument("--group", default="fraud-features")
    parser.add_argument("--from-start", action="store_true", help="fresh consumer group reading from offset 0")
    parser.add_argument("--allowed-lateness", type=float, default=600, help="seconds")
    parser.add_argument("--flush-every", type=float, default=10, help="seconds between Parquet flushes/commits")
    parser.add_argument("--metrics-port", type=int, default=8000)
    parser.add_argument("--score-url", help="fraud scoring service, e.g. http://127.0.0.1:8091/score, the scorer on Kubernetes (not localhost: on Windows that tries IPv6 first and stalls ~2s per request)")
    args = parser.parse_args()

    load_env()
    r = redis.Redis(host=os.environ.get("REDIS_HOST", "localhost"), port=6379)
    r.ping()
    group = f"{args.group}-{int(time.time())}" if args.from_start else args.group
    consumer = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"),
        "group.id": group,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # commit only after the offline flush
    })
    consumer.subscribe([TOPIC])
    start_http_server(args.metrics_port)
    stream = FeatureStream(r, args.allowed_lateness, args.score_url)

    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    print(f"Consuming {TOPIC} as group {group}; metrics on :{args.metrics_port}/metrics (Ctrl+C to stop)")

    last_flush = time.time()
    uncommitted = False
    while not stopping:
        message = consumer.poll(1.0)
        if message is not None:
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    print(f"Kafka error: {message.error()}")
            else:
                stream.process(message.value())
                uncommitted = True
        if time.time() - last_flush >= args.flush_every:
            written = stream.flush()
            if uncommitted:  # Kafka rejects a commit with nothing new consumed
                consumer.commit(asynchronous=False)
                uncommitted = False
            update_lag(consumer)
            counts = {s.labels["outcome"]: int(s.value) for m in EVENTS.collect() for s in m.samples
                      if s.name.endswith("_total")}
            print(f"{datetime.now():%H:%M:%S} flushed {written} rows; totals {counts}", flush=True)
            last_flush = time.time()

    stream.flush()
    if uncommitted:
        consumer.commit(asynchronous=False)
    consumer.close()
    print("Stopped cleanly; offsets committed.")


if __name__ == "__main__":
    main()
