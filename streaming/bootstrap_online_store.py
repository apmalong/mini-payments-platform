"""Module 7: load the online feature store (Redis) from the warehouse.

A consumer that starts from an empty Redis knows only the events it has streamed, while the
offline features the model was trained on see the full history. That gap is training/serving
skew (ml/check_skew.py measured it). This script writes the state the consumer's Lua script
keeps, computed from analytics.fct_transactions:

    merchant:<id>:amount_stats   n, mean, m2 (Welford) over all transactions
    features:card:<token>        last_txn_ts
    card:<token>:ip_countries    every IP country seen on the card
    card:<token>:window          transactions in the last hour (for the 5m/1h windows)

Run it before starting the consumer, with the consumer positioned after the last synced event,
so no event is counted twice.

    python bootstrap_online_store.py            # add to what's there
    python bootstrap_online_store.py --reset    # FLUSHDB first (Redis is dedicated to features)
"""
import argparse
import os
import time
from pathlib import Path

import duckdb
import redis

from consumer import STATE_TTL_SECONDS

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--reset", action="store_true", help="empty Redis first")
    args = parser.parse_args()

    started = time.time()
    warehouse = duckdb.connect(os.environ.get("DUCKDB_PATH", str(ROOT / "warehouse.duckdb")), read_only=True)
    store = redis.Redis(host=os.environ.get("REDIS_HOST", "localhost"), port=6379)
    if args.reset:
        store.flushdb()

    merchants = warehouse.execute("""
        select merchant_id, count(*), avg(amount), coalesce(var_samp(amount), 0) * (count(*) - 1)
        from analytics.fct_transactions group by 1
    """).fetchall()
    cards = warehouse.execute("""
        select card_token, max(epoch(created_at)), list(distinct ip_country) filter (where ip_country is not null)
        from analytics.fct_transactions group by 1
    """).fetchall()
    recent = warehouse.execute("""
        select card_token, transaction_id, epoch(created_at), amount
        from analytics.fct_transactions
        where created_at >= (select max(created_at) from analytics.fct_transactions) - interval 1 hour
    """).fetchall()
    warehouse.close()

    pipe = store.pipeline(transaction=False)
    for merchant_id, n, mean, m2 in merchants:
        key = f"merchant:{merchant_id}:amount_stats"
        pipe.hset(key, mapping={"n": n, "mean": float(mean), "m2": float(m2)})
        pipe.expire(key, STATE_TTL_SECONDS)
    for card_token, last_ts, countries in cards:
        pipe.hset(f"features:card:{card_token}", "last_txn_ts", last_ts)
        pipe.expire(f"features:card:{card_token}", STATE_TTL_SECONDS)
        if countries:
            pipe.sadd(f"card:{card_token}:ip_countries", *countries)
            pipe.expire(f"card:{card_token}:ip_countries", STATE_TTL_SECONDS)
    for card_token, transaction_id, ts, amount in recent:
        pipe.zadd(f"card:{card_token}:window", {f"{transaction_id}:{float(amount)}": ts})
        pipe.expire(f"card:{card_token}:window", 3600)
    pipe.execute()

    print(f"loaded {len(merchants)} merchants, {len(cards)} cards, {len(recent)} recent transactions "
          f"in {time.time() - started:.1f}s")


if __name__ == "__main__":
    main()
