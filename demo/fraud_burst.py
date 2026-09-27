"""Demo: publish a card-testing attack now and watch the fraud score climb as it builds.

    cd generator; uv run python ../demo/fraud_burst.py

Needs the feature consumer running with --score-url (demo/preflight.py checks it). One card gets
eight tiny online charges a few seconds apart, then a large one: the pattern the model learned.
The script writes them to Postgres and Redpanda like the generator does, then polls the consumer's
Parquet output and prints each transaction's features and score as it lands.
"""
import sys
import time
from datetime import timedelta
from pathlib import Path

import duckdb
import psycopg

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "generator"))
import generate  # noqa: E402

PARQUET = (ROOT / "streaming" / "offline" / "transaction_features" / "*" / "*.parquet").as_posix()


def main() -> None:
    sys.stdout.reconfigure(line_buffering=True)
    generate.load_env()
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    with psycopg.connect(generate.dsn()) as conn:
        gen = generate.Generator(conn, producer, seed=int(time.time()))
        gen.load_reference_data()
        now = gen.db_now()
        card = gen.cards[0 if not gen.cards else int(time.time()) % len(gen.cards)]
        merchant = next(m for m in gen.merchants if m["risky"])
        # Timestamps end at "now" so the events are on time for the consumer (not late).
        txns = [gen.make_transaction(now - timedelta(seconds=10 * (8 - i)), now, card, merchant, fraud=True,
                                     amount=round(1 + i * 0.37, 2)) for i in range(8)]
        txns.append(gen.make_transaction(now, now, card, merchant, fraud=True, amount=gen._amount(merchant, scale=4)))
        for txn in txns:
            txn["updated_at"] = now
        gen.write(txns, [], [])
    ids = [t["transaction_id"] for t in txns]
    print(f"Published a card-testing burst: card {card['card_token'][:14]}..., merchant {merchant['name']} "
          f"({merchant['category']}), {len(txns)} transactions\n")
    print(f"{'#':>2}  {'amount':>9}  {'txns on card, 5 min':>19}  {'new IP country':>14}  {'score':>6}  decision")

    shown, deadline = set(), time.time() + 45
    while len(shown) < len(ids) and time.time() < deadline:
        time.sleep(3)
        conn = duckdb.connect()
        try:
            rows = conn.execute(f"""
                select transaction_id, amount, card_txn_count_5m, is_new_ip_country, fraud_score
                from read_parquet('{PARQUET}', union_by_name = true)
                where transaction_id in ({', '.join(f"'{i}'" for i in ids)})""").fetchall()
        except duckdb.Error:
            rows = []
        finally:
            conn.close()
        for tid, amount, count, new_ip, score in sorted(rows, key=lambda r: ids.index(r[0])):
            if tid in shown:
                continue
            shown.add(tid)
            decision = "-" if score is None else ("REVIEW" if score >= 0.5 else "approve")
            print(f"{ids.index(tid) + 1:>2}  {amount:>9.2f}  {count:>19}  {str(bool(new_ip)):>14}  "
                  f"{'-' if score is None else f'{score:.3f}':>6}  {decision}")
    if len(shown) < len(ids):
        print(f"\nOnly {len(shown)} of {len(ids)} arrived in 45 s: is the consumer running with --score-url? "
              "(it flushes to Parquet every 10 s)")


if __name__ == "__main__":
    main()
