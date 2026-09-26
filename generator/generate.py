"""Module 1: synthetic payments source system.

Writes merchants, customers, cards, transactions, refunds and chargebacks to Postgres.
With --continuous it keeps inserting and updating rows; with --stream it also publishes
each new transaction to Redpanda (Module 6).

Deliberate messiness for the downstream modules to deal with:
- transactions.raw_payload is JSONB (card-network response) for semi-structured practice
- ~0.3% of transactions are inserted twice (client retries): same transaction_id, new row id
- status moves authorized -> captured -> settled after the fact, bumping updated_at
- ~1% of new rows in continuous mode arrive late (created_at hours in the past)
- fraud is never labelled directly; it only shows up later as chargebacks, and not all of it

Card numbers are tokenized at the source: only card_token, BIN and last4 are stored, never a PAN.
"""
import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta

import numpy as np
import psycopg
from faker import Faker
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

TOPIC = "payments.transactions"

# (category, MCC, card_present, median amount, fraud-attractive)
CATEGORIES = [
    ("grocery", "5411", True, 45, False),
    ("restaurant", "5812", True, 38, False),
    ("coffee_shop", "5814", True, 9, False),
    ("gas_station", "5541", True, 60, False),
    ("pharmacy", "5912", True, 30, False),
    ("salon", "7230", True, 70, False),
    ("auto_repair", "7538", True, 350, False),
    ("electronics", "5732", False, 220, True),
    ("jewelry", "5944", False, 400, True),
    ("digital_goods", "5815", False, 25, True),
    ("apparel_online", "5691", False, 85, False),
]
COUNTRIES = [("CA", "CAD", 0.70), ("US", "USD", 0.25), ("GB", "GBP", 0.03), ("DE", "EUR", 0.02)]
FOREIGN_IP_COUNTRIES = ["NG", "RO", "BR", "VN", "RU", "ID"]
CARD_BRANDS = [("visa", "4"), ("mastercard", "5"), ("amex", "37")]
REFUND_REASONS = ["customer_request", "duplicate", "product_not_received", "defective"]
FRAUD_REASON = ("10.4", "fraud_card_absent")
DISPUTE_REASONS = [("13.1", "merchandise_not_received"), ("13.3", "not_as_described"), ("12.6", "duplicate_processing")]

DDL = """
create table if not exists merchants (
    merchant_id  text primary key,
    name         text not null,
    category     text not null,
    mcc          text not null,
    country      text not null,
    currency     text not null,
    card_present boolean not null,
    created_at   timestamptz not null,
    updated_at   timestamptz not null
);
create table if not exists customers (
    customer_id text primary key,
    full_name   text not null,
    email       text not null,
    phone       text,
    country     text not null,
    created_at  timestamptz not null,
    updated_at  timestamptz not null
);
create table if not exists cards (
    card_token     text primary key,
    customer_id    text not null references customers,
    brand          text not null,
    bin            text not null,
    last4          text not null,
    issuer_country text not null,
    created_at     timestamptz not null
);
create table if not exists transactions (
    id             bigserial primary key,
    transaction_id text not null,
    merchant_id    text not null references merchants,
    customer_id    text not null references customers,
    card_token     text not null references cards,
    customer_email text,
    amount         numeric(18,2) not null,
    currency       text not null,
    status         text not null,
    entry_mode     text not null,
    ip_country     text,
    raw_payload    jsonb,
    created_at     timestamptz not null,
    updated_at     timestamptz not null
);
create index if not exists transactions_updated_at_idx on transactions (updated_at);
create index if not exists transactions_status_idx on transactions (status);
create table if not exists refunds (
    refund_id      text primary key,
    transaction_id text not null,
    amount         numeric(18,2) not null,
    reason         text not null,
    created_at     timestamptz not null,
    updated_at     timestamptz not null
);
create table if not exists chargebacks (
    chargeback_id  text primary key,
    transaction_id text not null,
    amount         numeric(18,2) not null,
    reason_code    text not null,
    reason         text not null,
    status         text not null,
    created_at     timestamptz not null,
    updated_at     timestamptz not null
);
"""

TXN_COLS = (
    "transaction_id, merchant_id, customer_id, card_token, customer_email, amount, currency, "
    "status, entry_mode, ip_country, raw_payload, created_at, updated_at"
)


def load_env() -> None:
    """Read ../.env into os.environ (no extra dependency)."""
    path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if not os.path.exists(path):
        return
    for line in open(path, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip())


def dsn() -> str:
    return (
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"user={os.environ.get('POSTGRES_USER', 'payments')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', 'payments')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'payments')}"
    )


def pick_country() -> tuple[str, str]:
    code, currency, _ = random.choices(COUNTRIES, weights=[c[2] for c in COUNTRIES])[0]
    return code, currency


class Generator:
    def __init__(self, conn: psycopg.Connection, producer=None, seed: int = 42):
        random.seed(seed)
        np.random.seed(seed)
        self.fake = Faker(["en_CA", "en_US"])
        Faker.seed(seed)
        self.conn = conn
        self.producer = producer
        self.merchants: list[dict] = []
        self.cards: list[dict] = []
        self.customers: dict[str, dict] = {}

    # ---------- reference data ----------

    def seed_reference_data(self, n_merchants: int, n_customers: int, now: datetime) -> None:
        merchants, customers, cards = [], [], []
        for _ in range(n_merchants):
            category, mcc, card_present, median, risky = random.choice(CATEGORIES)
            country, currency = pick_country()
            created = now - timedelta(days=random.randint(60, 1500))
            merchants.append({
                "merchant_id": f"mer_{uuid.uuid4().hex[:12]}",
                "name": self.fake.company(),
                "category": category, "mcc": mcc, "country": country, "currency": currency,
                "card_present": card_present, "median": median, "risky": risky,
                "created_at": created, "updated_at": created,
            })
        for _ in range(n_customers):
            country, _ = pick_country()
            created = now - timedelta(days=random.randint(1, 1000))
            customer = {
                "customer_id": f"cus_{uuid.uuid4().hex[:12]}",
                "full_name": self.fake.name(), "email": self.fake.unique.email(),
                "phone": self.fake.phone_number(), "country": country,
                "created_at": created, "updated_at": created,
            }
            customers.append(customer)
            for _ in range(random.choice([1, 1, 1, 2])):
                brand, prefix = random.choice(CARD_BRANDS)
                digits = "".join(random.choices("0123456789", k=10))
                cards.append({
                    "card_token": f"tok_{uuid.uuid4().hex}",
                    "customer_id": customer["customer_id"],
                    "brand": brand, "bin": (prefix + digits)[:6], "last4": digits[-4:],
                    "issuer_country": country, "created_at": created,
                })

        with self.conn.cursor() as cur:
            cur.executemany(
                "insert into merchants (merchant_id, name, category, mcc, country, currency, card_present, "
                "created_at, updated_at) values (%(merchant_id)s, %(name)s, %(category)s, %(mcc)s, %(country)s, "
                "%(currency)s, %(card_present)s, %(created_at)s, %(updated_at)s)",
                merchants,
            )
            cur.executemany(
                "insert into customers values (%(customer_id)s, %(full_name)s, %(email)s, %(phone)s, "
                "%(country)s, %(created_at)s, %(updated_at)s)",
                customers,
            )
            cur.executemany(
                "insert into cards values (%(card_token)s, %(customer_id)s, %(brand)s, %(bin)s, %(last4)s, "
                "%(issuer_country)s, %(created_at)s)",
                cards,
            )
        self.conn.commit()
        self.merchants, self.cards = merchants, cards
        self.customers = {c["customer_id"]: c for c in customers}

    def load_reference_data(self) -> None:
        """Reuse existing merchants/customers/cards when running continuous mode against a seeded DB."""
        medians = {c[0]: (c[3], c[4]) for c in CATEGORIES}
        with self.conn.cursor(row_factory=dict_row) as cur:
            self.merchants = cur.execute("select * from merchants").fetchall()
            for m in self.merchants:
                m["median"], m["risky"] = medians[m["category"]]
            self.cards = cur.execute("select * from cards").fetchall()
            self.customers = {c["customer_id"]: c for c in cur.execute("select * from customers").fetchall()}

    # ---------- transactions ----------

    def _payload(self, card: dict, entry_mode: str, approved: bool, fraud: bool) -> dict:
        return {
            "network": card["brand"],
            "auth_code": uuid.uuid4().hex[:6].upper() if approved else None,
            "response_code": "00" if approved else random.choice(["05", "51", "54", "59"]),
            "avs_result": random.choice(["N", "Z"]) if fraud else random.choice(["Y", "Y", "Y", "A"]),
            "cvv_result": ("N" if fraud and random.random() < 0.4 else "M") if entry_mode == "ecommerce" else None,
            "three_ds": {"version": "2.2", "authenticated": not fraud and random.random() < 0.8}
            if entry_mode == "ecommerce" else None,
            "network_risk_score": int(np.clip(np.random.normal(70 if fraud else 20, 15), 0, 99)),
            "device": {"type": random.choice(["mobile", "desktop"]), "os": random.choice(["ios", "android", "windows", "macos"])}
            if entry_mode == "ecommerce" else None,
        }

    def _amount(self, merchant: dict, scale: float = 1.0) -> float:
        return round(max(0.5, float(np.random.lognormal(np.log(merchant["median"] * scale), 0.6))), 2)

    def _status_for_age(self, approved: bool, created: datetime, now: datetime) -> tuple[str, datetime]:
        if not approved:
            return "declined", created
        age = now - created
        if age > timedelta(days=2):
            return "settled", created + timedelta(days=random.uniform(1, 2))
        if age > timedelta(hours=2):
            return "captured", created + timedelta(hours=random.uniform(0.5, 2))
        return "authorized", created

    def make_transaction(self, created: datetime, now: datetime, card: dict | None = None,
                         merchant: dict | None = None, fraud: bool = False, amount: float | None = None) -> dict:
        card = card or random.choice(self.cards)
        customer = self.customers[card["customer_id"]]
        merchant = merchant or random.choice(self.merchants)
        entry_mode = random.choice(["chip", "contactless", "contactless", "swipe"]) if merchant["card_present"] else "ecommerce"
        if fraud:
            entry_mode = "ecommerce"
            ip_country = random.choice(FOREIGN_IP_COUNTRIES)
        else:
            ip_country = customer["country"] if entry_mode == "ecommerce" else None
        approved = random.random() > (0.30 if fraud else 0.05)
        status, updated = self._status_for_age(approved, created, now)
        return {
            "transaction_id": f"txn_{uuid.uuid4().hex}",
            "merchant_id": merchant["merchant_id"],
            "customer_id": customer["customer_id"],
            "card_token": card["card_token"],
            "customer_email": customer["email"] if entry_mode == "ecommerce" else None,
            "amount": amount if amount is not None else self._amount(merchant),
            "currency": merchant["currency"],
            "status": status,
            "entry_mode": entry_mode,
            "ip_country": ip_country,
            "raw_payload": self._payload(card, entry_mode, approved, fraud),
            "created_at": created,
            "updated_at": updated,
            "_fraud": fraud,
        }

    def fraud_pattern(self, start: datetime, now: datetime) -> list[dict]:
        """Either card testing (burst of tiny online charges) or a big CNP purchase at a risky merchant."""
        card = random.choice(self.cards)
        risky = [m for m in self.merchants if m["risky"]] or self.merchants
        if random.random() < 0.5:
            merchant = random.choice(risky)
            txns, t = [], start
            for _ in range(random.randint(4, 12)):
                t += timedelta(seconds=random.randint(5, 90))
                txns.append(self.make_transaction(t, now, card, merchant, fraud=True, amount=round(random.uniform(0.5, 5), 2)))
            txns.append(self.make_transaction(t + timedelta(minutes=2), now, card, merchant, fraud=True,
                                              amount=self._amount(merchant, scale=4)))
            # Shift the burst back so it never ends in the future (future timestamps break cursors).
            overshoot = txns[-1]["created_at"] - now
            if overshoot > timedelta(0):
                for txn in txns:
                    txn["created_at"] -= overshoot
                    txn["status"], txn["updated_at"] = self._status_for_age(
                        txn["status"] != "declined", txn["created_at"], now)
            return txns
        night = start.replace(hour=random.choice([1, 2, 3, 4]))
        return [self.make_transaction(min(night, now), now, card, random.choice(risky), fraud=True,
                                      amount=self._amount(random.choice(risky), scale=5))]

    def random_time(self, start: datetime, end: datetime) -> datetime:
        """Timestamp between start and end, weighted towards daytime."""
        while True:
            t = start + (end - start) * random.random()
            if random.random() < (0.25 if t.hour < 7 else 1.0):
                return t

    def after_effects(self, txns: list[dict], now: datetime) -> tuple[list[dict], list[dict]]:
        refunds, chargebacks = [], []
        for t in txns:
            if t["status"] == "declined":
                continue
            if t["_fraud"]:
                # Only ~70% of fraud is ever disputed, and it arrives 5-45 days later.
                if random.random() < 0.7:
                    created = t["created_at"] + timedelta(days=random.uniform(5, 45))
                    if created < now:
                        chargebacks.append(self._chargeback(t, FRAUD_REASON, created, now))
            elif random.random() < 0.03:
                created = t["created_at"] + timedelta(days=random.uniform(0.5, 20))
                if created < now:
                    refunds.append({
                        "refund_id": f"re_{uuid.uuid4().hex[:16]}", "transaction_id": t["transaction_id"],
                        "amount": t["amount"] if random.random() < 0.8 else round(t["amount"] * random.uniform(0.2, 0.9), 2),
                        "reason": random.choice(REFUND_REASONS), "created_at": created, "updated_at": created,
                    })
            elif random.random() < 0.002:
                created = t["created_at"] + timedelta(days=random.uniform(10, 60))
                if created < now:
                    chargebacks.append(self._chargeback(t, random.choice(DISPUTE_REASONS), created, now))
        return refunds, chargebacks

    def _chargeback(self, t: dict, reason: tuple[str, str], created: datetime, now: datetime) -> dict:
        resolved = now - created > timedelta(days=30)
        status = random.choice(["won", "lost", "lost"]) if resolved else "open"
        updated = created + timedelta(days=30) if resolved else created
        return {
            "chargeback_id": f"cb_{uuid.uuid4().hex[:16]}", "transaction_id": t["transaction_id"],
            "amount": t["amount"], "reason_code": reason[0], "reason": reason[1],
            "status": status, "created_at": created, "updated_at": updated,
        }

    def write(self, txns: list[dict], refunds: list[dict], chargebacks: list[dict]) -> None:
        duplicates = [t for t in txns if random.random() < 0.003]
        rows = [{**t, "raw_payload": Jsonb(t["raw_payload"])} for t in txns + duplicates]
        placeholders = ", ".join(f"%({c.strip()})s" for c in TXN_COLS.split(","))
        with self.conn.cursor() as cur:
            cur.executemany(f"insert into transactions ({TXN_COLS}) values ({placeholders})", rows)
            if refunds:
                cur.executemany(
                    "insert into refunds values (%(refund_id)s, %(transaction_id)s, %(amount)s, %(reason)s, "
                    "%(created_at)s, %(updated_at)s)", refunds)
            if chargebacks:
                cur.executemany(
                    "insert into chargebacks values (%(chargeback_id)s, %(transaction_id)s, %(amount)s, "
                    "%(reason_code)s, %(reason)s, %(status)s, %(created_at)s, %(updated_at)s)", chargebacks)
        self.conn.commit()
        if self.producer:
            for t in txns + duplicates:
                event = {k: v for k, v in t.items() if k not in ("_fraud", "customer_email")}
                self.producer.produce(TOPIC, key=t["card_token"], value=json.dumps(event, default=str))
            self.producer.flush()

    # ---------- modes ----------

    def backfill(self, days: int, n_transactions: int, fraud_rate: float, now: datetime) -> int:
        start = now - timedelta(days=days)
        txns = [self.make_transaction(self.random_time(start, now), now) for _ in range(n_transactions)]
        while sum(t["_fraud"] for t in txns) < n_transactions * fraud_rate:
            txns.extend(self.fraud_pattern(self.random_time(start, now), now))
        txns.sort(key=lambda t: t["created_at"])
        refunds, chargebacks = self.after_effects(txns, now)
        for i in range(0, len(txns), 5000):
            batch = txns[i:i + 5000]
            ids = {t["transaction_id"] for t in batch}
            self.write(batch, [r for r in refunds if r["transaction_id"] in ids],
                       [c for c in chargebacks if c["transaction_id"] in ids])
        return len(txns)

    def db_now(self) -> datetime:
        """Use the database clock, not the host's: the two can drift apart by seconds, and
        host-stamped rows would then look future-dated to anything filtering on now()."""
        return self.conn.execute("select now()").fetchone()[0]

    def tick(self, per_tick: int, fraud_rate: float) -> str:
        now = self.db_now()
        txns = []
        for _ in range(per_tick):
            late = random.random() < 0.01
            created = now - timedelta(hours=random.uniform(1, 12)) if late else now - timedelta(seconds=random.uniform(0, 5))
            txns.append(self.make_transaction(created, now))
        if random.random() < fraud_rate * per_tick:
            txns.extend(self.fraud_pattern(now - timedelta(minutes=5), now))
        for txn in txns:
            # Rows are written now, whatever their business time: late arrivals keep an old
            # created_at but get a fresh updated_at, as a database default would give them.
            txn["updated_at"] = max(txn["updated_at"], now)
        self.write(txns, [], [])

        # Late updates: move older rows through their lifecycle.
        with self.conn.cursor() as cur:
            captured = cur.execute(
                "update transactions set status = 'captured', updated_at = now() "
                "where status = 'authorized' and created_at < now() - interval '2 minutes'").rowcount
            settled = cur.execute(
                "update transactions set status = 'settled', updated_at = now() "
                "where status = 'captured' and updated_at < now() - interval '5 minutes'").rowcount
            refunded = 0
            if random.random() < 0.3:
                refunded = cur.execute(
                    "insert into refunds select 're_' || substr(md5(random()::text), 1, 16), transaction_id, amount, "
                    "'customer_request', now(), now() from transactions where status = 'settled' "
                    "order by random() limit 1").rowcount
        self.conn.commit()
        return f"+{len(txns)} txns, {captured} captured, {settled} settled, {refunded} refunded"


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic payments source system (Module 1)")
    parser.add_argument("--merchants", type=int, default=200)
    parser.add_argument("--customers", type=int, default=5000)
    parser.add_argument("--transactions", type=int, default=50000, help="backfill size")
    parser.add_argument("--days", type=int, default=90, help="backfill history window")
    parser.add_argument("--fraud-rate", type=float, default=0.01)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--reset", action="store_true", help="drop and recreate all tables first")
    parser.add_argument("--continuous", action="store_true", help="keep inserting/updating rows")
    parser.add_argument("--interval", type=float, default=2.0, help="seconds between continuous ticks")
    parser.add_argument("--per-tick", type=int, default=20, help="transactions per continuous tick")
    parser.add_argument("--stream", action="store_true", help="also publish events to Redpanda")
    args = parser.parse_args()

    load_env()
    producer = None
    if args.stream:
        from confluent_kafka import Producer
        producer = Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")})

    with psycopg.connect(dsn()) as conn:
        if args.reset:
            conn.execute("drop table if exists chargebacks, refunds, transactions, cards, customers, merchants cascade")
        conn.execute(DDL)
        conn.commit()
        gen = Generator(conn, producer, seed=args.seed)

        seeded = conn.execute("select count(*) from merchants").fetchone()[0] > 0
        if seeded:
            gen.load_reference_data()
            print(f"Using existing data: {len(gen.merchants)} merchants, {len(gen.cards)} cards")
        else:
            now = gen.db_now()
            gen.seed_reference_data(args.merchants, args.customers, now)
            print(f"Seeded {len(gen.merchants)} merchants, {len(gen.customers)} customers, {len(gen.cards)} cards")
            n = gen.backfill(args.days, args.transactions, args.fraud_rate, now)
            print(f"Backfilled {n} transactions over {args.days} days")

        if args.continuous:
            print(f"Continuous mode: {args.per_tick} txns every {args.interval}s (Ctrl+C to stop)")
            try:
                while True:
                    print(datetime.now().strftime("%H:%M:%S"), gen.tick(args.per_tick, args.fraud_rate), flush=True)
                    time.sleep(args.interval)
            except KeyboardInterrupt:
                print("Stopped.")


if __name__ == "__main__":
    main()
