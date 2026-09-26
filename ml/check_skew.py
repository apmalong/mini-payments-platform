"""Module 7: training/serving skew check.

Joins the features the streaming consumer computed online (streaming/offline Parquet) to the
features dbt computed offline (analytics.ml_transaction_features) for the same transactions,
and reports how often each windowed feature agrees. The model is trained on the offline values
and scored on the online ones, so a feature that disagrees here is one the model sees differently
in production than in training.

    python check_skew.py              # report
    python check_skew.py --min-match 0.95   # exit 1 if any feature agrees less often than that

Run it after syncing and building dbt, so the streamed transactions are in the warehouse.
"""
import argparse
import os
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
PARQUET = ROOT / "streaming" / "offline" / "transaction_features" / "*" / "*.parquet"
# feature -> absolute tolerance for "agrees"
TOLERANCE = {
    "card_txn_count_5m": 0,
    "card_amount_sum_1h": 0.01,
    "seconds_since_last_card_txn": 1.0,
    "is_new_ip_country": 0,
    "merchant_amount_zscore": 0.01,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--min-match", type=float, help="fail if any feature's match rate is below this")
    parser.add_argument("--since", help="only events the consumer processed after this UTC time, e.g. 2026-09-26T21:30:00")
    args = parser.parse_args()
    since = f"and processed_at >= timestamptz '{args.since}+00:00'" if args.since else ""

    connection = duckdb.connect(os.environ.get("DUCKDB_PATH", str(ROOT / "warehouse.duckdb")), read_only=True)
    connection.execute(f"""
        create temp view online as
        select * from read_parquet('{PARQUET.as_posix()}', union_by_name = true)
        where not is_late {since}
        qualify row_number() over (partition by transaction_id order by processed_at) = 1
    """)
    joined = connection.execute("""
        select count(*) from online o join analytics.ml_transaction_features f using (transaction_id)
    """).fetchone()[0]
    online_total = connection.execute("select count(*) from online").fetchone()[0]
    if joined == 0:
        sys.exit("No streamed transactions found in ml_transaction_features: sync and run dbt first.")
    print(f"{joined} of {online_total} streamed transactions found offline\n")
    print(f"{'feature':<30}{'match rate':>11}{'median |diff|':>15}{'max |diff|':>12}")

    worst = 1.0
    for feature, tolerance in TOLERANCE.items():
        diff = (f"abs(cast(o.{feature} as double) - cast(f.{feature} as double))"
                if feature != "seconds_since_last_card_txn" else
                f"abs(coalesce(o.{feature}, -1) - coalesce(f.{feature}, -1))")
        match_rate, median_diff, max_diff = connection.execute(f"""
            select avg(case when {diff} <= {tolerance} then 1.0 else 0.0 end), median({diff}), max({diff})
            from online o join analytics.ml_transaction_features f using (transaction_id)
        """).fetchone()
        worst = min(worst, match_rate)
        print(f"{feature:<30}{match_rate:>10.1%}{median_diff:>15.3f}{max_diff:>12.3f}")

    if args.min_match is not None and worst < args.min_match:
        sys.exit(f"\nskew: a feature matches only {worst:.1%} of the time (minimum {args.min_match:.0%})")


if __name__ == "__main__":
    main()
