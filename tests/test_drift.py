import duckdb

from exporter import bucket_shares


def test_bucket_shares_cover_all_rows():
    conn = duckdb.connect()
    shares = bucket_shares(conn, "select range::double as v from range(100)", [24.5, 49.5, 74.5])
    assert shares == [0.25, 0.25, 0.25, 0.25]


def test_bucket_shares_handle_empty_buckets():
    conn = duckdb.connect()
    shares = bucket_shares(conn, "select 1.0 as v from range(10)", [5.0, 10.0])
    assert shares == [1.0, 0.0, 0.0]
