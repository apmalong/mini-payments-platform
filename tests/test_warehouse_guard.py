import pytest

import warehouse_tools as w


@pytest.fixture(autouse=True)
def analyst_views(monkeypatch):
    monkeypatch.setattr(w, "schema", lambda: {"fct_transactions": [("amount_cad", "DECIMAL")],
                                              "dim_merchants": [("merchant_id", "VARCHAR")]})


def test_allows_select_and_scopes_it_to_the_agent_schema():
    sql = w.check("select merchant_id from dim_merchants").sql()
    assert "role_analyst.dim_merchants" in sql and "LIMIT 200" in sql


def test_allows_ctes():
    w.check("with t as (select * from fct_transactions) select count(*) from t")


def test_keeps_a_smaller_limit():
    assert "LIMIT 5" in w.check("select * from dim_merchants limit 5").sql()


@pytest.mark.parametrize("sql, reason", [
    ("delete from fct_transactions", "only SELECT"),
    ("select 1 from dim_merchants; drop table x", "exactly one"),
    ("select * from raw.customers", "not available"),
    ("select email from analytics.stg_customers", "not available"),
    ("select * from role_fraud_ops.stg_customers", "not available"),
    ("select * from fct_transactions where merchant_id in (select merchant_id from raw.merchants)", "not available"),
    ("select * from read_csv('.env')", "table functions"),
    ("select * from read_parquet('x/*.parquet')", "table functions"),
])
def test_rejects(sql, reason):
    with pytest.raises(w.QueryRejected, match=reason):
        w.check(sql)
