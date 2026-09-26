-- Training features: the offline twin of streaming/consumer.py. Same definitions, computed
-- point-in-time over history (each row sees only itself and earlier transactions), so a model
-- trained here sees what the scorer sees online. ml/check_skew.py compares the two.
-- DuckDB-only: the windows range over epoch seconds.
with transactions as (
    select
        *,
        epoch(created_at) as event_ts
    from {{ ref('fct_transactions') }}
),

features as (
    select
        transaction_id,
        card_token,
        merchant_id,
        created_at,
        transaction_date,
        amount,
        currency,
        amount_cad,
        entry_mode,
        avs_result,
        cvv_result,
        three_ds_authenticated,
        network_risk_score,
        is_approved,

        count(*) over card_5m as card_txn_count_5m,
        sum(amount) over card_1h as card_amount_sum_1h,
        event_ts - lag(event_ts) over card_order as seconds_since_last_card_txn,
        coalesce(
            ip_country is not null
            and count(ip_country) over card_before > 0
            and count(*) over card_country_before = 0,
            false
        ) as is_new_ip_country,
        count(*) over merchant_before as merchant_prior_count,
        avg(amount) over merchant_before as merchant_prior_mean,
        stddev_samp(amount) over merchant_before as merchant_prior_stddev,

        has_fraud_chargeback as label
    from transactions
    window
        card_order as (partition by card_token order by event_ts),
        card_5m as (partition by card_token order by event_ts range between 300 preceding and current row),
        card_1h as (partition by card_token order by event_ts range between 3600 preceding and current row),
        card_before as (partition by card_token order by event_ts rows between unbounded preceding and 1 preceding),
        card_country_before as (
            partition by card_token, ip_country order by event_ts rows between unbounded preceding and 1 preceding),
        merchant_before as (
            partition by merchant_id order by event_ts rows between unbounded preceding and 1 preceding)
)

select
    * exclude (merchant_prior_count, merchant_prior_mean, merchant_prior_stddev),
    case
        when merchant_prior_count >= 30 and merchant_prior_stddev > 0
            then (amount - merchant_prior_mean) / merchant_prior_stddev
        else 0
    end as merchant_amount_zscore,
    -- Chargebacks arrive up to 45 days after the transaction; younger labels are incomplete.
    transaction_date <= current_date - 45 as label_mature
from features
