-- Refunds and chargebacks rolled up to one row per transaction. They arrive days or weeks
-- after the transaction, so fct_transactions must pick up changes here, not only in the
-- transaction row itself.
with refunds as (
    select
        transaction_id,
        count(*) as refund_count,
        sum(amount) as refunded_amount,
        max(updated_at) as updated_at
    from {{ ref('stg_refunds') }}
    group by transaction_id
),

chargebacks as (
    select
        transaction_id,
        count(*) as chargeback_count,
        sum(amount) as chargeback_amount,
        bool_or(is_fraud) as has_fraud_chargeback,
        max(chargeback_status) as chargeback_status,
        max(updated_at) as updated_at
    from {{ ref('stg_chargebacks') }}
    group by transaction_id
)

select
    coalesce(r.transaction_id, c.transaction_id) as transaction_id,
    coalesce(r.refund_count, 0) as refund_count,
    coalesce(r.refunded_amount, 0) as refunded_amount,
    coalesce(c.chargeback_count, 0) as chargeback_count,
    coalesce(c.chargeback_amount, 0) as chargeback_amount,
    coalesce(c.has_fraud_chargeback, false) as has_fraud_chargeback,
    c.chargeback_status,
    greatest(coalesce(r.updated_at, c.updated_at), coalesce(c.updated_at, r.updated_at)) as updated_at
from refunds as r
full outer join chargebacks as c on r.transaction_id = c.transaction_id
