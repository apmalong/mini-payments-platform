{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge' if target.type == 'bigquery' else 'delete+insert',
        on_schema_change='append_new_columns',
        partition_by={'field': 'transaction_date', 'data_type': 'date'} if target.type == 'bigquery' else none,
        cluster_by=['merchant_id'] if target.type == 'bigquery' else none,
    )
}}

-- Incremental: a transaction is reprocessed when its own row changes (status updates) OR a
-- refund/chargeback lands against it later. Filtering on the transaction's updated_at alone
-- would miss chargebacks arriving weeks after settlement.
with transactions as (
    select * from {{ ref('stg_transactions') }}
),

adjustments as (
    select * from {{ ref('int_transaction_adjustments') }}
),

fx as (
    select * from {{ ref('fx_rates') }}
),

joined as (
    select
        t.transaction_id,
        t.merchant_id,
        t.customer_id,
        t.card_token,
        {{ mask_email('t.customer_email') }} as customer_email_hash,

        t.amount,
        t.currency,
        round(t.amount * fx.rate_to_cad, 2) as amount_cad,
        t.status,
        t.is_approved,
        t.entry_mode,
        t.is_card_not_present,
        t.ip_country,
        t.card_network,
        t.response_code,
        t.avs_result,
        t.cvv_result,
        t.three_ds_authenticated,
        t.network_risk_score,
        t.device_type,

        coalesce(a.refund_count, 0) as refund_count,
        coalesce(a.refunded_amount, 0) as refunded_amount,
        round(coalesce(a.refunded_amount, 0) * fx.rate_to_cad, 2) as refunded_amount_cad,
        coalesce(a.chargeback_count, 0) as chargeback_count,
        coalesce(a.has_fraud_chargeback, false) as has_fraud_chargeback,
        a.chargeback_status,

        t.transaction_date,
        t.created_at,
        t.updated_at,
        greatest(t.updated_at, coalesce(a.updated_at, t.updated_at)) as last_changed_at
    from transactions as t
    left join adjustments as a on t.transaction_id = a.transaction_id
    left join fx on t.currency = fx.currency
)

select * from joined
{% if is_incremental() %}
where last_changed_at > (select max(last_changed_at) from {{ this }})
{% endif %}
