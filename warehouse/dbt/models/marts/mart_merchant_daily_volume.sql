-- Daily volume and risk metrics per merchant, in CAD.
select
    t.transaction_date,
    t.merchant_id,
    m.merchant_name,
    m.category,
    m.country,
    count(*) as transaction_count,
    count(case when t.is_approved then 1 end) as approved_count,
    count(case when not t.is_approved then 1 end) as declined_count,
    round(count(case when not t.is_approved then 1 end) * 1.0 / count(*), 4) as decline_rate,
    sum(case when t.is_approved then t.amount_cad else 0 end) as approved_volume_cad,
    sum(t.refunded_amount_cad) as refunded_volume_cad,
    cast(sum(t.chargeback_count) as bigint) as chargeback_count,
    count(case when t.has_fraud_chargeback then 1 end) as fraud_chargeback_count,
    round(count(case when t.is_card_not_present then 1 end) * 1.0 / count(*), 4) as card_not_present_share
from {{ ref('fct_transactions') }} as t
inner join {{ ref('dim_merchants') }} as m on t.merchant_id = m.merchant_id
group by 1, 2, 3, 4, 5
