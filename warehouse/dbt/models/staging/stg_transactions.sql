-- One row per transaction_id. Client retries leave duplicate rows in raw (same transaction_id,
-- different source row id); keep the most recently updated copy.
with deduped as (
    select *
    from {{ source('raw', 'transactions') }}
    qualify row_number() over (partition by transaction_id order by updated_at desc, id desc) = 1
)

select
    transaction_id,
    id as source_row_id,
    merchant_id,
    customer_id,
    card_token,
    customer_email,
    cast(amount as decimal(18, 2)) as amount,
    upper(currency) as currency,
    status,
    status in ('authorized', 'captured', 'settled') as is_approved,
    entry_mode,
    entry_mode = 'ecommerce' as is_card_not_present,
    ip_country,

    -- card-network response (semi-structured)
    {{ json_field('raw_payload', 'network') }} as card_network,
    {{ json_field('raw_payload', 'response_code') }} as response_code,
    {{ json_field('raw_payload', 'avs_result') }} as avs_result,
    {{ json_field('raw_payload', 'cvv_result') }} as cvv_result,
    {{ json_field('raw_payload', 'three_ds.version') }} as three_ds_version,
    cast({{ json_field('raw_payload', 'three_ds.authenticated') }} as boolean) as three_ds_authenticated,
    cast({{ json_field('raw_payload', 'network_risk_score') }} as integer) as network_risk_score,
    {{ json_field('raw_payload', 'device.type') }} as device_type,
    {{ json_field('raw_payload', 'device.os') }} as device_os,

    cast(created_at as date) as transaction_date,
    created_at,
    updated_at
from deduped
