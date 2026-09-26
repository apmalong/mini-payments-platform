select
    merchant_id,
    name as merchant_name,
    category,
    mcc,
    country,
    currency,
    card_present,
    created_at,
    updated_at
from {{ source('raw', 'merchants') }}
