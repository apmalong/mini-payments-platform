select
    card_token,
    customer_id,
    brand,
    bin,
    last4,
    issuer_country,
    created_at
from {{ source('raw', 'cards') }}
