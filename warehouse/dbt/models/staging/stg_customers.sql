select
    customer_id,
    full_name,
    email,
    phone,
    country,
    created_at,
    updated_at
from {{ source('raw', 'customers') }}
