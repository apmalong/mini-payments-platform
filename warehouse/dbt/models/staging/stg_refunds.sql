select
    refund_id,
    transaction_id,
    cast(amount as decimal(18, 2)) as amount,
    reason,
    created_at,
    updated_at
from {{ source('raw', 'refunds') }}
