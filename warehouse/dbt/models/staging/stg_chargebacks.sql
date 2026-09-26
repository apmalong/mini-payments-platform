select
    chargeback_id,
    transaction_id,
    cast(amount as decimal(18, 2)) as amount,
    reason_code,
    reason,
    reason_code = '10.4' as is_fraud,   -- Visa 10.4: other fraud, card-absent environment
    status as chargeback_status,
    created_at,
    updated_at
from {{ source('raw', 'chargebacks') }}
