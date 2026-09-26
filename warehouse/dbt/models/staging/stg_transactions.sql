-- Module 3: typed, deduplicated transactions with fields extracted from raw_payload.
-- TODO: dedupe on transaction_id (latest updated_at wins), extract JSON fields.
select *
from {{ source('raw', 'transactions') }}
