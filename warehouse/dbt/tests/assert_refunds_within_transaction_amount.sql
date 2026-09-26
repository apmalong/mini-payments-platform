-- A transaction can't be refunded for more than it was charged.
{{ config(severity='warn') }}

select transaction_id, amount, refunded_amount, refund_count
from {{ ref('fct_transactions') }}
where refunded_amount > amount
