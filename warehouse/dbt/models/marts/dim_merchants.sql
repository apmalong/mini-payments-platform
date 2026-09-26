-- Current version of each merchant, from the SCD2 snapshot.
select
    merchant_id,
    merchant_name,
    category,
    mcc,
    country,
    currency,
    card_present,
    created_at,
    dbt_valid_from as valid_from
from (
    select
        merchant_id,
        name as merchant_name,
        category,
        mcc,
        country,
        currency,
        card_present,
        created_at,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('snap_merchants') }}
) as snapshot
where dbt_valid_to is null
