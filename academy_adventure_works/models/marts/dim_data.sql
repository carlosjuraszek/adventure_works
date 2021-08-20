with staging_file as (
    select *
    from {{ ref('stg_dates') }}
)

, create_sk as (
    select
        *
        , {{dbt_utils.surrogate_key(['id'])}} as sk_data
    from staging_file
)

select *
from create_sk