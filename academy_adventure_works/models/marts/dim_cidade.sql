with staging_file as (
    select *
    from {{ ref('stg_city') }}
)

select *
from staging_file