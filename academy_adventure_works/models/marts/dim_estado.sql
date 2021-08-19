with staging_file as (
    select *
    from {{ ref('stg_stateprovince') }}
)

select *
from staging_file