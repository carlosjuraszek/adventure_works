with staging_file as (
    select *
    from {{ ref('stg_countryregion') }}
)

select *
from staging_file