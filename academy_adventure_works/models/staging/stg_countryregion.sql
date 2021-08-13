with src_data as (
    select
        name as pais
        , countryregioncode as sigla_pais
    from {{ source('adventure_works', 'countryregion') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['sigla_pais']) }} as sk_pais
    from src_data
)

select *
from create_sk