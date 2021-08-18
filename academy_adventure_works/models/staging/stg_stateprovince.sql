with src_data as (
    select
        stateprovinceid as id_estado
        , stateprovincecode as sigla_estado
        , countryregioncode as sigla_pais
        , name as estado
    from {{ source('adventure_works', 'stateprovince') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['sigla_pais']) }} as sk_pais
        , {{ dbt_utils.surrogate_key(['id_estado']) }} as sk_estado
    from src_data
)

select *
from create_sk