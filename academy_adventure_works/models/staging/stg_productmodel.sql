with src_data as (
    select
        productmodelid as id_modelo
        , name as nome_modelo
    from {{ source('adventure_works', 'productmodel') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_modelo']) }} as sk_modelo
    from src_data
)

select *
from create_sk