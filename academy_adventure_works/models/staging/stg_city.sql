with src_data as (
    select
        addressid as id_endereco
        , city as cidade
        , stateprovinceid as id_estado
    from {{ source('adventure_works', 'address') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_estado']) }} as sk_estado
        , {{ dbt_utils.surrogate_key(['id_endereco']) }} as sk_endereco
    from src_data
)

select *
from create_sk