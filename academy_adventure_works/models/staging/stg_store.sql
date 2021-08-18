with src_data as (
    select
        name as nome_loja
        , businessentityid as id_loja
        , salespersonid as id_vendedor
    from {{ source('adventure_works', 'store') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_loja']) }} as sk_loja
        , {{ dbt_utils.surrogate_key(['id_vendedor']) }} as sk_vendedor
    from src_data
)

select *
from create_sk