with src_data as (
    select
        customerid as id_cliente
        , personid as id_pessoa
        , storeid as id_loja
    from {{ source('adventure_works', 'customer') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_pessoa']) }} as sk_pessoa
        , {{ dbt_utils.surrogate_key(['id_cliente']) }} as sk_cliente
        , {{ dbt_utils.surrogate_key(['id_loja']) }} as sk_loja
    from src_data
)

select *
from create_sk