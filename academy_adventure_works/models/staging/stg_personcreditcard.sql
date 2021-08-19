with src_data as (
    select
        businessentityid as id_pessoa
        , creditcardid as id_cartaocredito
    from {{ source('adventure_works', 'personcreditcard') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_cartaocredito']) }} as sk_cartaocredito
        , {{ dbt_utils.surrogate_key(['id_pessoa']) }} as sk_pessoa
    from src_data
)

select *
from create_sk