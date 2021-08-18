with src_data as (
    select
        businessentityid as id_vendedor
        , territoryid as id_local
        , salesquota as previsao_anual_vendas
        , salesytd as vendas_agora
        , saleslastyear as vendas_passado
    from {{ source('adventure_works', 'salesperson') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_vendedor']) }} as sk_vendedor
        , {{ dbt_utils.surrogate_key(['id_local']) }} as sk_local
    from src_data
)

select *
from create_sk