with src_data as (
    select
        salesorderid as id_pedido
        , salesreasonid as id_motivo
        , modifieddate as data_modificado
    from {{ source('adventure_works', 'salesorderheadersalesreason') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_pedido']) }} as sk_pedido
        , {{ dbt_utils.surrogate_key(['id_motivo']) }} as sk_motivo
    from src_data
)

, data_deduplicate as (
    select
        *
        , row_number() over (partition by sk_pedido order by data_modificado desc) as index
    from create_sk
)

select *
from data_deduplicate
where index = 1