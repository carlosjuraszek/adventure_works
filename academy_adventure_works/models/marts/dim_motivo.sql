with salesreason as (
    select *
    from {{ ref('stg_salesreason') }}
)

, salesorderheadersalesreason as (
    select *
    from {{ ref('stg_salesorderheadersalesreason') }}
)

, joining_tables as (
    select
        salesorderheadersalesreason.sk_pedido
        , salesreason.sk_motivo
        , salesreason.motivo_venda
        , salesreason.tipo_motivo
    from salesorderheadersalesreason
    left join salesreason on salesorderheadersalesreason.sk_motivo = salesreason.sk_motivo
)

select *
from joining_tables