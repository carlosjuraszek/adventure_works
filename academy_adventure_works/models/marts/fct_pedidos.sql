with orders as (
    select *
    from {{ ref('stg_salesorderheader') }}
)

, clientes as (
    select *
    from {{ ref('dim_clientes') }}
)

, cidade as (
    select *
    from {{ ref('dim_cidade') }}
)

, estado as (
    select *
    from {{ ref('dim_estado') }}
)

, pais as (
    select *
    from {{ ref('dim_pais') }}
)

, vendedor as (
    select *
    from {{ ref('dim_vendedor') }}
)

, motivo as (
    select *
    from {{ ref('dim_motivo') }}
)

, datas as (
    select *
    from {{ ref('dim_data') }}
)

, joining_tables as (
    select
        orders.sk_cliente
        , orders.sk_endereco
        , pais.sk_pais
        , estado.sk_estado
        , orders.sk_vendedor
        , datas.sk_data
        , orders.sk_pedido
        , orders.id_pedido
        , case
            when motivo.motivo_venda is null then 'Not Informed'
            else motivo.motivo_venda
          end as motivo
        , case
            when motivo.tipo_motivo is null then 'Not Informed'
            else motivo.tipo_motivo
          end as tipo_motivo
        , orders.numero_compra
        , orders.pedido_online
        , orders.data_pedido
        , orders.data_envio
        , orders.data_entrega
        , orders.frete
        , orders.imposto
        , orders.valor_total
    from orders
    left join clientes on orders.sk_cliente = clientes.sk_cliente
    left join vendedor on orders.sk_vendedor = vendedor.sk_vendedor
    left join motivo on orders.sk_pedido = motivo.sk_pedido
    left join datas on orders.data_pedido = datas.dates
    left join cidade on orders.sk_endereco = cidade.sk_endereco
    left join estado on cidade.sk_estado = estado.sk_estado
    left join pais on estado.sk_pais = pais.sk_pais
)

select *
from joining_tables