with orders as (
    select *
    from {{ ref('stg_salesorderheader') }}
)

, orderdetail as (
    select *
    from {{ ref('stg_salesorderdetail') }}
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

, produtos as (
    select *
    from {{ ref('dim_produtos') }}
)

, datas as (
    select *
    from {{ ref('dim_data') }}
)

, joining_tables as (
    select
        orders.sk_cliente
        , orderdetail.sk_produto
        , vendedor.sk_vendedor
        , orders.sk_endereco
        , pais.sk_pais
        , estado.sk_estado
        , datas.sk_data
        , orders.sk_pedido
        , orders.id_pedido
        , orderdetail.sk_item
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
        , orderdetail.quantidade
        , orderdetail.preco_unidade
        , orderdetail.desconto_item
        , orderdetail.subtotal
        , orders.frete
        , orders.imposto
        , orders.valor_total
    from orders
    left join orderdetail on orders.sk_pedido = orderdetail.sk_pedido
    left join clientes on orders.sk_cliente = clientes.sk_cliente
    left join produtos on orderdetail.sk_produto = produtos.sk_produto
    left join vendedor on orders.sk_vendedor = vendedor.sk_vendedor
    left join motivo on motivo.sk_pedido = orders.sk_pedido
    left join datas on orders.data_pedido = datas.dates
    left join cidade on orders.sk_endereco = cidade.sk_endereco
    left join estado on cidade.sk_estado = estado.sk_estado
    left join pais on estado.sk_pais = pais.sk_pais
)

select *
from joining_tables