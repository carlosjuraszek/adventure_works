with src_data as (
    select
        cast(salesorderid as INT64) as id_pedido
        , revisionnumber as revisao_numero
        , cast(orderdate as date) as data_pedido
        , cast(duedate as date) as data_entrega
        , cast(shipdate as date) as data_envio
        , case
            when status = 1 then 'In Process'
            when status = 2 then 'Approved'
            when status = 3 then 'Backordered'
            when status = 4 then 'Rejected'
            when status = 5 then 'Shipped'
            else 'Cancelled'
          end as status_pedido
        , case
            when onlineorderflag is true then 'Sim'
            else 'Nao'
          end as pedido_online
        , purchaseordernumber as numero_compra
        , accountnumber as numero_conta
        , customerid as id_cliente
        , salespersonid as id_vendedor
        , shiptoaddressid as id_endereco
        , creditcardid as id_cartaocredito
        , taxamt as imposto
        , freight as frete
        , totaldue as valor_total
    from {{ source('adventure_works', 'salesorderheader') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_pedido']) }} as sk_pedido
        , {{ dbt_utils.surrogate_key(['id_cliente']) }} as sk_cliente
        , {{ dbt_utils.surrogate_key(['id_vendedor']) }} as sk_vendedor
        , {{ dbt_utils.surrogate_key(['id_endereco']) }} as sk_endereco
        , {{ dbt_utils.surrogate_key(['id_cartaocredito']) }} as sk_cartaocredito
        , {{ dbt_utils.surrogate_key(['data_pedido']) }} as sk_data
    from src_data
)

select *
from create_sk