with src_data as (
    select
        cast(salesorderid as INT64) as id_pedido
        , cast(salesorderdetailid as INT64) as id_item
        , orderqty as quantidade
        , cast(productid as INT64) as id_produto
        , unitprice as preco_unidade
        , unitpricediscount as desconto_item
        , (unitprice * (1 - unitpricediscount) * orderqty) as subtotal
    from {{ source('adventure_works', 'salesorderdetail') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_pedido']) }} as sk_pedido
        , {{ dbt_utils.surrogate_key(['id_item']) }} as sk_item
        , {{ dbt_utils.surrogate_key(['id_produto']) }} as sk_produto
    from src_data
)

select *
from create_sk