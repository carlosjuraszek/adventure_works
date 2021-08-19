with src_data as (
    select
        productid as id_produto
        , name as produto
        , productnumber as produto_numero
        , case
            when makeflag is true then 'Sim'
            else 'Nao'
          end as produto_comprado
        , case
            when finishedgoodsflag is true then 'Sim'
            else 'Nao'
          end as pode_vender
        , coalesce(cast(color as string), 'Not Informed') as cor_produto
        , safetystocklevel as estoque_seguranca
        , reorderpoint as ponto_compra
        , standardcost as custo_padrao
        , listprice as preco_venda
        , coalesce(cast(size as string), 'Not Informed') as tamanho_produto  
        , coalesce(cast(sizeunitmeasurecode as string), 'Not Informed') as tamanho_unidade
        , coalesce(cast(weightunitmeasurecode as string), 'Not Informed') as peso_unidade
        , coalesce(cast(weight as string), 'Not Informed') as peso_produto
        , daystomanufacture as dias_fabricar
        , case
            when productline = 'R' then 'Road'
            when productline = 'M' then 'Mountain'
            when productline = 'T' then 'Touring'
            when productline = 'S' then 'Standard'
            else 'Not Informed'
          end as linha_produto
        , case
            when class = 'H' then 'High'
            when class = 'M' then 'Medium'
            when class = 'L' then 'Low'
            else 'Not Informed'
          end as classe_produto
        , case
            when style = 'W' then 'Womens'
            when style = 'M' then 'Mens'
            when style = 'U' then 'Universal'
            else 'Not Informed'
          end as estilo_produto
        , productmodelid as id_modelo
        , productsubcategoryid as id_subcategoria
    from {{ source('adventure_works', 'product') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_produto']) }} as sk_produto
        , {{ dbt_utils.surrogate_key(['id_modelo']) }} as sk_modelo
        , {{ dbt_utils.surrogate_key(['id_subcategoria']) }} as sk_subcategoria
    from src_data
)

select *
from create_sk