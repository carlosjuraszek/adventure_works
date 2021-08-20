with productcategory as (
    select *
    from {{ ref('stg_productcategory') }}
)

, productsubcategory as (
    select *
    from {{ ref('stg_productsubcategory') }}
)

, productdescription as (
    select *
    from {{ ref('stg_productdescription') }}
)

, productmodelproductdescriptionculture as (
    select *
    from {{ ref('stg_productmodelproductdescriptionculture') }}
)

, productmodel as (
    select *
    from {{ ref('stg_productmodel') }}
)

, product as (
    select *
    from {{ ref('stg_product') }}
)

, final_table as (
    select
        product.sk_produto
        , product.id_produto
        , product.produto
        , product.produto_numero
        , product.produto_comprado
        , product.pode_vender
        , product.cor_produto
        , product.estoque_seguranca
        , product.ponto_compra
        , product.custo_padrao
        , product.preco_venda
        , product.tamanho_produto
        , product.tamanho_unidade
        , product.peso_produto
        , product.peso_unidade
        , product.dias_fabricar
        , product.linha_produto
        , product.classe_produto
        , product.estilo_produto
        , coalesce(productcategory.categoria, 'Not Informed') as categoria
        , coalesce(productsubcategory.subcategoria, 'Not Informed') as subcategoria
        , coalesce(productdescription.descricao, 'Not Informed') as descricao
        , productmodel.nome_modelo as produto_modelo
    from product
    left join productsubcategory on product.sk_subcategoria = productsubcategory.sk_subcategoria
    left join productcategory on productcategory.sk_categoria = productsubcategory.sk_categoria
    left join productmodel on product.sk_modelo = productmodel.sk_modelo
    left join productmodelproductdescriptionculture on productmodel.sk_modelo = productmodelproductdescriptionculture.sk_modelo
    left join productdescription on productmodelproductdescriptionculture.sk_descricao = productdescription.sk_descricao

)

select *
from final_table