with src_data as (
    select
        productsubcategoryid as id_subcategoria
        , productcategoryid as id_categoria
        , name as subcategoria
    from {{ source('adventure_works', 'productsubcategory') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_subcategoria']) }} as sk_subcategoria
        , {{ dbt_utils.surrogate_key(['id_categoria']) }} as sk_categoria
    from src_data
)

select *
from create_sk