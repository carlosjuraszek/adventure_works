with src_data as (
    select
        productcategoryid as id_categoria
        , name as categoria
    from {{ source('adventure_works', 'productcategory') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_categoria']) }} as sk_categoria
    from src_data
)

select *
from create_sk