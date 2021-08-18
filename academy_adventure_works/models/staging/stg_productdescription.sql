with src_data as (
    select
        productdescriptionid as id_descricao
        , description as descricao
    from {{ source('adventure_works', 'productdescription') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_descricao']) }} as sk_descricao
    from src_data
)

select *
from create_sk