with src_data as (
    select
        salesreasonid as id_motivo
        , name as motivo_venda
        , reasontype as tipo_motivo
    from {{ source('adventure_works', 'salesreason') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_motivo']) }} as sk_motivo
    from src_data
)

select *
from create_sk