with src_data as (
    select
        creditcardid as id_cartaocredito
        , cardtype as tipo_cartao
        , cardnumber as numero_cartao
        , expmonth as mes_expira
        , expyear as ano_expira
    from {{ source('adventure_works', 'creditcard') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_cartaocredito']) }} as sk_cartaocredito
    from src_data
)

select *
from create_sk