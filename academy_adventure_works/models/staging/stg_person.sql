with src_data as (
    select
        businessentityid as id_pessoa
        , persontype as pessoa_tipo
        , case
            when middlename is not null then concat(firstname, ' ', middlename, ' ', lastname)
            else concat(firstname, ' ', lastname)
          end as nome_pessoa
        , case
            when emailpromotion = 1 then 'Sim'
            else 'Nao'
          end as email_promo        
    from {{ source('adventure_works', 'person') }}
)

, create_sk as (
    select
        *
        , {{ dbt_utils.surrogate_key(['id_pessoa']) }} as sk_pessoa
    from src_data
)

select *
from create_sk