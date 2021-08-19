with person as (
    select *
    from {{ ref('stg_person') }}
)

, salesperson as (
    select *
    from {{ ref('stg_salesperson') }}
)

, joining_tables as (
    select
        salesperson.*
        , person.nome_pessoa
        , person.pessoa_tipo
        , person.email_promo
    from salesperson
    left join person on salesperson.id_vendedor = person.id_pessoa
)

select *
from joining_tables