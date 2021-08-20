with store as (
    select *
    from {{ ref('stg_store') }}
)

, personcreditcard as (
    select *
    from {{ ref('stg_personcreditcard') }}
)

, creditcard as (
    select *
    from {{ ref('stg_creditcard') }}
)

, person as (
    select *
    from {{ ref('stg_person') }}
)


, customer as (
    select *
    from {{ ref('stg_customer') }}
)

, store_customer as (
    select
        customer.id_cliente
        , customer.sk_cliente
        , person.nome_pessoa
        , 'SC' as pessoa_tipo
        , 'Not Informed' as tipo_cartao
        , 'Nao' as email_promo
    from customer
    left join store on store.sk_loja = customer.sk_loja
    left join person on store.sk_vendedor = person.sk_pessoa
    where customer.id_pessoa is null
)

, retail_customer as (
    select
        customer.id_cliente
        , customer.sk_cliente
        , person.nome_pessoa
        , person.pessoa_tipo
        , cast(coalesce(creditcard.tipo_cartao, 'Not Informed') as string) as tipo_cartao
        , person.email_promo
    from customer
    left join person on customer.sk_pessoa = person.sk_pessoa
    left join personcreditcard on customer.sk_pessoa = personcreditcard.sk_pessoa
    left join creditcard on personcreditcard.sk_cartaocredito = creditcard.sk_cartaocredito
)

, joining_tables as (
    select *
    from store_customer
    union all
    select *
    from retail_customer
)

select *
from joining_tables