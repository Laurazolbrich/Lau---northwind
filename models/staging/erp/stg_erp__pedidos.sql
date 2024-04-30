with
    fonte_pedidos as (
        select 
           cast(ID as int) pk_pedido
           , cast(CUSTOMERID as string) as fk_cliente
           , cast(EMPLOYEEID as int) as fk_funcionario
           , cast(ORDERDATE as date) as data_do_pedido
           , cast(REQUIREDDATE as date) as data_requerida_entrega
           , cast(SHIPPEDDATE as date) as data_do_envio
           , cast(SHIPVIA as int) as fk_transportadora
           , cast(FREIGHT as numeric) as frete
           , cast(SHIPNAME as string) as nm_destinatario
           --, cast(SHIPADDRESS as string) as endereco_destinatario 
           , cast(SHIPCITY as string) as cidade_destinatario
        from {{ source('erp', '_order_') }}
    )

select *
from fonte_pedidos