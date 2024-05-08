with
    pedidos as (
        select *
        from {{ ref('stg_erp__pedidos') }}
    )

    , detalhes_pedidos as (
        select *
        from {{ ref('stg_erp__detalhes_pedidos') }}
    )

    , joined as (
        select
            detalhes_pedidos.FK_PEDIDO
            , detalhes_pedidos.FK_PRODUTO
            , pedidos.FK_FUNCIONARIO
            , pedidos.FK_CLIENTE
            , pedidos.FK_TRANSPORTADORA
            , detalhes_pedidos.PRECO_DA_UNIDADE
            , detalhes_pedidos.QUANTIDADE
            , detalhes_pedidos.DESCONTO_PERC
            , pedidos.DATA_DO_PEDIDO
            , pedidos.DATA_REQUERIDA_ENTREGA
            , pedidos.DATA_DO_ENVIO
            , pedidos.FRETE
            , pedidos.NM_DESTINATARIO
            , pedidos.CIDADE_DESTINATARIO
        from detalhes_pedidos
        left join pedidos on detalhes_pedidos.fk_pedido = pedidos.pk_pedido
    )

    , criada_chave_primaria as(
        select
            cast(FK_PEDIDO as varchar) || '-' || FK_PRODUTO::varchar as sk_vendas
            , *
        from joined 
    )

select *
from criada_chave_primaria