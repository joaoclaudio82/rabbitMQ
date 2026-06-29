"""
Testes da camada de schema e regra de total.
Rodar com:  pytest -v
"""
import pytest
from pydantic import ValidationError

from app.schemas.pedido import PedidoInput, ItemPedido, MensagemPedido, StatusPedido


def test_calculo_total():
    pedido = PedidoInput(
        cliente="Joao",
        itens=[
            ItemPedido(produto="teclado", quantidade=2, preco_unitario=100.0),
            ItemPedido(produto="cabo", quantidade=3, preco_unitario=10.0),
        ],
    )
    total = sum(i.quantidade * i.preco_unitario for i in pedido.itens)
    assert total == 230.0


def test_quantidade_invalida_falha():
    with pytest.raises(ValidationError):
        ItemPedido(produto="x", quantidade=0, preco_unitario=10.0)


def test_mensagem_serializa_json():
    msg = MensagemPedido(
        pedido_id="abc",
        cliente="Joao",
        itens=[ItemPedido(produto="teclado", quantidade=1, preco_unitario=50.0)],
        status=StatusPedido.criado,
        total=50.0,
        criado_em="2026-01-01T00:00:00Z",
    )
    payload = msg.model_dump_json()
    assert "abc" in payload
    assert "criado" in payload