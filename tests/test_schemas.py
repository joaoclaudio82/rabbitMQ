"""Testes dos schemas Pydantic: validação de entrada e formato da mensagem."""
import pytest
from pydantic import ValidationError

from app.schemas.pedido import ItemPedido, MensagemPedido, PedidoInput, StatusPedido


def test_item_quantidade_invalida_falha():
    with pytest.raises(ValidationError):
        ItemPedido(produto="x", quantidade=0, preco_unitario=10.0)


def test_item_preco_invalido_falha():
    with pytest.raises(ValidationError):
        ItemPedido(produto="x", quantidade=1, preco_unitario=0)


def test_pedido_input_aceita_multiplos_itens():
    pedido = PedidoInput(
        cliente="Joao",
        itens=[
            ItemPedido(produto="teclado", quantidade=2, preco_unitario=100.0),
            ItemPedido(produto="cabo", quantidade=3, preco_unitario=10.0),
        ],
    )
    assert len(pedido.itens) == 2


def test_mensagem_serializa_json_com_campos_esperados():
    msg = MensagemPedido(
        pedido_id="abc",
        cliente="Joao",
        itens=[ItemPedido(produto="teclado", quantidade=1, preco_unitario=50.0)],
        status=StatusPedido.criado,
        total=50.0,
        criado_em="2026-01-01T00:00:00Z",
    )
    payload = msg.model_dump_json()
    assert '"pedido_id":"abc"' in payload
    assert '"status":"criado"' in payload


def test_mensagem_roundtrip_json():
    """Serializar e desserializar preserva o conteúdo — é o que trafega na fila."""
    original = MensagemPedido(
        pedido_id="abc",
        cliente="Joao",
        itens=[ItemPedido(produto="teclado", quantidade=1, preco_unitario=50.0)],
        status=StatusPedido.pago,
        total=50.0,
        criado_em="2026-01-01T00:00:00Z",
    )
    restaurada = MensagemPedido.model_validate_json(original.model_dump_json())
    assert restaurada == original
