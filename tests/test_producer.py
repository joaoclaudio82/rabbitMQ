"""Testes do producer com uma exchange falsa, sem RabbitMQ real."""
import json

import pytest

from app.core import broker as broker_mod
from app.schemas.pedido import ItemPedido, PedidoInput, StatusPedido
from app.services.producer import producer

settings = broker_mod.settings


class FakeExchange:
    """Captura o que seria publicado no RabbitMQ."""

    def __init__(self):
        self.publicacoes = []  # lista de (Message, routing_key)

    async def publish(self, message, routing_key):
        self.publicacoes.append((message, routing_key))


@pytest.fixture
def exchange(monkeypatch):
    fake = FakeExchange()
    monkeypatch.setattr(broker_mod.broker, "exchange", fake)
    return fake


def _dados():
    return PedidoInput(
        cliente="Ana",
        itens=[
            ItemPedido(produto="mouse", quantidade=2, preco_unitario=50.0),
            ItemPedido(produto="cabo", quantidade=1, preco_unitario=20.0),
        ],
    )


async def test_publicar_pedido_criado_gera_id_e_calcula_total(exchange):
    msg = await producer.publicar_pedido(_dados(), settings.routing_pedido_criado)

    assert msg.pedido_id  # uuid gerado
    assert msg.status == StatusPedido.criado
    assert msg.total == 120.0
    enviada, routing_key = exchange.publicacoes[0]
    assert routing_key == settings.routing_pedido_criado
    assert enviada.message_id == msg.pedido_id  # envelope carrega o id para idempotência


async def test_publicar_pagamento_preserva_pedido_id_informado(exchange):
    """Correção do bug: o pagamento referencia o pedido existente, não cria outro id."""
    msg = await producer.publicar_pedido(
        _dados(), settings.routing_pedido_pago, pedido_id="PED-123"
    )

    assert msg.pedido_id == "PED-123"
    assert msg.status == StatusPedido.pago
    enviada, routing_key = exchange.publicacoes[0]
    assert routing_key == settings.routing_pedido_pago
    assert enviada.message_id == "PED-123"
    corpo = json.loads(enviada.body)
    assert corpo["pedido_id"] == "PED-123"


async def test_publicar_sem_broker_conectado_levanta_runtime_error(monkeypatch):
    monkeypatch.setattr(broker_mod.broker, "exchange", None)
    with pytest.raises(RuntimeError):
        await producer.publicar_pedido(_dados(), settings.routing_pedido_criado)
