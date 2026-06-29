"""Testes do consumer: caminho feliz, idempotência, regra de negócio e mensagem malformada."""
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.schemas.pedido import ItemPedido, MensagemPedido, StatusPedido
from app.workers.consumer import PedidoConsumer


def _corpo(pedido_id="ped-1", total=100.0, status=StatusPedido.criado) -> bytes:
    return MensagemPedido(
        pedido_id=pedido_id,
        cliente="Ana",
        itens=[ItemPedido(produto="x", quantidade=1, preco_unitario=total)],
        status=status,
        total=total,
        criado_em=datetime(2026, 1, 1, tzinfo=timezone.utc),
    ).model_dump_json().encode("utf-8")


class FakeProcessCtx:
    """Imita aio_pika: ACK ao sair limpo, NACK e propaga em caso de exceção."""

    def __init__(self, msg):
        self.msg = msg

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if exc_type is None:
            self.msg.acked = True
        else:
            self.msg.nacked = True
        return False  # não suprime a exceção


class FakeIncoming:
    def __init__(self, body: bytes):
        self.body = body
        self.acked = False
        self.nacked = False

    def process(self, requeue=False):
        return FakeProcessCtx(self)


async def test_processar_corpo_caminho_feliz():
    consumer = PedidoConsumer()
    await consumer.processar_corpo(_corpo(pedido_id="ped-ok"))
    assert "ped-ok" in consumer._processados


async def test_idempotencia_ignora_pedido_duplicado():
    consumer = PedidoConsumer()
    await consumer.processar_corpo(_corpo(pedido_id="dup"))
    # segunda entrega do mesmo id não deve estourar nem reprocessar
    await consumer.processar_corpo(_corpo(pedido_id="dup"))
    assert consumer._processados == {"dup"}


async def test_total_invalido_vai_para_dlq():
    consumer = PedidoConsumer()
    with pytest.raises(ValueError):
        await consumer.processar_corpo(_corpo(pedido_id="ruim", total=0.0))


async def test_mensagem_malformada_vai_para_dlq():
    consumer = PedidoConsumer()
    with pytest.raises(ValidationError):
        await consumer.processar_corpo(b"{ isto nao eh json valido }")


async def test_processar_pedido_faz_ack_no_sucesso():
    consumer = PedidoConsumer()
    msg = FakeIncoming(_corpo(pedido_id="ack-1"))
    await consumer.processar_pedido(msg)
    assert msg.acked and not msg.nacked


async def test_processar_pedido_faz_nack_em_mensagem_malformada():
    consumer = PedidoConsumer()
    msg = FakeIncoming(b"nao eh json")
    with pytest.raises(ValidationError):
        await consumer.processar_pedido(msg)
    assert msg.nacked and not msg.acked
