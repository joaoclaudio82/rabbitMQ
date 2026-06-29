"""Testes das rotas HTTP com httpx + ASGITransport (sem subir broker nem servidor).

O producer é substituído por um fake, então as rotas são exercitadas de ponta a ponta
sem RabbitMQ. ASGITransport não dispara o lifespan, então o broker nunca conecta.
"""
import httpx
import pytest

from app.main import app
from app.schemas.pedido import MensagemPedido, StatusPedido


@pytest.fixture
def fake_producer(monkeypatch):
    async def _fake(dados, routing_key, pedido_id=None):
        return MensagemPedido(
            pedido_id=pedido_id or "ID-GERADO",
            cliente=dados.cliente,
            itens=dados.itens,
            status=StatusPedido.criado if routing_key.endswith("criado") else StatusPedido.pago,
            total=sum(i.quantidade * i.preco_unitario for i in dados.itens),
            criado_em="2026-01-01T00:00:00Z",
        )

    monkeypatch.setattr("app.routes.pedidos.producer.publicar_pedido", _fake)


def _client():
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


PEDIDO = {"cliente": "Joao", "itens": [{"produto": "teclado", "quantidade": 1, "preco_unitario": 200.0}]}


async def test_criar_pedido_retorna_202_enfileirado(fake_producer):
    async with _client() as client:
        resp = await client.post("/pedidos", json=PEDIDO)
    assert resp.status_code == 202
    body = resp.json()
    assert body["status"] == "enfileirado"
    assert body["routing_key"] == "pedido.criado"
    assert body["pedido_id"] == "ID-GERADO"


async def test_pagar_usa_pedido_id_da_url(fake_producer):
    """Verifica a correção do bug: o id da URL é o id do evento publicado."""
    async with _client() as client:
        resp = await client.post("/pedidos/PED-XYZ/pagar", json=PEDIDO)
    assert resp.status_code == 202
    body = resp.json()
    assert body["pedido_id"] == "PED-XYZ"
    assert body["routing_key"] == "pedido.pago"


async def test_criar_pedido_com_quantidade_invalida_retorna_422(fake_producer):
    invalido = {"cliente": "Joao", "itens": [{"produto": "x", "quantidade": 0, "preco_unitario": 10.0}]}
    async with _client() as client:
        resp = await client.post("/pedidos", json=invalido)
    assert resp.status_code == 422


async def test_health_responde_ok():
    async with _client() as client:
        resp = await client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["broker_conectado"] is False  # sem lifespan, não conectou
