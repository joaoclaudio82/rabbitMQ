"""Conexão e topologia do RabbitMQ (aio-pika).

Decisões de design relevantes:
- connect_robust: reconecta sozinho após quedas de rede, sem código extra de retry.
- Exchange topic: uma routing key (ex. pedido.*) atende várias filas com bindings distintos.
- DLX/DLQ: pedidos que falham no processamento ou estouram o TTL são desviados para a
  dead-letter queue em vez de descartados, permitindo inspeção e reprocessamento.
"""
import aio_pika
from aio_pika import ExchangeType
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel, AbstractExchange

from app.core.config import get_settings

settings = get_settings()


class RabbitMQBroker:
    """Encapsula a conexão robusta e a declaração da topologia."""

    def __init__(self) -> None:
        self.connection: AbstractRobustConnection | None = None
        self.channel: AbstractRobustChannel | None = None
        self.exchange: AbstractExchange | None = None

    async def conectar(self) -> None:
        """Abre a conexão e declara exchange, filas, DLQ e bindings. Idempotente no RabbitMQ."""
        self.connection = await aio_pika.connect_robust(settings.amqp_url)
        self.channel = await self.connection.channel()
        # prefetch limita mensagens não confirmadas por consumidor: evita um worker açambarcar a fila.
        await self.channel.set_qos(prefetch_count=10)

        self.exchange = await self.channel.declare_exchange(
            settings.exchange_name, ExchangeType.TOPIC, durable=True
        )

        # Dead-letter exchange (fanout) + fila: destino das mensagens rejeitadas ou expiradas.
        dlx = await self.channel.declare_exchange(settings.dlx_name, ExchangeType.FANOUT, durable=True)
        dlq = await self.channel.declare_queue(settings.queue_dlq, durable=True)
        await dlq.bind(dlx)

        # Fila de pedidos: x-dead-letter-exchange + TTL ligam o desvio automático para a DLQ.
        fila_pedidos = await self.channel.declare_queue(
            settings.queue_pedidos,
            durable=True,
            arguments={
                "x-dead-letter-exchange": settings.dlx_name,
                "x-message-ttl": settings.pedidos_ttl_ms,
            },
        )
        fila_notif = await self.channel.declare_queue(settings.queue_notificacoes, durable=True)

        # pedido.* alimenta a fila de pedidos; só pedido.pago dispara notificação.
        await fila_pedidos.bind(self.exchange, routing_key="pedido.*")
        await fila_notif.bind(self.exchange, routing_key=settings.routing_pedido_pago)

    async def fechar(self) -> None:
        if self.connection and not self.connection.is_closed:
            await self.connection.close()


broker = RabbitMQBroker()
