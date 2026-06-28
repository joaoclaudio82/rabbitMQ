"""
Camada de conexao com o RabbitMQ usando aio-pika (cliente assincrono).
Aqui criamos a topologia: exchange, filas, dead-letter queue e bindings.
"""
import aio_pika
from aio_pika import ExchangeType
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel, AbstractExchange

from app.core.config import get_settings

settings = get_settings()


class RabbitMQBroker:
    """
    Encapsula a conexao robusta (reconecta sozinha) e a declaracao da topologia.
    """
    #aqui criamos a topologia do RabbitMQ, ou seja, criamos a exchange, as filas e os bindings entre elas
    def __init__(self) -> None:
        self.connection: AbstractRobustConnection | None = None
        self.channel: AbstractRobustChannel | None = None
        self.exchange: AbstractExchange | None = None
    
    #aqui criamos a topologia do RabbitMQ, ou seja, criamos a exchange, as filas e os bindings entre elas
    async def conectar(self) -> None:
       
        """Abre conexao robusta e declara toda a topologia."""
        self.connection = await aio_pika.connect_robust(settings.amqp_url)
        self.channel = await self.connection.channel()
        # Limita quantas mensagens nao confirmadas o consumidor pega por vez.
        await self.channel.set_qos(prefetch_count=10)

        # Exchange principal (topic permite roteamento por padrao de chave).
        self.exchange = await self.channel.declare_exchange(
            settings.exchange_name,
            ExchangeType.TOPIC,
            durable=True,
        )

        # Dead-letter exchange + fila (mensagens que falham ou expiram caem aqui).
        dlx = await self.channel.declare_exchange(
            "pedidos.dlx", ExchangeType.FANOUT, durable=True
        )
        dlq = await self.channel.declare_queue(settings.queue_dlq, durable=True)
        await dlq.bind(dlx)

        # Fila de pedidos com dead-letter configurada.
        fila_pedidos = await self.channel.declare_queue(
            settings.queue_pedidos,
            durable=True,
            arguments={
                "x-dead-letter-exchange": "pedidos.dlx",
                "x-message-ttl": 60000,  # 60s; expira e vai pra DLQ
            },
        )
        # Fila de notificacoes.
        fila_notif = await self.channel.declare_queue(
            settings.queue_notificacoes, durable=True
        )

        # Bindings: associam routing keys as filas.
        await fila_pedidos.bind(self.exchange, routing_key="pedido.*")
        await fila_notif.bind(self.exchange, routing_key="pedido.pago")

    async def fechar(self) -> None:
        if self.connection and not self.connection.is_closed:
            await self.connection.close()


broker = RabbitMQBroker()