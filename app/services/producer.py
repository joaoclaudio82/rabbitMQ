"""Serviço produtor: monta a mensagem do pedido e publica na exchange."""
import uuid
from datetime import datetime, timezone

import aio_pika
from aio_pika import DeliveryMode

from app.core.broker import broker
from app.core.config import get_settings
from app.schemas.pedido import PedidoInput, MensagemPedido, StatusPedido

settings = get_settings()


class PedidoProducer:
    async def publicar_pedido(
        self, dados: PedidoInput, routing_key: str, pedido_id: str | None = None
    ) -> MensagemPedido:
        """Publica um pedido na exchange.

        Em pedido.criado o id é gerado; em eventos sobre um pedido já existente
        (ex. pagamento) o chamador passa o pedido_id para preservar a identidade.
        """
        if broker.exchange is None:
            raise RuntimeError("Broker nao conectado. Chame broker.conectar() no startup.")

        total = sum(item.quantidade * item.preco_unitario for item in dados.itens)

        mensagem = MensagemPedido(
            pedido_id=pedido_id or str(uuid.uuid4()),
            cliente=dados.cliente,
            itens=dados.itens,
            status=StatusPedido.criado if routing_key.endswith("criado") else StatusPedido.pago,
            total=total,
            criado_em=datetime.now(timezone.utc),
        )

        await broker.exchange.publish(
            aio_pika.Message(
                body=mensagem.model_dump_json().encode("utf-8"),
                content_type="application/json",
                delivery_mode=DeliveryMode.PERSISTENT,  # sobrevive a restart do broker
                message_id=mensagem.pedido_id,  # usado para deduplicação/idempotência no consumer
            ),
            routing_key=routing_key,
        )
        return mensagem


producer = PedidoProducer()
