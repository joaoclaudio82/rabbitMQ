"""Worker consumidor: processo separado que escuta as filas e processa mensagens.

Executar com:  python -m app.workers.consumer

Tratamento de erros:
- Mensagem malformada (JSON inválido / fora do schema) é rejeitada sem requeue, então
  o RabbitMQ a desvia para a DLQ via x-dead-letter-exchange. Sem isso, um "poison message"
  ficaria em loop ou derrubaria o processamento.
- Falha de regra de negócio também vai para a DLQ pelo mesmo caminho.
- Idempotência: pedidos já processados são ignorados (entregas duplicadas do broker são
  normais em at-least-once). O controle aqui é em memória — em produção use Redis/DB.
"""
import asyncio
import logging
import time

from aio_pika.abc import AbstractIncomingMessage
from pydantic import ValidationError

from app.core.broker import broker
from app.core.config import get_settings
from app.schemas.pedido import MensagemPedido

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("consumer")
settings = get_settings()


class PedidoConsumer:
    def __init__(self) -> None:
        self._processados: set[str] = set()

    def _parse(self, body: bytes) -> MensagemPedido:
        """Desserializa e valida; ValidationError aqui acaba desviando a mensagem para a DLQ."""
        try:
            return MensagemPedido.model_validate_json(body)
        except ValidationError as e:
            log.error("Mensagem malformada, rejeitando para a DLQ: %s", e)
            raise

    async def processar_corpo(self, body: bytes) -> None:
        """Regra de negócio pura (sem AMQP), o que a torna testável isoladamente."""
        pedido = self._parse(body)

        if pedido.pedido_id in self._processados:
            log.info("Pedido %s já processado, ignorando (idempotência).", pedido.pedido_id)
            return

        if pedido.total <= 0:
            raise ValueError(f"Total inválido ({pedido.total}) para o pedido {pedido.pedido_id} -> DLQ")

        inicio = time.perf_counter()
        await asyncio.sleep(0.2)  # simula trabalho
        self._processados.add(pedido.pedido_id)
        log.info(
            "Pedido %s processado em %.0fms | cliente=%s total=%.2f",
            pedido.pedido_id, (time.perf_counter() - inicio) * 1000, pedido.cliente, pedido.total,
        )

    async def processar_pedido(self, mensagem: AbstractIncomingMessage) -> None:
        # process(requeue=False): ACK no sucesso, NACK sem requeue na exceção -> DLQ.
        async with mensagem.process(requeue=False):
            await self.processar_corpo(mensagem.body)

    async def processar_notificacao(self, mensagem: AbstractIncomingMessage) -> None:
        async with mensagem.process(requeue=False):
            pedido = self._parse(mensagem.body)
            log.info("Notificação: aviso de pagamento enviado ao cliente %s", pedido.cliente)


consumer = PedidoConsumer()


async def main() -> None:
    await broker.conectar()
    assert broker.channel is not None

    fila_pedidos = await broker.channel.get_queue(settings.queue_pedidos)
    fila_notif = await broker.channel.get_queue(settings.queue_notificacoes)

    await fila_pedidos.consume(consumer.processar_pedido)
    await fila_notif.consume(consumer.processar_notificacao)

    log.info("Consumidores ativos. Aguardando mensagens... (CTRL+C para sair)")
    try:
        await asyncio.Future()  # mantém o processo vivo
    finally:
        await broker.fechar()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Encerrando consumidor.")
