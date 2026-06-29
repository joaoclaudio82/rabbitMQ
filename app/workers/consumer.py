"""
Worker consumidor: roda como processo separado, escuta as filas e processa.
Executar com:  python -m app.workers.consumer
"""
import asyncio
import json
import logging

from aio_pika.abc import AbstractIncomingMessage

from app.core.broker import broker
from app.core.config import get_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("consumer")
settings = get_settings()


async def processar_pedido(mensagem: AbstractIncomingMessage) -> None:
    """
    Callback de processamento. Usa 'async with mensagem.process()'
    para fazer ACK automatico no sucesso e NACK em caso de excecao.
    """
    async with mensagem.process(requeue=False):
        dados = json.loads(mensagem.body.decode("utf-8"))
        log.info("Pedido recebido: %s | cliente=%s | total=%.2f",
                 dados["pedido_id"], dados["cliente"], dados["total"])

        # Simula uma regra de negocio. Se total invalido, levanta erro -> vai pra DLQ.
        if dados["total"] <= 0:
            raise ValueError("Total invalido, enviando para DLQ")

        await asyncio.sleep(0.2)  # simula trabalho
        log.info("Pedido %s processado com sucesso", dados["pedido_id"])


async def processar_notificacao(mensagem: AbstractIncomingMessage) -> None:
    async with mensagem.process(requeue=False):
        dados = json.loads(mensagem.body.decode("utf-8"))
        log.info("Notificacao: enviando aviso de pagamento ao cliente %s", dados["cliente"])


async def main() -> None:
    await broker.conectar()
    assert broker.channel is not None

    fila_pedidos = await broker.channel.get_queue(settings.queue_pedidos)
    fila_notif = await broker.channel.get_queue(settings.queue_notificacoes)

    await fila_pedidos.consume(processar_pedido)
    await fila_notif.consume(processar_notificacao)

    log.info("Consumidores ativos. Aguardando mensagens... (CTRL+C para sair)")
    await asyncio.Future()  # mantem o processo vivo


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Encerrando consumidor.")