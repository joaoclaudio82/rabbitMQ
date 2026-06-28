"""
Rotas da API. Aqui ficam os endpoints que publicam mensagens no broker.
"""
from fastapi import APIRouter, HTTPException

from app.core.config import get_settings
from app.schemas.pedido import PedidoInput, RespostaPublicacao
from app.services.producer import producer

settings = get_settings()
router = APIRouter(prefix="/pedidos", tags=["pedidos"])


@router.post("", response_model=RespostaPublicacao, status_code=202)
async def criar_pedido(dados: PedidoInput):
    """Publica um pedido novo (routing key: pedido.criado)."""
    try:
        msg = await producer.publicar_pedido(dados, settings.routing_pedido_criado)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    return RespostaPublicacao(
        pedido_id=msg.pedido_id,
        status="enfileirado",
        mensagem="Pedido publicado na fila de pedidos.",
        routing_key=settings.routing_pedido_criado,
    )


@router.post("/{pedido_id}/pagar", response_model=RespostaPublicacao, status_code=202)
async def pagar_pedido(pedido_id: str, dados: PedidoInput):
    """
    Publica um evento de pagamento (routing key: pedido.pago).
    Cai na fila de pedidos E na de notificacoes pelo binding.
    """
    try:
        msg = await producer.publicar_pedido(dados, settings.routing_pedido_pago)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    return RespostaPublicacao(
        pedido_id=msg.pedido_id,
        status="enfileirado",
        mensagem="Evento de pagamento publicado.",
        routing_key=settings.routing_pedido_pago,
    )