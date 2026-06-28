"""
Schemas Pydantic: validacao de entrada e formato das mensagens.
"""
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field


class StatusPedido(str, Enum):
    criado = "criado"
    pago = "pago"
    cancelado = "cancelado"


class ItemPedido(BaseModel):
    produto: str
    quantidade: int = Field(gt=0)
    preco_unitario: float = Field(gt=0)


class PedidoInput(BaseModel):
    cliente: str
    itens: list[ItemPedido]


class MensagemPedido(BaseModel):
    """Formato da mensagem que trafega na fila."""
    pedido_id: str
    cliente: str
    itens: list[ItemPedido]
    status: StatusPedido
    total: float
    criado_em: datetime


class RespostaPublicacao(BaseModel):
    pedido_id: str
    status: str
    mensagem: str
    routing_key: strqu