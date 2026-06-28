"""Rota de saude da aplicacao e do broker."""
from fastapi import APIRouter
from app.core.broker import broker

router = APIRouter(tags=["health"])


@router.get("/health")
async def health():
    conectado = broker.connection is not None and not broker.connection.is_closed
    return {"status": "ok", "broker_conectado": conectado}