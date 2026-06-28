"""
Ponto de entrada da API FastAPI.
O lifespan conecta no broker no startup e fecha no shutdown.
Rodar com:  uvicorn app.main:app --reload
"""
from contextlib import asynccontextmanager
from fastapi import FastAPI

from app.core.broker import broker
from app.core.config import get_settings
from app.routes import pedidos, health

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await broker.conectar()
    yield
    await broker.fechar()


app = FastAPI(title=settings.api_title, version=settings.api_version, lifespan=lifespan)
app.include_router(health.router)
app.include_router(pedidos.router)


@app.get("/")
async def raiz():
    return {"app": settings.api_title, "docs": "/docs"}