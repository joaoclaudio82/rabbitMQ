"""Configurações da aplicação, lidas de variáveis de ambiente via Pydantic Settings."""
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Conexão RabbitMQ
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "guest"
    rabbitmq_password: str = "guest"
    rabbitmq_vhost: str = "/"

    # Topologia: nomes via config para não duplicar strings entre producer, broker e consumer.
    exchange_name: str = "pedidos.exchange"
    dlx_name: str = "pedidos.dlx"
    queue_pedidos: str = "pedidos.fila"
    queue_notificacoes: str = "notificacoes.fila"
    queue_dlq: str = "pedidos.dlq"

    # Routing keys
    routing_pedido_criado: str = "pedido.criado"
    routing_pedido_pago: str = "pedido.pago"

    # TTL das mensagens na fila de pedidos (ms); ao expirar, vão para a DLQ.
    pedidos_ttl_ms: int = 60000

    # API
    api_title: str = "Broker RabbitMQ API"
    api_version: str = "1.0.0"

    @property
    def amqp_url(self) -> str:
        return (
            f"amqp://{self.rabbitmq_user}:{self.rabbitmq_password}@"
            f"{self.rabbitmq_host}:{self.rabbitmq_port}/"
            f"{self.rabbitmq_vhost.lstrip('/')}"
        )


@lru_cache
def get_settings() -> Settings:
    return Settings()
