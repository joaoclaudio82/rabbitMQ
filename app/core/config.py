"""
Configuracoes centrais da aplicacao.
Le variaveis de ambiente via Pydantic Settings.
"""
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Conexao RabbitMQ
    rabbitmq_host: str = "localhost"
    rabbitmq_port: int = 5672
    rabbitmq_user: str = "admin"
    rabbitmq_password: str = "admin123"
    rabbitmq_vhost: str = "/"

    # Nomes de exchanges e filas, isso facilita a manutencao e evita hardcoding, existe para cada fila/exchange uma variavel de ambiente, a fila de pedidos e a de notificacoes, e uma fila de dead letter para pedidos que falharem
    #o exchange de pedidos e do tipo topic, para que possamos ter mais de uma fila consumindo a mesma mensagem, e a fila de notificacoes e do tipo fanout, para que todas as filas recebam a mesma mensagem
    exchange_name: str = "pedidos.exchange"
    exchange_type: str = "topic"

    queue_pedidos: str = "pedidos.fila"
    queue_notificacoes: str = "notificacoes.fila"
    queue_dlq: str = "pedidos.dlq"

    # Routing keys
    routing_pedido_criado: str = "pedido.criado"
    routing_pedido_pago: str = "pedido.pago"

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