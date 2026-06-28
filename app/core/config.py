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
    rabbitmq_user: str = "guest"
    rabbitmq_password: str = "guest"
    rabbitmq_vhost: str = "/"

    # Nomes de exchanges e filas, isso facilita a manutencao e evita hardcoding, existe para cada fila/exchange uma variavel de ambiente, a fila de pedidos e a de notificacoes, e uma fila de dead letter para pedidos que falharem
    #o exchange de pedidos e do tipo topic, para que possamos ter mais de uma fila consumindo a mesma mensagem, e a fila de notificacoes e do tipo fanout, para que todas as filas recebam a mesma mensagem
    exchange_name: str = "pedidos.exchange"
    exchange_type: str = "topic"
    
    #aqui queue_pedidos, tem esse nome pois e a fila que vai receber as mensagens de pedidos, 
    # queue_notificacoes e a fila que vai receber as mensagens de notificacoes, e queue_dlq e a fila que vai receber as mensagens que falharem, poderiamos ter outros nomes representando essas 
    # variaveis, pois elas sao apenas nomes de filas, mas esses nomes sao bons pois representam bem o que cada fila faz, eles serao usadas no broker.py para declarar as filas e fazer o binding com a exchange, e tambem serao usados no consumer.py para consumir as mensagens dessas filas
    queue_pedidos: str = "pedidos.fila"
    queue_notificacoes: str = "notificacoes.fila"
    queue_dlq: str = "pedidos.dlq"
    #elas sao usadas no broker.py para declarar as filas e fazer o binding com a exchange, e tambem serao usados no consumer.py para consumir as mensagens dessas filas

    # Routing keys
    #aqui routing_pedido_criado e a routing key que sera usada para enviar mensagens de pedidos criados,
    # e routing_pedido_pago e a routing key que sera usada para enviar mensagens de pedidos pagos, elas 
    # serao usadas no producer.py para publicar as mensagens na exchange com a routing key correta, e tambem serao usadas no consumer.py para consumir as mensagens dessas filas
    routing_pedido_criado: str = "pedido.criado"
    routing_pedido_pago: str = "pedido.pago"

    # API
    api_title: str = "Broker RabbitMQ API"
    api_version: str = "1.0.0"



    #@propertyele faz com que amqp_url se comporte como se fosse um atributo comum (um valor), 
    # mesmo sendo na verdade uma pequena função que monta o texto.
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