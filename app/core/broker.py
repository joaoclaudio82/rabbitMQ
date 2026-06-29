"""
Camada de conexao com o RabbitMQ usando aio-pika (cliente assincrono).
Aqui criamos a topologia: exchange, filas, dead-letter queue e bindings.

connection = o cano/fio físico ligando seu programa ao RabbitMQ
channel = um "sub-cano" dentro da conexão (você pode ter vários canais numa conexão só, é mais eficiente)
exchange = a agência central que vamos criar para enviar mensagens para as filas corretas
"""
import aio_pika
from aio_pika import ExchangeType
from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel, AbstractExchange

from app.core.config import get_settings

settings = get_settings()


class RabbitMQBroker:
    """
    Encapsula a conexao robusta (reconecta sozinha) e a declaracao da topologia.
    """
    #essa funcao aqui e chamada no main.py, quando a aplicacao inicia, para que a topologia seja criada antes de qualquer mensagem ser enviada ou recebida
    def __init__(self) -> None:
        self.connection: AbstractRobustConnection | None = None 
        self.channel: AbstractRobustChannel | None = None
        self.exchange: AbstractExchange | None = None
    
    #essa funcao conecta ao RabbitMQ e  cria a exchange, as filas e os bindings entre elas
    async def conectar(self) -> None:
   
        """Abre conexao robusta e declara toda a topologia."""
     
        self.connection = await aio_pika.connect_robust(settings.amqp_url)    #settings.amqp_url é o endereço completo de conexão com o RabbitMQ 
        self.channel = await self.connection.channel() #canal de comunicacao com o RabbitMQ, que sera usado para declarar a exchange, as filas e os bindings entre elas
        # Limita quantas mensagens nao confirmadas o consumidor pega por vez.
        await self.channel.set_qos(prefetch_count=10)

        # Exchange principal (topic permite roteamento por padrao de chave).
        self.exchange = await self.channel.declare_exchange(#declare_exchange é uma função da aio_pika que significa: crie uma exchange com essas características 
            settings.exchange_name,
            ExchangeType.TOPIC, #isso permite que a exchange envie mensagens para filas com base em padrões de roteamento, como "pedido.*" ou "pedido.pago"
            durable=True,
        )
        ''' 
            O conjunto funciona assim:

            dlx — cria a agência das cartas mortas.
            dlq — cria a caixa de correio das cartas mortas (a fila onde elas vão ficar guardadas).
            dlq.bind(dlx) — amarra a caixa na agência: "tudo que chegar nesta agência, ponha nesta caixa".
            Como o tipo é FANOUT (joga para todas as filas ligadas, sem olhar endereço), qualquer mensagem que chegue na dlx cai automaticamente na dlq. Simples e direto — não precisa de regra de endereço, porque a fila das mortas recebe tudo mesmo.
        '''
        
        # Dead-letter exchange + fila (mensagens que falham ou expiram caem aqui).
        #É para onde vão as mensagens que falharam ou venceram o tempo.
        dlx = await self.channel.declare_exchange( #aqui o dlx é uma exchange do tipo fanout, que significa que todas as mensagens enviadas para ela serão enviadas para todas as filas ligadas a ela, nesse caso a fila de dead letter
            "pedidos.dlx", ExchangeType.FANOUT, durable=True
        )
        dlq = await self.channel.declare_queue(settings.queue_dlq, durable=True)
        await dlq.bind(dlx)
        
        
        
        

        # Fila de pedidos com dead-letter configurada.
        fila_pedidos = await self.channel.declare_queue( #o declare_queue é uma função da aio_pika que significa: crie uma fila com essas características, nesse caso a fila de pedidos tem uma dead-letter exchange configurada, que significa que se uma mensagem falhar ou vencer o tempo, ela será enviada para a dlx
            #settings.queue_pedidos, #queue_pedidos é o nome da fila que vai receber as mensagens de pedidos, esse nome é definido nas variáveis de
            # ambiente e lido pelo get_settings(), por meio do settings.queue_pedidos, que é uma instância da classe Settings, que é uma subclasse da BaseSettings do 
            # Pydantic, que lê as variáveis de ambiente e as transforma 
            # em atributos da classe Settings
            durable=True,
            arguments={
                "x-dead-letter-exchange": "pedidos.dlx",
                "x-message-ttl": 60000,  # 60s; expira e vai pra DLQ
            },
        )
        
        # Fila de notificacoes.
        fila_notif = await self.channel.declare_queue(
            settings.queue_notificacoes, durable=True
        )
        #fila de pedidos e fila de notificacoes são declaradas, e depois são feitas as ligações (bindings) entre elas e a exchange, 
        # para que as mensagens sejam enviadas para as filas corretas com base nas routing keys.
        #a diferenca entre a fila de pedidos e a fila de notificacoes é que a fila de pedidos tem uma dead-letter exchange configurada,
        # que significa que se uma mensagem falhar ou vencer o tempo, ela será enviada para, já a fila de notificacoes não tem essa configuração, 
        # então as mensagens que falharem ou vencerem o tempo serão descartadas. A diferença entre a exchange de pedidos e a exchange de notificacoes 
        # é que a exchange de pedidos é do tipo topic, que significa que ela envia mensagens para filas com base em padrões de roteamento,
        # como "pedido.*" ou "pedido.pago", já a exchange de notificacoes é do tipo fanout, que significa que ela envia mensagens para todas as 
        # filas ligadas a ela, sem olhar para o endereço.
        
        # Bindings: associam routing keys as filas.
        #aqui estamos fazendo o binding da fila de pedidos com a exchange de pedidos, e da fila de notificacoes com a exchange de pedidos, ou seja, 
        # estamos dizendo que as mensagens enviadas para a exchange de pedidos com a routing key "pedido.*" serão enviadas para a fila de pedidos, 
        # e as mensagens enviadas para a exchange de pedidos com a routing key "pedido.pago" serão enviadas para a fila de notificacoes. Temos um unico exchange,
        # mas duas filas diferentes, e cada fila recebe mensagens diferentes com base na routing key. 
        # A fila de pedidos recebe todas as mensagens com a routing key "pedido.*", ou seja, todas as mensagens de pedidos criados e pagos,
        # enquanto a fila de notificacoes recebe apenas as mensagens de pedidos pagos, com a routing key "pedido.pago". 
        # Isso permite que tenhamos um fluxo de mensagens mais organizado e eficiente, onde cada fila recebe apenas as mensagens que lhe interessam.

        await fila_pedidos.bind(self.exchange, routing_key="pedido.*")
        await fila_notif.bind(self.exchange, routing_key="pedido.pago")

    async def fechar(self) -> None:
        if self.connection and not self.connection.is_closed:
            await self.connection.close()


broker = RabbitMQBroker()