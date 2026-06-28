# Explicação do `broker.py` (para iniciantes)

Este documento explica, do zero e em linguagem acessível, o arquivo
[app/core/broker.py](../app/core/broker.py) — a camada que monta a estrutura
de comunicação com o RabbitMQ.

---

## Primeiro: o que é o RabbitMQ?

Imagine os **correios**. Você (um programa) quer mandar uma carta para outra
pessoa (outro programa), mas não quer ficar esperando ela receber. Então você
joga a carta numa **caixa de correio**, e os correios se viram para entregar.

O **RabbitMQ** é esse "correio" para programas. Ele recebe **mensagens** de quem
manda (o *produtor*) e entrega para quem deve receber (o *consumidor*). Isso
permite que os programas conversem sem ficar travados esperando um pelo outro.

O arquivo `broker.py` é justamente a parte que **monta a estrutura dos
correios**: cria as caixas, define as regras de entrega, etc.

---

## Os "personagens" do RabbitMQ

| Termo | Analogia dos correios |
|-------|----------------------|
| **Exchange** | A **agência central** que recebe a carta e decide para onde mandar |
| **Queue (Fila)** | A **caixa de correio** onde a carta fica esperando ser retirada |
| **Routing key** | O **endereço** escrito no envelope (ex: `pedido.pago`) |
| **Binding** | A **regra** que diz "cartas com tal endereço vão para tal caixa" |

A sacada importante: quem manda a mensagem **não escolhe a fila diretamente**.
Ele entrega para a *exchange* com um "endereço" (routing key), e a exchange usa
as *regras* (bindings) para decidir em quais filas colocar.

---

## O código, parte por parte

### O cabeçalho

```python
import aio_pika
from aio_pika import ExchangeType
```

`aio_pika` é a **biblioteca** (código pronto de terceiros) que sabe conversar com
o RabbitMQ. O "aio" significa *assíncrono* — ou seja, o programa consegue fazer
outras coisas enquanto espera o RabbitMQ responder, em vez de ficar parado.

```python
from app.core.config import get_settings
settings = get_settings()
```

Isso pega as **configurações** do projeto (nomes das filas, endereço do
RabbitMQ, etc.) de outro arquivo. Assim os nomes não ficam "chumbados" aqui no
meio do código — ficam centralizados num lugar só.

### A classe

```python
class RabbitMQBroker:
```

Uma **classe** é como uma "planta de uma casa": um molde. Aqui o molde
representa a conexão com o RabbitMQ e tudo que ela sabe fazer.

```python
def __init__(self) -> None:
    self.connection = None
    self.channel = None
    self.exchange = None
```

`__init__` é o que roda quando o objeto é **criado**. Ele só deixa três "gavetas"
vazias (`None` = vazio), que serão preenchidas depois:

- **connection** = o cano/fio físico ligando seu programa ao RabbitMQ
- **channel** = um "sub-cano" dentro da conexão (você pode ter vários canais numa
  conexão só, é mais eficiente)
- **exchange** = a agência central que vamos criar

### Conectar e montar tudo

```python
async def conectar(self) -> None:
```

O `async` confirma que essa função é assíncrona. O `await` que aparece dentro
dela significa "espere isso terminar antes de continuar, mas sem travar o resto
do programa".

```python
self.connection = await aio_pika.connect_robust(settings.amqp_url)
```

Abre a conexão. O **"robust"** é ótimo: se a conexão cair, ela **se reconecta
sozinha** automaticamente. Você não precisa se preocupar com isso.

```python
self.channel = await self.connection.channel()
await self.channel.set_qos(prefetch_count=10)
```

Cria o canal e define o `prefetch_count=10`: o consumidor pega **no máximo 10
mensagens por vez** para processar. Sem isso, um consumidor poderia "abocanhar"
milhares de mensagens de uma vez e sobrecarregar. É um controle de fluxo.

```python
self.exchange = await self.channel.declare_exchange(
    settings.exchange_name,
    ExchangeType.TOPIC,
    durable=True,
)
```

Cria a **agência central (exchange)**. Dois detalhes:

- **TOPIC**: é um tipo de exchange que roteia por *padrão* de endereço, usando
  coringas. Ex: `pedido.*` pega `pedido.pago`, `pedido.criado`, etc. (o `*` é
  "qualquer coisa nessa posição").
- **durable=True**: a exchange **sobrevive se o RabbitMQ for reiniciado**. Sem
  isso, ela sumiria.

### A Dead-Letter Queue (a "fila dos mortos")

```python
dlx = await self.channel.declare_exchange(
    "pedidos.dlx", ExchangeType.FANOUT, durable=True
)
dlq = await self.channel.declare_queue(settings.queue_dlq, durable=True)
await dlq.bind(dlx)
```

**Dead-letter** = "carta morta". É o destino das mensagens que **deram errado** —
ou falharam ao ser processadas, ou ficaram velhas demais e expiraram.

Pense num **setor de cartas extraviadas** dos correios. Em vez de jogar fora uma
mensagem problemática, o RabbitMQ a manda para essa fila especial (`dlq`), onde
você pode investigar depois o que deu errado. Isso evita perder dados
silenciosamente.

(O tipo **FANOUT** aqui simplesmente joga a mensagem para *todas* as filas
ligadas a ela, sem se importar com endereço.)

### As filas de verdade

```python
fila_pedidos = await self.channel.declare_queue(
    settings.queue_pedidos,
    durable=True,
    arguments={
        "x-dead-letter-exchange": "pedidos.dlx",
        "x-message-ttl": 60000,  # 60s; expira e vai pra DLQ
    },
)
```

A **fila de pedidos**, com duas regras especiais:

- `x-dead-letter-exchange`: "se algo der errado aqui, mande a mensagem para a
  dead-letter que criamos acima".
- `x-message-ttl: 60000`: **TTL** = "tempo de vida". 60000 milissegundos = **60
  segundos**. Se uma mensagem ficar parada na fila por mais de 60s sem ser
  processada, ela "vence" e vai automaticamente para a dead-letter.

```python
fila_notif = await self.channel.declare_queue(
    settings.queue_notificacoes, durable=True
)
```

Uma segunda fila, para **notificações**. Mais simples, sem regras especiais.

### As regras de entrega (bindings)

```python
await fila_pedidos.bind(self.exchange, routing_key="pedido.*")
await fila_notif.bind(self.exchange, routing_key="pedido.pago")
```

Aqui conectamos tudo. As regras de entrega são:

- **fila_pedidos** recebe tudo que começar com `pedido.` (graças ao coringa `*`):
  `pedido.criado`, `pedido.pago`, `pedido.cancelado`...
- **fila_notif** recebe **só** o `pedido.pago`.

Então, quando um pedido é pago (endereço `pedido.pago`), a mensagem cai nas
**duas filas** ao mesmo tempo: a de pedidos *e* a de notificações (provavelmente
para avisar o cliente "seu pagamento foi aprovado").

### Fechar

```python
async def fechar(self) -> None:
    if self.connection and not self.connection.is_closed:
        await self.connection.close()
```

Fecha a conexão de forma educada quando o programa termina — *só se* ela existir
e ainda não estiver fechada. Boa prática para não deixar conexões abertas
penduradas.

### A última linha

```python
broker = RabbitMQBroker()
```

Cria **uma instância** (um objeto real, usando aquele molde/classe) chamada
`broker`. É esse `broker` que o resto do projeto vai importar e usar para mandar
e receber mensagens.

---

## Resumindo o fluxo completo

1. Seu programa cria um **pedido** e manda a mensagem com o endereço `pedido.pago`
   para a **exchange**.
2. A exchange olha as **regras (bindings)** e percebe que `pedido.pago` casa com
   `pedido.*` **e** com `pedido.pago`.
3. A mensagem é colocada nas duas filas: **pedidos** e **notificações**.
4. Os programas consumidores retiram as mensagens dessas filas (de 10 em 10) e
   processam.
5. Se uma mensagem falhar ou passar de 60 segundos parada, ela vai para a
   **dead-letter queue**, onde fica guardada para análise.

A grande vantagem disso tudo: os programas ficam **desacoplados**. Quem cria o
pedido não precisa saber quem vai processar, nem esperar — só joga na "agência
central" e segue a vida.