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

---

## Dúvidas de iniciante (perguntas frequentes)

Esta seção reúne dúvidas comuns que surgem ao ler o `broker.py` pela primeira
vez, com explicações detalhadas de cada uma.

### 1. Por que separar `__init__` e `conectar`? (definir `None` e só depois preencher)

A confusão é: por que no `__init__` definimos `self.connection`, `self.channel`
e `self.exchange` como `None`, e só depois, na função `conectar`, damos o
conteúdo de verdade a eles?

**A chave: `__init__` e `conectar` rodam em momentos diferentes.**

```python
broker = RabbitMQBroker()   # <-- AQUI roda o __init__ (momento 1)

# ... mais tarde, em outro lugar do programa ...

await broker.conectar()     # <-- AQUI roda o conectar (momento 2)
```

- **Momento 1** — quando você escreve `RabbitMQBroker()`, o `__init__` dispara
  **na hora**, automaticamente. É o "nascimento" do objeto.
- **Momento 2** — o `conectar` só roda **quando alguém o chama de propósito**, o
  que normalmente acontece bem depois, com o programa já de pé.

**Por que não conectar logo no `__init__`?**

1. **Abrir conexão é assíncrono (e demorado).** Envolve rede, precisa de `await`,
   pode falhar. E o `__init__` **não pode ser assíncrono** — não dá para usar
   `await` nele normalmente. Então a conexão *precisa* ficar para outra função.
2. **Controle sobre o "quando".** Criar o objeto é instantâneo e seguro. Conectar
   de verdade é algo que você quer controlar — no momento certo, tratando erros.

**O que o `__init__` faz com aqueles `None`?** Ele **reserva os lugares** (as
"gavetas") onde a conexão, o canal e a exchange vão morar — mas deixa vazios por
enquanto. Pense numa estante com 3 prateleiras etiquetadas:

```
__init__ (antes):              conectar (depois):
┌─────────────────────────┐    ┌──────────────────────────────────────┐
│  connection  →  (vazia)  │    │  connection  →  [conexão real aberta] │
│  channel     →  (vazia)  │ →  │  channel     →  [canal real aberto]   │
│  exchange    →  (vazia)  │    │  exchange    →  [exchange criada]      │
└─────────────────────────┘    └──────────────────────────────────────┘
```

É a **mesma** `self.connection` — a mesma prateleira, que antes estava vazia
(`None`) e depois recebeu o conteúdo real.

**E o `AbstractRobustConnection | None`?** É só um *type hint* ("dica de tipo"),
um bilhete explicativo que não faz nada acontecer. Lendo em português:

> "A gaveta `connection` vai guardar **ou** uma conexão de verdade
> (`AbstractRobustConnection`) **ou** nada (`None`). Por enquanto, começa com
> nada (`= None`)."

O `|` significa **"ou"** — avisa que a gaveta às vezes está vazia (antes de
conectar) e às vezes cheia (depois).

**Em uma frase:** criar o objeto é instantâneo e seguro, mas conectar na rede é
lento, assíncrono e pode falhar — então o `__init__` apenas *reserva os lugares
vazios* (`None`), e o `conectar` é quem *preenche* esses lugares no momento certo.

### 2. `connect_robust` é uma função da biblioteca `aio_pika`?

**Sim, exatamente.** Por causa do `import aio_pika` no topo do arquivo, você
consegue usar as funções da biblioteca escrevendo `aio_pika.` na frente:

```python
aio_pika.connect_robust(settings.amqp_url)
#  ↑              ↑
#  biblioteca     função que pertence a ela
```

O ponto (`.`) liga as duas coisas: "da biblioteca `aio_pika`, use a função
`connect_robust`".

Essa função **abre a conexão com o servidor RabbitMQ**. O detalhe do nome
**"robust"** é o pulo do gato: existe uma versão mais simples chamada só
`connect`, mas a `connect_robust` **se reconecta sozinha** se a conexão cair.

**Como saber se algo vem da biblioteca ou foi escrito por você?** Olhe de onde a
coisa "vem" (o nome antes do ponto):

| O que você vê | De onde vem |
|---|---|
| `aio_pika.connect_robust(...)` | da biblioteca **`aio_pika`** (instalada, não escrita por você) |
| `aio_pika.ExchangeType` | também da biblioteca `aio_pika` |
| `get_settings()` | de **outro arquivo do projeto** (`app.core.config`) — veja o `import` |
| `self.conectar(...)` | escrito por **você**, na própria classe (o `self.` denuncia) |

A biblioteca foi **instalada** no projeto (listada no `requirements.txt`). A
vantagem de usar bibliotecas: alguém já resolveu o problema difícil para você —
basta saber *o que* a função faz e chamá-la corretamente.

### 3. O que é o `settings.amqp_url`?

É o **endereço completo de conexão com o RabbitMQ** — uma única linha de texto
que junta usuário, senha, servidor e porta, no formato que o `aio_pika` lê. É
como o endereço completo de um site, mas para o RabbitMQ.

Ele é definido em [app/core/config.py](../app/core/config.py) e **montado na
hora**, juntando pedacinhos configuráveis:

```python
@property
def amqp_url(self) -> str:
    return (
        f"amqp://{self.rabbitmq_user}:{self.rabbitmq_password}@"
        f"{self.rabbitmq_host}:{self.rabbitmq_port}/"
        f"{self.rabbitmq_vhost.lstrip('/')}"
    )
```

Com os valores padrão (`admin`, `admin123`, `localhost`, `5672`), o resultado é:

```
amqp://admin:admin123@localhost:5672/
```

**Lendo pedaço por pedaço** (como o endereço de um site):

```
amqp://  admin  :  admin123  @  localhost  :  5672  /
  │        │         │           │            │
  │        │         │           │            └── porta ("porta de entrada" do servidor)
  │        │         │           └── host (em que máquina está o RabbitMQ)
  │        │         └── senha
  │        └── usuário
  └── protocolo (a "língua" do RabbitMQ: AMQP)
```

| Pedaço | Valor | O que significa |
|--------|-------|-----------------|
| **amqp://** | fixo | O "idioma" do RabbitMQ (assim como sites usam `https://`) |
| **usuário:senha** | `admin:admin123` | As credenciais para entrar |
| **host** | `localhost` | Qual computador. `localhost` = "a minha própria máquina" |
| **porta** | `5672` | A "porta de entrada" do RabbitMQ (a padrão dele) |
| **vhost** | `/` | Um "espaço separado" dentro do RabbitMQ (avançado; `/` é o padrão) |

**Por que montar assim, separado?** Se amanhã o RabbitMQ for para outro servidor,
você muda **uma linha só** (`rabbitmq_host`) e a `amqp_url` se ajusta sozinha.
Além disso, esses valores podem vir de um arquivo `.env` secreto — os valores no
código (`"admin"`, `"admin123"`) são só os **padrões** caso nada seja configurado.

**E o `@property`?** Ele faz com que `amqp_url` se comporte como um **atributo
comum** (um valor), mesmo sendo uma função que monta o texto. Por isso usamos
`settings.amqp_url` (sem parênteses) e não `settings.amqp_url()`.

### 4. Por que `self.channel =` tem `self.` e `dlx =` não tem?

Compare estas duas linhas:

```python
self.channel = await self.connection.channel()           # COM self.
dlx          = await self.channel.declare_exchange(...)   # SEM self.
```

A diferença do `self.` muda **onde** o resultado é guardado e por **quanto tempo**
ele vive.

**Regra: `self.` = permanente | sem `self.` = temporário**

- **`self.channel`** — o `self.` guarda o resultado no **próprio objeto `broker`**,
  numa das "prateleiras da estante". Continua existindo depois que a função
  `conectar` termina, e fica acessível em qualquer parte do código. O `channel`
  precisa disso porque é usado em **vários lugares** (declarar exchange, filas,
  dead-letter...).

- **`dlx`** (sem `self.`) — é uma **variável local**, um "papelzinho de rascunho"
  que existe **apenas dentro da função `conectar`** e some quando ela termina.

**Por que o `dlx` não precisa ser permanente?** Porque ele é usado **uma vez** e
nunca mais:

```python
dlx = await self.channel.declare_exchange(        # cria a dead-letter exchange
    "pedidos.dlx", ExchangeType.FANOUT, durable=True
)
dlq = await self.channel.declare_queue(settings.queue_dlq, durable=True)
await dlq.bind(dlx)                                # usa o dlx aqui... e acabou
```

Depois do `bind`, o `dlx` nunca mais é mencionado. Não há motivo para ocupar uma
prateleira permanente — um rascunho basta.

| | `self.channel` | `dlx` |
|---|---|---|
| Tem `self.`? | **Sim** | Não |
| Onde mora | na estante do objeto `broker` | só dentro da função `conectar` |
| Vive até quando | enquanto o `broker` existir | até a função `conectar` terminar |
| É usado onde | em **várias** partes do código | só **ali**, uma vez, para o `bind` |
| Analogia | prateleira etiquetada | papelzinho de rascunho |

**A pergunta que decide qual usar:** *"Vou precisar disso de novo, em outra parte
do programa? Ou só aqui mesmo, agora?"*

- "Vou precisar depois, em outros lugares" → põe `self.` (caso de `connection`,
  `channel`, `exchange`).
- "Só preciso aqui, rapidinho" → não põe `self.` (caso de `dlx`, `dlq`,
  `fila_pedidos`, `fila_notif` — todos criados e amarrados ali mesmo).

Note que até as **filas** (`fila_pedidos`, `fila_notif`) **não têm `self.`**:
depois do `bind`, o objeto Python não precisa mais segurá-las (elas continuam
existindo no servidor RabbitMQ). Já a `exchange` **tem `self.`** porque é por ela
que as mensagens serão **enviadas** mais tarde.

### 5. O que faz a linha `dlx = await self.channel.declare_exchange(...)`?

A linha completa é:

```python
dlx = await self.channel.declare_exchange(
    "pedidos.dlx", ExchangeType.FANOUT, durable=True
)
```

**Em uma frase:** ela **cria a "agência central" da fila de mensagens com
problema** — a *dead-letter exchange* (a "exchange das cartas mortas"), para onde
vão as mensagens que falharam ou venceram o tempo.

**Lendo de trás para frente** (a mesma receita das outras linhas: *fazer ação na
rede → esperar com `await` → guardar o resultado*):

1. **`self.channel.declare_exchange(...)`** — pede ao canal para **criar uma
   exchange** (criar se não existir; se já existir, apenas usar). Os 3 valores
   definem como ela é:

   | Valor | O que significa |
   |-------|----------------|
   | `"pedidos.dlx"` | O **nome** da exchange. "dlx" = **D**ead-**L**etter e**X**change |
   | `ExchangeType.FANOUT` | O **tipo**: FANOUT joga a mensagem para **todas** as filas ligadas, sem ligar para endereço |
   | `durable=True` | **Sobrevive** se o RabbitMQ reiniciar |

2. **`await`** — espera o RabbitMQ confirmar antes de seguir.
3. **`dlx = ...`** — guarda o resultado em `dlx`. Como é **sem `self.`**, é uma
   variável temporária (um "rascunho") que só existe dentro de `conectar` (veja
   a pergunta nº 4 acima).

**Por que essa exchange existe? Para onde ela joga as mensagens?** Ela trabalha
em conjunto com as duas linhas seguintes:

```python
dlx = await self.channel.declare_exchange(           # 1. cria a agência (dead-letter)
    "pedidos.dlx", ExchangeType.FANOUT, durable=True
)
dlq = await self.channel.declare_queue(settings.queue_dlq, durable=True)  # 2. cria a caixa
await dlq.bind(dlx)                                   # 3. liga a caixa na agência
```

1. **`dlx`** — cria a *agência* das cartas mortas.
2. **`dlq`** — cria a *caixa de correio* das cartas mortas (a fila onde elas ficam
   guardadas).
3. **`dlq.bind(dlx)`** — **amarra** a caixa na agência: "tudo que chegar nesta
   agência, ponha nesta caixa".

Como o tipo é **FANOUT** (joga para todas as filas ligadas, sem olhar endereço),
qualquer mensagem que chegue na `dlx` cai automaticamente na `dlq`.

**Quem manda mensagem para a `dlx`?** Não somos nós diretamente — é o **próprio
RabbitMQ**, automaticamente. A ligação está na configuração da fila de pedidos:

```python
fila_pedidos = await self.channel.declare_queue(
    settings.queue_pedidos,
    durable=True,
    arguments={
        "x-dead-letter-exchange": "pedidos.dlx",   # <-- aponta para a dlx!
        "x-message-ttl": 60000,
    },
)
```

Aquele `"x-dead-letter-exchange": "pedidos.dlx"` diz à fila de pedidos: *"se uma
mensagem aqui falhar ou vencer os 60 segundos, mande-a para a `pedidos.dlx`"*. E
como a `pedidos.dlx` está ligada à `dlq`, a mensagem acaba guardada lá, segura,
para análise posterior.

**Comparando com a exchange principal:**

| | Exchange **principal** | **Dead-letter** exchange |
|---|---|---|
| Linha | `self.exchange = ...` | `dlx = ...` |
| Tem `self.`? | **Sim** (usada para enviar depois) | Não (rascunho temporário) |
| Tipo | `TOPIC` (roteia por endereço) | `FANOUT` (joga para todas, sem endereço) |
| Para quê | tráfego normal de mensagens | mensagens com problema |

### 6. O que são as variáveis `fila_pedidos` e `fila_notif`?

São as duas **caixas de correio** (filas) onde as mensagens ficam esperando para
serem retiradas pelos consumidores. Se a *exchange* é a "agência central" que
decide para onde mandar, a **fila** é a caixa onde a carta fica guardada até
alguém buscar — como uma fila de banco: as mensagens entram e esperam a vez de
serem processadas.

As duas seguem a mesma receita das outras linhas (*ação na rede → `await` →
guardar*), usando a função `declare_queue` (*"crie a fila; se já existir, use a
que está lá"*). E repara: as duas são guardadas em variáveis **sem `self.`** —
são "rascunhos temporários" que só existem dentro de `conectar`, pois são criadas
e amarradas com `bind` logo abaixo (veja a pergunta nº 4).

**`fila_pedidos` — a caixa principal, com regras especiais:**

```python
fila_pedidos = await self.channel.declare_queue(
    settings.queue_pedidos,        # nome: "pedidos.fila"
    durable=True,
    arguments={
        "x-dead-letter-exchange": "pedidos.dlx",
        "x-message-ttl": 60000,
    },
)
```

| Configuração | O que faz |
|--------------|-----------|
| `settings.queue_pedidos` | O **nome** da fila (vem do config: `"pedidos.fila"`) |
| `durable=True` | A fila **sobrevive** se o RabbitMQ reiniciar (e as mensagens guardadas também) |
| `arguments={...}` | **Regras extras** especiais (é o que a diferencia da outra fila) |

Dentro do `arguments`:

- **`"x-dead-letter-exchange": "pedidos.dlx"`** — *"se uma mensagem aqui der
  errado, mande-a para a `pedidos.dlx`"* (a dead-letter da pergunta nº 5). É a
  rede de segurança da fila.
- **`"x-message-ttl": 60000`** — **TTL** = "tempo de vida". 60000 ms = **60
  segundos**. Se uma mensagem ficar parada mais de 60s sem processamento, ela
  "vence" e vai automaticamente para a dead-letter.

> 💡 Os nomes que começam com `x-` são "argumentos estendidos" — configurações
> especiais que o RabbitMQ entende. Não precisa decorar: o importante é saber que
> ajustam o comportamento da fila.

**`fila_notif` — a caixa de notificações, simples:**

```python
fila_notif = await self.channel.declare_queue(
    settings.queue_notificacoes, durable=True
)
```

| Configuração | O que faz |
|--------------|-----------|
| `settings.queue_notificacoes` | O **nome** da fila (vem do config: `"notificacoes.fila"`) |
| `durable=True` | **Sobrevive** se o RabbitMQ reiniciar |

Ela é mais simples (**sem `arguments`**) porque não precisa de dead-letter nem de
tempo de expiração. As notificações são tarefas mais "leves" e, neste projeto,
não receberam o mesmo tratamento de segurança dos pedidos.

**Comparando as duas:**

| | `fila_pedidos` | `fila_notif` |
|---|---|---|
| Nome real | `pedidos.fila` | `notificacoes.fila` |
| Sobrevive a reinício (`durable`)? | Sim | Sim |
| Tem dead-letter? | **Sim** (`pedidos.dlx`) | Não |
| Tem tempo de expiração (TTL)? | **Sim** (60 segundos) | Não |
| Complexidade | mais robusta, com proteções | simples |

**Por que elas foram guardadas em variáveis?** Para serem usadas logo abaixo, nos
**bindings** (as regras de entrega):

```python
await fila_pedidos.bind(self.exchange, routing_key="pedido.*")
await fila_notif.bind(self.exchange, routing_key="pedido.pago")
```

- **`fila_pedidos`** recebe tudo que começar com `pedido.` (o `*` é coringa):
  `pedido.criado`, `pedido.pago`, `pedido.cancelado`...
- **`fila_notif`** recebe **só** o `pedido.pago`.