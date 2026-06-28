# Explicação do `producer.py` (para iniciantes)

Este documento explica, do zero e em linguagem acessível, o arquivo
[app/services/producer.py](../app/services/producer.py) — o "produtor", ou seja,
a parte que **monta uma mensagem de pedido e a envia** para o RabbitMQ.

Se você ainda não leu, vale começar pela
[explicação do broker](explicacao-do-broker.md), porque o producer **usa** a
estrutura (exchange, filas) que o broker montou.

---

## Onde o producer se encaixa no sistema?

Lembra da analogia dos correios? Até agora, no broker, nós **montamos a agência**
(exchange) e as **caixas de correio** (filas). Mas ninguém ainda escreveu nem
postou uma carta.

O **producer (produtor)** é justamente **quem escreve e posta a carta**. Ele:

1. Recebe os dados de um pedido.
2. Monta a mensagem no formato certo.
3. Entrega essa mensagem para a exchange (a "agência central").

A partir daí, o RabbitMQ se encarrega de levar a mensagem para as filas certas,
segundo as regras (bindings) que o broker definiu.

```
  VOCÊ (producer)  →  exchange  →  filas  →  consumidor (ainda não existe)
   "escreve e          "agência"   "caixas"   "quem lê a carta"
    posta a carta"
```

---

## O código, parte por parte

### O cabeçalho (os `import`)

```python
import uuid
from datetime import datetime, timezone

import aio_pika
from aio_pika import DeliveryMode

from app.core.broker import broker
from app.core.config import get_settings
from app.schemas.pedido import PedidoInput, MensagemPedido, StatusPedido
```

`import` significa "traga para cá uma ferramenta que está em outro lugar". Cada
linha traz algo:

| Import | O que traz | Para quê serve aqui |
|--------|-----------|---------------------|
| `uuid` | gerador de identificadores únicos | criar um `pedido_id` único para cada pedido |
| `datetime, timezone` | ferramentas de data/hora | carimbar a data de criação do pedido |
| `aio_pika` | a biblioteca do RabbitMQ | montar e enviar a mensagem |
| `DeliveryMode` | um "modo de entrega" do aio_pika | dizer que a mensagem deve ser persistente |
| `broker` | o **nosso** broker (do `broker.py`) | acessar a exchange já conectada |
| `get_settings` | as configurações do projeto | (disponível para uso) |
| `PedidoInput, MensagemPedido, StatusPedido` | os "moldes de dados" | dar formato e validar o pedido |

> 💡 Repare na diferença: `import aio_pika` traz uma **biblioteca de terceiros**
> (instalada), enquanto `from app.core.broker import broker` traz algo do
> **próprio projeto** (escrito por você). O caminho que começa com `app.` é o seu
> código.

```python
settings = get_settings()
```

Pega as configurações do projeto e guarda em `settings` (mesmo padrão do broker).

### A classe `PedidoProducer`

```python
class PedidoProducer:
```

Uma **classe** é um "molde" (já vimos isso no broker). Aqui ela agrupa tudo que
tem a ver com **produzir/publicar pedidos**. Por enquanto ela tem uma função só:
`publicar_pedido`.

### A função `publicar_pedido` — a assinatura

```python
async def publicar_pedido(self, dados: PedidoInput, routing_key: str) -> MensagemPedido:
```

Essa primeira linha é a **assinatura** — a "etiqueta" que descreve o que a função
recebe e o que devolve. Pensando numa máquina de fazer suco: o que entra
(ingredientes) e o que sai (suco).

| Pedaço | O que é | Significado |
|--------|---------|-------------|
| `async` | modificador | função assíncrona (pode usar `await`, não trava o programa) |
| `def` | palavra-chave | "estou definindo uma função" |
| `publicar_pedido` | nome | "publica um pedido" (manda para o RabbitMQ) |
| `self` | parâmetro | o próprio objeto (`producer`); automático, você não passa |
| `dados: PedidoInput` | parâmetro | o **pedido cru** a publicar (cliente + itens) |
| `routing_key: str` | parâmetro | o "endereço" da mensagem (texto), ex: `"pedido.pago"` |
| `-> MensagemPedido` | retorno | devolve a **mensagem pronta e enviada** |
| `:` | pontuação | indica que o corpo da função vem a seguir |

**Em uma frase:** "recebo um pedido cru (`dados`) e um endereço (`routing_key`),
e devolvo a mensagem já formatada e publicada (`MensagemPedido`)".

### Passo 1 — Conferir se o broker está conectado

```python
if broker.exchange is None:
    raise RuntimeError("Broker nao conectado. Chame broker.conectar() no startup.")
```

Lembra que no broker a `exchange` começa como `None` (vazia) e só vira algo real
depois do `conectar()`? Esta é a **proteção** contra usar o producer antes de
conectar.

- `if broker.exchange is None:` — "se a exchange ainda está vazia (ninguém
  conectou)..."
- `raise RuntimeError(...)` — "...**pare tudo** e mostre um erro claro explicando
  o que fazer".

`raise` significa "levantar/lançar um erro". É melhor falhar com uma mensagem
clara ("Broker não conectado") do que quebrar de um jeito confuso lá na frente.
É aquele conceito do `None` aparecendo na prática.

### Passo 2 — Calcular o total do pedido

```python
total = sum(item.quantidade * item.preco_unitario for item in dados.itens)
```

Esta linha calcula quanto custa o pedido inteiro. Lendo da forma "humana":

> "Para cada item do pedido, multiplique a quantidade pelo preço unitário; depois
> some tudo."

Por exemplo, um pedido com 2 cafés a R$5 e 1 bolo a R$8:

```
(2 × 5)  +  (1 × 8)  =  10 + 8  =  18
```

- `item.quantidade * item.preco_unitario` → o subtotal de **um** item.
- `for item in dados.itens` → repita isso para **cada** item da lista.
- `sum(...)` → some todos os subtotais.

Isso é um "list comprehension" (uma forma compacta do Python de percorrer uma
lista e calcular algo). Não precisa dominar a sintaxe agora — o importante é
entender que ela soma o valor de todos os itens.

### Passo 3 — Montar a mensagem

```python
mensagem = MensagemPedido(
    pedido_id=str(uuid.uuid4()),
    cliente=dados.cliente,
    itens=dados.itens,
    status=StatusPedido.criado if routing_key.endswith("criado") else StatusPedido.pago,
    total=total,
    criado_em=datetime.now(timezone.utc),
)
```

Aqui montamos o objeto `MensagemPedido` — a mensagem "oficial" que vai trafegar
na fila. Cada campo:

| Campo | Valor | O que é |
|-------|-------|---------|
| `pedido_id` | `str(uuid.uuid4())` | um **identificador único** gerado na hora (ver abaixo) |
| `cliente` | `dados.cliente` | o nome do cliente, copiado do pedido cru |
| `itens` | `dados.itens` | a lista de itens, copiada do pedido cru |
| `status` | `criado` **ou** `pago` | o estado do pedido, decidido pela routing_key |
| `total` | `total` | o valor que calculamos no passo 2 |
| `criado_em` | `datetime.now(timezone.utc)` | a data/hora atual, em fuso UTC |

Dois detalhes que valem explicação:

**`str(uuid.uuid4())` — o identificador único.** `uuid.uuid4()` gera um código
gigante e praticamente impossível de se repetir, tipo
`f47ac10b-58cc-4372-a567-0e02b2c3d479`. Serve como a "etiqueta de rastreamento" do
pedido — cada pedido ganha um id diferente. O `str(...)` em volta transforma esse
código em **texto**.

**O `status` com `if`/`else` numa linha só:**

```python
status=StatusPedido.criado if routing_key.endswith("criado") else StatusPedido.pago,
```

Lê-se assim: *"o status é `criado` **se** a routing_key terminar com 'criado';
**senão**, é `pago`"*. Ou seja, ele olha o "endereço" da mensagem para decidir o
estado. Se você publicar com `routing_key="pedido.criado"`, o status vira
`criado`; com qualquer outra coisa (ex: `pedido.pago`), vira `pago`.

### Passo 4 — Transformar a mensagem em "texto para envio"

```python
corpo = mensagem.model_dump_json().encode("utf-8")
```

A mensagem é um objeto Python, mas pela rede só trafegam **bytes** (a forma mais
"crua" de dados). Então convertemos em duas etapas:

1. `mensagem.model_dump_json()` → transforma o objeto em **JSON** (um texto
   padronizado, tipo `{"pedido_id": "...", "cliente": "Ana", ...}`).
2. `.encode("utf-8")` → transforma esse texto em **bytes**, prontos para viajar
   pela rede.

Pense assim: você escreveu a carta (objeto), passou ela a limpo num formato que
todo mundo entende (JSON) e a colocou num envelope lacrado para o transporte
(bytes).

### Passo 5 — Publicar (enviar) a mensagem

```python
await broker.exchange.publish(
    aio_pika.Message(
        body=corpo,
        content_type="application/json",
        delivery_mode=DeliveryMode.PERSISTENT,  # sobrevive a restart do broker
        message_id=mensagem.pedido_id,
    ),
    routing_key=routing_key,
)
```

Este é o coração da função: **entregar a mensagem para a exchange**. Vamos por
partes.

- `broker.exchange.publish(...)` → "peça à exchange para **publicar** (enviar)
  esta mensagem". É a mesma exchange que o broker criou e guardou.
- `await` → espere o envio ser confirmado antes de seguir.

Dentro, criamos um `aio_pika.Message` — o "envelope" oficial, com o conteúdo e
algumas etiquetas:

| Campo do envelope | O que significa |
|-------------------|-----------------|
| `body=corpo` | o **conteúdo** da carta (os bytes do passo 4) |
| `content_type="application/json"` | avisa ao destinatário: "isto é JSON" |
| `delivery_mode=DeliveryMode.PERSISTENT` | a mensagem é **salva em disco** — sobrevive se o RabbitMQ reiniciar (não se perde) |
| `message_id=mensagem.pedido_id` | etiqueta o envelope com o id do pedido (ajuda a rastrear) |

E, fora do envelope, o `routing_key=routing_key` é o **endereço** escrito por
fora — é o que a exchange usa para decidir em quais filas colocar a mensagem
(lembra dos bindings `pedido.*` e `pedido.pago` no broker?).

> 💡 **PERSISTENT vs. durable:** no broker usamos `durable=True` para a *fila/
> exchange* sobreviver a um reinício. Aqui usamos `PERSISTENT` para a *mensagem*
> também sobreviver. Os dois trabalham juntos: de nada adianta a caixa sobreviver
> se a carta dentro dela for perdida. Para uma mensagem realmente não se perder,
> precisa dos dois.

### Passo 6 — Devolver a mensagem

```python
return mensagem
```

`return` "entrega" um resultado para quem chamou a função. Aqui devolvemos a
`mensagem` pronta (`MensagemPedido`) — assim quem chamou pode ver o `pedido_id`
gerado, o `total` calculado, a data, etc.

### A última linha — criar o produtor

```python
producer = PedidoProducer()
```

Cria **uma instância** (um objeto real) da classe, chamada `producer`. É esse
`producer` que o resto do projeto vai importar e usar para publicar pedidos —
exatamente como o broker faz com `broker = RabbitMQBroker()`.

---

## Os "moldes de dados" (schemas)

A função usa três moldes definidos em
[app/schemas/pedido.py](../app/schemas/pedido.py). Eles são feitos com **Pydantic**,
uma biblioteca que **valida** os dados automaticamente (garante que estão no
formato certo). Pense neles como "formulários com campos obrigatórios".

### `PedidoInput` — o pedido cru (o que entra)

```python
class PedidoInput(BaseModel):
    cliente: str
    itens: list[ItemPedido]
```

É o formato do pedido que **chega** para ser publicado. Tem só dois campos: o
`cliente` (texto) e os `itens` (uma lista de `ItemPedido`).

### `ItemPedido` — cada item da lista

```python
class ItemPedido(BaseModel):
    produto: str
    quantidade: int = Field(gt=0)
    preco_unitario: float = Field(gt=0)
```

Cada item tem um `produto` (texto), uma `quantidade` (número inteiro) e um
`preco_unitario` (número com casas decimais). O `Field(gt=0)` é uma **regra de
validação**: `gt` = *greater than* (maior que). Ou seja, quantidade e preço
**precisam ser maiores que zero** — não dá para pedir 0 unidades nem com preço
negativo. Se alguém tentar, o Pydantic recusa automaticamente.

### `MensagemPedido` — a mensagem oficial (o que sai e trafega)

```python
class MensagemPedido(BaseModel):
    pedido_id: str
    cliente: str
    itens: list[ItemPedido]
    status: StatusPedido
    total: float
    criado_em: datetime
```

É o formato **completo** da mensagem que viaja na fila. Repare que é o
`PedidoInput` "turbinado": além de `cliente` e `itens`, ele ganha o `pedido_id`,
o `status`, o `total` e o `criado_em` — tudo que o producer preencheu.

### `StatusPedido` — os estados possíveis

```python
class StatusPedido(str, Enum):
    criado = "criado"
    pago = "pago"
    cancelado = "cancelado"
```

Um `Enum` ("enumeração") é uma **lista fechada de opções válidas**. O status de um
pedido só pode ser um destes três: `criado`, `pago` ou `cancelado`. Isso evita
erros de digitação (tipo escrever "pagoo") — só os valores da lista são aceitos.

---

## O fluxo completo do producer, em ordem

1. **Confere** se o broker está conectado (senão, erro claro).
2. **Calcula** o total somando todos os itens.
3. **Monta** a `MensagemPedido` (com id único, status, total, data...).
4. **Converte** a mensagem para JSON e depois para bytes.
5. **Publica** na exchange, com o envelope (persistente) e o endereço
   (routing_key).
6. **Devolve** a mensagem pronta para quem chamou.

E o que acontece depois? A exchange recebe a mensagem e, usando os bindings que o
broker definiu, coloca-a nas filas certas — de onde, um dia, um **consumidor**
(ainda não escrito, irá para [app/workers/](../app/workers/)) vai retirá-la e
processá-la.

```
producer.publicar_pedido(dados, "pedido.pago")
        │
        ▼
   [monta a MensagemPedido]
        │
        ▼
   broker.exchange.publish(...)  ──►  exchange "pedidos.exchange" (TOPIC)
                                          │
                          ┌───────────────┴───────────────┐
                          ▼                                ▼
                 fila "pedidos.fila"            fila "notificacoes.fila"
                 (binding: pedido.*)            (binding: pedido.pago)
```

---

## Resumo de uma frase

O **producer** é quem **escreve e posta a carta**: recebe um pedido cru, monta
uma mensagem completa e validada (com id, total e status), embala em JSON/bytes e
entrega para a exchange — que, pelas regras do broker, leva a mensagem até as
filas certas.
