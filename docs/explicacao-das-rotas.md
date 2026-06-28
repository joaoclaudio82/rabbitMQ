# Explicação das rotas da API (`pedidos.py`) — para iniciantes

Este documento explica, do zero e em linguagem acessível, o arquivo
[app/routes/pedidos.py](../app/routes/pedidos.py) — a **API**, ou seja, a "porta
de entrada" pela qual o mundo de fora conversa com o nosso sistema.

Vale ler antes a [explicação do broker](explicacao-do-broker.md) e a
[explicação do producer](explicacao-do-producer.md), porque as rotas **usam** o
producer (que por sua vez usa o broker).

---

## O que é uma "API" e uma "rota"? (o conceito)

Imagine um **restaurante**. Você (o cliente) não entra na cozinha para cozinhar —
você fala com o **garçom**, faz um pedido pelo **cardápio**, e a cozinha faz o
resto.

Numa aplicação, a **API** é esse garçom + cardápio: é o jeito organizado de o
mundo de fora (um site, um aplicativo de celular, outro programa) **pedir coisas**
ao seu sistema, sem precisar saber como ele funciona por dentro.

Alguns termos que vão aparecer:

| Termo | O que é | Analogia do restaurante |
|-------|---------|-------------------------|
| **API** | conjunto de "pedidos" que seu sistema aceita | o cardápio inteiro |
| **Rota** / **Endpoint** | um "endereço" + ação específica da API | um prato específico do cardápio |
| **Requisição (request)** | a chamada que o cliente faz | o pedido que você faz ao garçom |
| **Resposta (response)** | o que o sistema devolve | o prato que chega à mesa |
| **Método HTTP** (GET, POST...) | o "tipo" da ação | pedir (POST) vs. consultar (GET) |

Neste arquivo, cada rota é uma "porta" que, quando acionada, **publica uma
mensagem no RabbitMQ** usando o producer.

---

## O papel deste arquivo no sistema

O fluxo completo do sistema fica assim:

```
  Cliente (site/app)
        │  faz uma requisição HTTP
        ▼
  ROTAS (pedidos.py)  ◄── você está aqui
        │  chama
        ▼
  PRODUCER (producer.py)
        │  publica
        ▼
  EXCHANGE → FILAS (broker.py)
        │
        ▼
  CONSUMIDOR (ainda não existe → app/workers/)
```

As rotas são a **camada mais externa**: o ponto onde o usuário "toca" no sistema.

---

## O código, parte por parte

### O cabeçalho (os `import`)

```python
from fastapi import APIRouter, HTTPException

from app.core.config import get_settings
from app.schemas.pedido import PedidoInput, RespostaPublicacao
from app.services.producer import producer
```

| Import | O que traz | Para quê serve |
|--------|-----------|----------------|
| `APIRouter` | ferramenta do **FastAPI** para agrupar rotas | criar o "grupo de rotas de pedidos" |
| `HTTPException` | ferramenta do FastAPI para devolver erros | avisar o cliente quando algo dá errado |
| `get_settings` | as configurações do projeto | pegar as routing keys (`pedido.criado` etc.) |
| `PedidoInput, RespostaPublicacao` | os "moldes de dados" | validar a entrada e formatar a resposta |
| `producer` | o **nosso** produtor (do `producer.py`) | de fato publicar a mensagem |

> 💡 **FastAPI** é a biblioteca que transforma funções Python comuns em uma API
> web de verdade. Você escreve funções normais e ela cuida de receber as
> requisições da internet, validar os dados e devolver as respostas.

```python
settings = get_settings()
router = APIRouter(prefix="/pedidos", tags=["pedidos"])
```

- `settings = get_settings()` — pega as configurações (mesmo padrão dos outros
  arquivos).
- `router = APIRouter(...)` — cria um **agrupador de rotas**. Dois detalhes:
  - **`prefix="/pedidos"`** — todas as rotas deste arquivo começam com
    `/pedidos`. É como dizer "todos os pratos deste arquivo estão na seção
    'pedidos' do cardápio". Assim você não precisa repetir `/pedidos` em cada
    rota.
  - **`tags=["pedidos"]`** — uma "etiqueta" só para **organizar a documentação**
    automática que o FastAPI gera. Agrupa essas rotas sob o título "pedidos".

---

## Rota 1 — Criar um pedido

```python
@router.post("", response_model=RespostaPublicacao, status_code=202)
async def criar_pedido(dados: PedidoInput):
    """Publica um pedido novo (routing key: pedido.criado)."""
    try:
        msg = await producer.publicar_pedido(dados, settings.routing_pedido_criado)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    return RespostaPublicacao(
        pedido_id=msg.pedido_id,
        status="enfileirado",
        mensagem="Pedido publicado na fila de pedidos.",
        routing_key=settings.routing_pedido_criado,
    )
```

### A linha do decorador

```python
@router.post("", response_model=RespostaPublicacao, status_code=202)
```

Essa linha que começa com `@` é um **decorador** — ele "cola" informações na
função logo abaixo, dizendo ao FastAPI: *"esta função é uma rota; eis como ela
funciona"*. Vamos destrinchar:

| Pedaço | O que significa |
|--------|-----------------|
| `@router.post` | é uma rota do tipo **POST** (POST = "criar/enviar algo"; ver abaixo) |
| `""` | o caminho é **vazio** — então, com o prefixo, o endereço final é só `/pedidos` |
| `response_model=RespostaPublicacao` | a resposta vai ter o **formato** `RespostaPublicacao` |
| `status_code=202` | quando der certo, devolve o **código 202** (ver abaixo) |

**O que é POST?** Na web, cada ação tem um "método". Os mais comuns:

| Método | Significado | Exemplo |
|--------|-------------|---------|
| **GET** | **buscar/ler** algo (não muda nada) | ver a lista de pedidos |
| **POST** | **criar/enviar** algo novo | criar um pedido |
| **PUT/PATCH** | **atualizar** algo | mudar um pedido |
| **DELETE** | **apagar** algo | cancelar um pedido |

Como aqui estamos **criando** um pedido, usamos POST.

**O que é o código 202?** Todo retorno da web vem com um número de status. Você
talvez conheça o **404** ("não encontrado"). O **202** significa **"Accepted"
(Aceito)** — uma escolha proposital e elegante aqui: ela diz *"recebi seu pedido e
coloquei na fila, mas ainda não terminei de processar"*. É perfeito para o nosso
caso, porque o sistema só **enfileira** a mensagem; quem vai processá-la de fato é
o consumidor, depois. (O `200`, mais comum, significaria "já fiz tudo", o que não
é verdade aqui.)

### A linha da função

```python
async def criar_pedido(dados: PedidoInput):
```

- `async def` — função assíncrona (não trava o programa; ver a explicação do
  producer).
- `criar_pedido` — o nome da função.
- `dados: PedidoInput` — recebe os dados do pedido, **no formato `PedidoInput`**
  (cliente + itens). O FastAPI automaticamente lê o corpo da requisição que o
  cliente enviou (em JSON), **valida** se está no formato certo e entrega aqui
  como `dados`. Se o cliente mandar algo inválido (ex: sem cliente, ou quantidade
  zero), o FastAPI **rejeita sozinho** antes mesmo de entrar na função.

A linha com `"""..."""` logo abaixo é a **docstring** — uma descrição da rota que
também aparece na documentação automática.

### O bloco `try` / `except` — lidando com erros

```python
try:
    msg = await producer.publicar_pedido(dados, settings.routing_pedido_criado)
except RuntimeError as e:
    raise HTTPException(status_code=503, detail=str(e))
```

Esse é o padrão **"tente; se der erro, faça outra coisa"**:

- **`try:`** — "tente fazer isto..." → chama o producer para publicar o pedido,
  passando os `dados` e a routing key `pedido.criado` (que vem do config).
- **`except RuntimeError as e:`** — "...mas, se acontecer um `RuntimeError`,
  capture-o na variável `e` e faça isto:" → devolve um `HTTPException` com código
  **503**.

Lembra que o producer lança um `RuntimeError` se o broker não estiver conectado?
É **exatamente esse erro** que estamos capturando aqui. Em vez de a aplicação
quebrar feio, ela responde de forma educada:

- **Código 503** = "Service Unavailable" (Serviço Indisponível) — o jeito padrão
  da web de dizer "estou temporariamente fora do ar, tente de novo depois".
- `detail=str(e)` — inclui a mensagem do erro original ("Broker nao conectado...")
  para ajudar a entender o que houve.

> 💡 Em resumo: o producer **detecta** o problema (broker desconectado) e a rota
> **traduz** esse problema para a "língua da web" (código 503), para o cliente
> receber uma resposta clara em vez de um travamento.

### O `return` — a resposta de sucesso

```python
return RespostaPublicacao(
    pedido_id=msg.pedido_id,
    status="enfileirado",
    mensagem="Pedido publicado na fila de pedidos.",
    routing_key=settings.routing_pedido_criado,
)
```

Se tudo deu certo, a rota devolve um `RespostaPublicacao` — um "comprovante"
estruturado para o cliente, com:

| Campo | Valor | O que é |
|-------|-------|---------|
| `pedido_id` | `msg.pedido_id` | o id único do pedido (gerado pelo producer) |
| `status` | `"enfileirado"` | confirma que foi colocado na fila |
| `mensagem` | texto amigável | explicação legível para humanos |
| `routing_key` | `pedido.criado` | o "endereço" usado (útil para depuração) |

---

## Rota 2 — Pagar um pedido

```python
@router.post("/{pedido_id}/pagar", response_model=RespostaPublicacao, status_code=202)
async def pagar_pedido(pedido_id: str, dados: PedidoInput):
    """
    Publica um evento de pagamento (routing key: pedido.pago).
    Cai na fila de pedidos E na de notificacoes pelo binding.
    """
    try:
        msg = await producer.publicar_pedido(dados, settings.routing_pedido_pago)
    except RuntimeError as e:
        raise HTTPException(status_code=503, detail=str(e))
    return RespostaPublicacao(
        pedido_id=msg.pedido_id,
        status="enfileirado",
        mensagem="Evento de pagamento publicado.",
        routing_key=settings.routing_pedido_pago,
    )
```

Esta rota é **quase idêntica** à primeira — a estrutura (try/except, return) é a
mesma. As diferenças importantes são duas:

### Diferença 1 — o caminho tem uma parte variável: `{pedido_id}`

```python
@router.post("/{pedido_id}/pagar", ...)
```

O endereço final desta rota é `/pedidos/{pedido_id}/pagar`. Aquele
`{pedido_id}` entre chaves é um **parâmetro de caminho** (path parameter): uma
parte do endereço que **muda** conforme o pedido. Por exemplo, se o id for `abc`,
o endereço acessado seria:

```
/pedidos/abc/pagar
```

Esse pedaço variável é capturado automaticamente e entregue à função no parâmetro
`pedido_id: str`. Repare na assinatura:

```python
async def pagar_pedido(pedido_id: str, dados: PedidoInput):
#                       └─ vem do endereço     └─ vem do corpo da requisição
```

São duas fontes de dados diferentes: o `pedido_id` vem **da URL** (do endereço), e
o `dados` vem **do corpo** da requisição (o JSON enviado). O FastAPI sabe separar
os dois sozinho.

### Diferença 2 — a routing key é `pedido.pago`

```python
msg = await producer.publicar_pedido(dados, settings.routing_pedido_pago)
```

Em vez de `pedido.criado`, esta rota publica com `pedido.pago`. E isso muda **para
onde** a mensagem vai! Como diz a própria docstring:

> "Cai na fila de pedidos **E** na de notificações pelo binding."

Lembra dos bindings no broker?

```python
await fila_pedidos.bind(self.exchange, routing_key="pedido.*")     # pega pedido.pago também
await fila_notif.bind(self.exchange,   routing_key="pedido.pago")  # pega SÓ pedido.pago
```

Por isso, ao chamar **esta** rota, a mensagem `pedido.pago` cai nas **duas filas**
ao mesmo tempo: a de pedidos (para processar) e a de notificações (para avisar o
cliente). Já a rota 1 (`pedido.criado`) cai **só** na fila de pedidos.

---

## Comparando as duas rotas

| | Rota 1 — criar | Rota 2 — pagar |
|---|---|---|
| Endereço | `POST /pedidos` | `POST /pedidos/{pedido_id}/pagar` |
| Tem parâmetro na URL? | Não | **Sim** (`pedido_id`) |
| Routing key | `pedido.criado` | `pedido.pago` |
| Cai em quais filas? | só `pedidos.fila` | `pedidos.fila` **e** `notificacoes.fila` |
| Código de sucesso | 202 (Aceito) | 202 (Aceito) |

---

## Observação importante: falta "ligar" as rotas

Por enquanto este arquivo só **define** o `router`, mas ele ainda não está
conectado a uma aplicação que roda. Para a API funcionar de verdade, normalmente
existe um arquivo `main.py` que cria a aplicação FastAPI e "inclui" este router,
mais ou menos assim:

```python
# exemplo de um main.py (ainda não existe no projeto)
from fastapi import FastAPI
from app.routes.pedidos import router as pedidos_router
from app.core.broker import broker

app = FastAPI()
app.include_router(pedidos_router)

@app.on_event("startup")
async def startup():
    await broker.conectar()   # conecta o broker quando a API sobe
```

Esse `main.py` ainda **não existe** no projeto — é provavelmente um dos próximos
passos. Repare no detalhe do `broker.conectar()` no startup: é ele que evita
aquele erro 503 que a rota trata, garantindo que o broker esteja conectado antes
de qualquer requisição chegar.

---

## Resumo de uma frase

As **rotas** são a porta de entrada da API: cada uma recebe uma requisição HTTP,
valida os dados, chama o `producer` para publicar a mensagem no RabbitMQ (com a
routing key certa), trata o erro de "broker desconectado" devolvendo 503, e
responde ao cliente com um comprovante (`RespostaPublicacao`) e o código 202
("aceito, foi para a fila").
