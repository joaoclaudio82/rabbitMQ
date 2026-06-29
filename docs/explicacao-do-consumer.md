# Explicação do `consumer.py` (para iniciantes)

Este documento explica, do zero e em linguagem acessível, o arquivo
[app/workers/consumer.py](../app/workers/consumer.py) — o "consumidor", ou seja,
a parte que **lê as mensagens das filas e as processa**.

Vale ler antes a [explicação do broker](explicacao-do-broker.md), a
[explicação do producer](explicacao-do-producer.md) e a
[explicação das rotas](explicacao-das-rotas.md), porque o consumer é a **última
peça** do quebra-cabeça — ele recebe o que as outras enviaram.

---

## Onde o consumer se encaixa? (a peça que faltava)

Até agora montamos toda a "ida" da mensagem:

```
  rotas → producer → exchange → FILAS
                                  │
                                  ▼
                            ???  (quem retira as mensagens?)
```

As mensagens chegavam nas filas e ficavam **paradas lá, esperando**. O
**consumer** é justamente **quem retira e processa** essas mensagens. Voltando à
analogia dos correios: se o producer é quem **posta a carta** e a fila é a **caixa
de correio**, o consumer é o **morador que abre a caixa, lê a carta e age** de
acordo com ela.

Uma diferença importante em relação aos outros arquivos: o consumer é um
**programa separado, que roda sozinho**. Ele não faz parte da API web — é um
processo à parte que fica **ligado o tempo todo**, vigiando as filas. Por isso o
comentário no topo diz como rodá-lo:

```
python -m app.workers.consumer
```

---

## O código, parte por parte

### O cabeçalho (os `import` e a configuração de log)

```python
import asyncio
import json
import logging

from aio_pika.abc import AbstractIncomingMessage

from app.core.broker import broker
from app.core.config import get_settings
```

| Import | O que traz | Para quê serve |
|--------|-----------|----------------|
| `asyncio` | o "motor" do código assíncrono do Python | rodar e manter o programa vivo |
| `json` | ferramenta para ler/escrever JSON | decodificar a mensagem recebida |
| `logging` | ferramenta para registrar mensagens (logs) | mostrar no terminal o que está acontecendo |
| `AbstractIncomingMessage` | o "tipo" de uma mensagem que chega | só uma dica de tipo para a mensagem recebida |
| `broker` | o **nosso** broker | conectar e acessar as filas |
| `get_settings` | as configurações | pegar os nomes das filas |

```python
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("consumer")
settings = get_settings()
```

Aqui configuramos os **logs** — que são mensagens que o programa escreve no
terminal para você acompanhar o que ele está fazendo. É melhor que usar `print`
porque os logs vêm com **data/hora** e **nível de importância** (INFO, ERROR...).

- `logging.basicConfig(...)` — define o formato: cada linha mostra a hora
  (`%(asctime)s`), o nível (`[INFO]`) e a mensagem.
- `log = logging.getLogger("consumer")` — cria o "registrador" que usaremos
  (chamando `log.info(...)`).

> 💡 **Log vs. print:** ambos mostram texto no terminal, mas o log é a forma
> "profissional" — organizada, com horário e níveis, e fácil de ligar/desligar.

---

## As duas funções de processamento (callbacks)

O consumer tem duas funções que são chamadas **automaticamente** sempre que uma
mensagem chega. Esse tipo de função — que você escreve mas **não chama você
mesmo** (o sistema chama por você quando algo acontece) — tem um nome: **callback**
("função de retorno").

### `processar_pedido` — o que fazer com cada pedido

```python
async def processar_pedido(mensagem: AbstractIncomingMessage) -> None:
    async with mensagem.process(requeue=False):
        dados = json.loads(mensagem.body.decode("utf-8"))
        log.info("Pedido recebido: %s | cliente=%s | total=%.2f",
                 dados["pedido_id"], dados["cliente"], dados["total"])

        # Simula uma regra de negocio. Se total invalido, levanta erro -> vai pra DLQ.
        if dados["total"] <= 0:
            raise ValueError("Total invalido, enviando para DLQ")

        await asyncio.sleep(0.2)  # simula trabalho
        log.info("Pedido %s processado com sucesso", dados["pedido_id"])
```

**A assinatura:**

```python
async def processar_pedido(mensagem: AbstractIncomingMessage) -> None:
```

- `async def` — função assíncrona (não trava o programa).
- `mensagem: AbstractIncomingMessage` — recebe a **mensagem que chegou** da fila.
- `-> None` — não devolve nada (`None` = nada). A função só "faz coisas", não
  retorna um valor.

**A linha mais importante — o `async with ... process()`:**

```python
async with mensagem.process(requeue=False):
```

Esta linha cuida automaticamente da **confirmação** da mensagem. No RabbitMQ,
depois de processar uma mensagem o consumidor precisa avisar o servidor: "terminei
com esta, pode descartá-la". Esse aviso tem dois resultados possíveis:

- **ACK** ("acknowledge" = confirmar) → "processei com sucesso, pode remover da
  fila".
- **NACK** ("negative acknowledge" = recusar) → "deu errado, NÃO processei".

O `async with mensagem.process(...)` faz isso **sozinho**, de forma inteligente:

| O que acontece dentro do bloco | O que o `process()` faz |
|--------------------------------|-------------------------|
| O código termina **sem erro** | dá **ACK** (mensagem some da fila, sucesso) |
| O código **lança um erro** | dá **NACK** (mensagem não foi processada) |

E o `requeue=False` decide o que fazer no caso de NACK: **não** devolver a
mensagem para a mesma fila (para não ficar tentando de novo eternamente). Em vez
disso, a mensagem vai para a **dead-letter queue** (a DLQ), exatamente como
configuramos no broker. Sem isso, uma mensagem com erro poderia entrar num loop
infinito.

> 💡 Em resumo: você só escreve o que fazer com a mensagem, e o
> `async with mensagem.process(requeue=False)` cuida automaticamente de confirmar
> sucesso ou mandar para a DLQ em caso de falha. É uma "rede de segurança"
> automática.

**Ler o conteúdo da mensagem:**

```python
dados = json.loads(mensagem.body.decode("utf-8"))
```

Lembra que o producer **embalou** a mensagem (objeto → JSON → bytes)? Aqui fazemos
o caminho **inverso**, para "desembrulhar":

1. `mensagem.body` → os **bytes** crus que chegaram.
2. `.decode("utf-8")` → transforma os bytes de volta em **texto** (o JSON).
3. `json.loads(...)` → transforma o texto JSON em um **dicionário Python** (uma
   estrutura com a qual dá para trabalhar, acessando `dados["cliente"]`,
   `dados["total"]`, etc.).

```
bytes  →  texto JSON  →  dicionário Python
(body)   (.decode)       (json.loads)
```

É o espelho exato do que o producer fez ao enviar.

**Registrar o que recebeu:**

```python
log.info("Pedido recebido: %s | cliente=%s | total=%.2f",
         dados["pedido_id"], dados["cliente"], dados["total"])
```

Escreve no terminal um log com os dados do pedido. Aqueles `%s` e `%.2f` são
"espaços reservados" que serão preenchidos pelos valores na ordem: `%s` = um texto
qualquer, `%.2f` = um número com 2 casas decimais (ex: `18.00`).

**A regra de negócio (e a ida para a DLQ):**

```python
if dados["total"] <= 0:
    raise ValueError("Total invalido, enviando para DLQ")
```

Aqui há uma verificação de exemplo: se o total do pedido for **zero ou negativo**
(o que não deveria acontecer), a função **lança um erro** (`raise`). E é aí que a
mágica do `process()` entra: como ocorreu um erro, ele dá **NACK**, e com
`requeue=False` a mensagem vai para a **dead-letter queue**. Ou seja, pedidos
"defeituosos" são automaticamente separados para análise, em vez de bagunçar o
sistema.

**Simular o trabalho e confirmar sucesso:**

```python
await asyncio.sleep(0.2)  # simula trabalho
log.info("Pedido %s processado com sucesso", dados["pedido_id"])
```

- `await asyncio.sleep(0.2)` → espera 0,2 segundo, **fingindo** que está fazendo
  um trabalho demorado (consultar banco, chamar outro sistema...). Num projeto
  real, aqui entraria a lógica de verdade.
- O `log.info(...)` final registra que deu tudo certo. Quando a função termina sem
  erro, o `process()` dá **ACK** e a mensagem some da fila. 

### `processar_notificacao` — o que fazer com cada notificação

```python
async def processar_notificacao(mensagem: AbstractIncomingMessage) -> None:
    async with mensagem.process(requeue=False):
        dados = json.loads(mensagem.body.decode("utf-8"))
        log.info("Notificacao: enviando aviso de pagamento ao cliente %s", dados["cliente"])
```

É a versão **simplificada** da anterior. Mesma estrutura (`process`, desembrulhar
o JSON), mas o trabalho aqui é só registrar que enviaria um aviso de pagamento ao
cliente. Num projeto real, é onde entraria o envio de e-mail, SMS ou push.

Repare que ela não tem a verificação de erro nem o `sleep` — porque notificar é
uma tarefa mais leve e simples, condizente com a fila de notificações (que também
é a fila "simples", sem dead-letter, como vimos no broker).

---

## A função `main` — onde tudo começa

```python
async def main() -> None:
    await broker.conectar()
    assert broker.channel is not None

    fila_pedidos = await broker.channel.get_queue(settings.queue_pedidos)
    fila_notif = await broker.channel.get_queue(settings.queue_notificacoes)

    await fila_pedidos.consume(processar_pedido)
    await fila_notif.consume(processar_notificacao)

    log.info("Consumidores ativos. Aguardando mensagens... (CTRL+C para sair)")
    await asyncio.Future()  # mantem o processo vivo
```

Esta é a função que "liga" o consumidor. Passo a passo:

**1. Conectar ao broker:**

```python
await broker.conectar()
```

Chama aquele mesmo `conectar()` do broker — abre a conexão e monta toda a
topologia (exchange, filas, bindings). O consumer **precisa** disso para poder
acessar as filas.

**2. Conferir que o canal existe:**

```python
assert broker.channel is not None
```

`assert` é uma verificação de segurança: "garanta que `broker.channel` não está
vazio; se estiver, pare com erro". É mais uma aparição daquele conceito do `None`
— depois do `conectar()`, o canal deveria estar preenchido, e o `assert` confirma
isso antes de prosseguir.

**3. Pegar as filas:**

```python
fila_pedidos = await broker.channel.get_queue(settings.queue_pedidos)
fila_notif = await broker.channel.get_queue(settings.queue_notificacoes)
```

Aqui usamos `get_queue` (pegar uma fila que **já existe**), diferente do
`declare_queue` do broker (que **cria**). Como o broker já criou as filas no
`conectar()`, o consumer só precisa "pegar uma referência" a elas para poder
escutá-las.

**4. Começar a consumir (o passo central):**

```python
await fila_pedidos.consume(processar_pedido)
await fila_notif.consume(processar_notificacao)
```

`consume` significa: "fique **escutando** esta fila; toda vez que chegar uma
mensagem, chame esta função para processá-la". É aqui que as duas funções
callback são **registradas**:

- toda mensagem que cair em `pedidos.fila` → chama `processar_pedido`.
- toda mensagem que cair em `notificacoes.fila` → chama `processar_notificacao`.

Repare que **você não chama** essas funções diretamente — você só diz ao RabbitMQ
"chame-as quando chegar mensagem". Por isso elas são *callbacks*.

**5. Manter o programa vivo:**

```python
log.info("Consumidores ativos. Aguardando mensagens... (CTRL+C para sair)")
await asyncio.Future()  # mantem o processo vivo
```

Um detalhe curioso mas essencial. O `consume` apenas **registra** os callbacks e
retorna na hora — ele não "trava" esperando. Se o programa terminasse aqui, o
consumidor morreria e não escutaria nada.

O `await asyncio.Future()` é um truque para **pausar para sempre** (uma "espera
que nunca termina"), mantendo o processo **vivo e escutando** indefinidamente. O
programa fica nesse ponto até você apertar **CTRL+C** para encerrar.

---

## O bloco final — rodar o programa

```python
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Encerrando consumidor.")
```

- **`if __name__ == "__main__":`** — uma forma padrão do Python de dizer "só rode
  isto se este arquivo for executado **diretamente**" (com
  `python -m app.workers.consumer`), e não quando ele for apenas importado por
  outro arquivo. É uma "trava" para o código de inicialização.
- **`asyncio.run(main())`** — liga o "motor assíncrono" e executa a função
  `main()`. É o que de fato dá partida em tudo.
- **`except KeyboardInterrupt:`** — quando você aperta **CTRL+C**, o Python gera
  um `KeyboardInterrupt`. Capturamos esse "erro" para encerrar de forma educada,
  com uma mensagem de log limpa em vez de um traceback feio na tela.

---

## O fluxo completo do consumer, em ordem

1. O programa é iniciado (`python -m app.workers.consumer`).
2. `main()` **conecta** o broker e monta a topologia.
3. Pega as duas filas e registra os callbacks com `consume`.
4. Fica **vivo**, escutando, graças ao `await asyncio.Future()`.
5. Quando chega uma mensagem:
   - O callback correspondente é chamado automaticamente.
   - `async with mensagem.process(requeue=False)` envolve o processamento.
   - A mensagem é **desembrulhada** (bytes → JSON → dicionário).
   - O trabalho é feito (ou simulado).
   - **Sucesso** → ACK (mensagem removida). **Erro** → NACK → vai para a **DLQ**.
6. Isso se repete para cada mensagem, até você apertar CTRL+C.

```
   FILA "pedidos.fila"  ──► processar_pedido   ──► ACK (ok) ou DLQ (erro)
   FILA "notificacoes.fila" ──► processar_notificacao ──► ACK
```

---

## Como ele fecha o ciclo com os outros arquivos

Agora o sistema está **completo de ponta a ponta**:

```
  Cliente HTTP
      │
      ▼
  ROTAS (pedidos.py)  ── chama ──►  PRODUCER (producer.py)
                                         │ publica
                                         ▼
                               EXCHANGE → FILAS (broker.py)
                                         │
                                         ▼
                               CONSUMER (consumer.py)  ◄── você está aqui
                                  processa e confirma (ACK) ou manda pra DLQ
```

- O **broker** monta a estrutura.
- As **rotas** recebem o pedido do mundo externo.
- O **producer** publica a mensagem.
- O **consumer** retira e processa — fechando o ciclo.

---

## Resumo de uma frase

O **consumer** é um programa separado que fica **ligado o tempo todo escutando as
filas**: quando chega uma mensagem, ele a desembrulha (bytes → JSON), executa o
trabalho e, automaticamente, confirma o sucesso (ACK) ou — se algo der errado —
manda a mensagem para a dead-letter queue (DLQ), tudo graças ao
`async with mensagem.process(requeue=False)`.
