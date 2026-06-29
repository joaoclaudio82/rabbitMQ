# Broker de Mensagens com RabbitMQ

API de pedidos com processamento assíncrono usando RabbitMQ, FastAPI e aio-pika.
A API publica eventos de pedido em uma exchange; um worker separado consome as
filas e processa as mensagens, de forma totalmente desacoplada.

## Arquitetura

```
[Cliente HTTP] → [FastAPI / producer] → (pedidos.exchange) → filas → [Worker / consumer]
                                                                ↓ (falha ou TTL)
                                                          (pedidos.dlq)
```

- **API (producer):** recebe pedidos via HTTP e publica na exchange.
- **RabbitMQ:** roteia as mensagens conforme a routing key (exchange tipo *topic*).
- **Worker (consumer):** consome as filas, processa e confirma (ACK).
- **DLQ:** mensagens que falham ou expiram são desviadas para inspeção.

A entrega é *at-least-once*: o broker pode reentregar uma mensagem, então o consumer
é idempotente e ignora `pedido_id` já processado.

## Tecnologias

- Python 3.12
- FastAPI + Uvicorn
- aio-pika (cliente assíncrono do RabbitMQ)
- Pydantic (validação)
- Docker e Docker Compose
- pytest (testes)

## Como rodar

### Pré-requisitos

- Docker e Docker Compose instalados.

### Subir tudo com Docker Compose

```bash
docker compose up --build
```

Isso sobe três serviços orquestrados: RabbitMQ, a API e o worker.

- API e documentação interativa: http://localhost:8000/docs
- Painel de administração do RabbitMQ: http://localhost:15672

### Configuração

As credenciais ficam em um arquivo `.env` (não versionado). Use o `.env.example`
como modelo:

```bash
cp .env.example .env
```

#### Variáveis de ambiente

| Variável | Obrigatória | Padrão | Descrição |
|----------|:-----------:|--------|-----------|
| `RABBITMQ_HOST` | sim | `localhost` | Host do RabbitMQ (`rabbitmq` dentro do Compose). |
| `RABBITMQ_PORT` | sim | `5672` | Porta AMQP. |
| `RABBITMQ_USER` | sim | `guest` | Usuário. |
| `RABBITMQ_PASSWORD` | sim | `guest` | Senha. |
| `RABBITMQ_VHOST` | sim | `/` | Virtual host. |
| `EXCHANGE_NAME` | não | `pedidos.exchange` | Exchange principal (*topic*). |
| `DLX_NAME` | não | `pedidos.dlx` | Dead-letter exchange (*fanout*). |
| `QUEUE_PEDIDOS` | não | `pedidos.fila` | Fila de pedidos. |
| `QUEUE_NOTIFICACOES` | não | `notificacoes.fila` | Fila de notificações. |
| `QUEUE_DLQ` | não | `pedidos.dlq` | Dead-letter queue. |
| `ROUTING_PEDIDO_CRIADO` | não | `pedido.criado` | Routing key de criação. |
| `ROUTING_PEDIDO_PAGO` | não | `pedido.pago` | Routing key de pagamento. |
| `PEDIDOS_TTL_MS` | não | `60000` | TTL da fila de pedidos (ms); ao expirar, vai para a DLQ. |

## Endpoints

| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/health` | Verifica a API e a conexão com o broker. |
| POST | `/pedidos` | Publica um pedido novo (routing key `pedido.criado`); o `pedido_id` é gerado. |
| POST | `/pedidos/{id}/pagar` | Publica evento de pagamento (routing key `pedido.pago`) **preservando o `id` da URL**. |

### Exemplo de requisição

```bash
curl -X POST http://localhost:8000/pedidos \
  -H "Content-Type: application/json" \
  -d '{"cliente": "Joao Claudio", "itens": [{"produto": "Teclado", "quantidade": 1, "preco_unitario": 5200.0}]}'
```

## Tratamento de erros e entrega confiável

- **Dead-letter queue (DLQ):** a fila de pedidos é declarada com `x-dead-letter-exchange`
  e `x-message-ttl`. Mensagens rejeitadas pelo consumer (`NACK` sem requeue) ou que
  excedem o TTL são desviadas para a `pedidos.dlq` em vez de descartadas.
- **Mensagem malformada (*poison message*):** JSON inválido ou fora do schema é
  rejeitado sem requeue e vai direto para a DLQ — evita loop infinito de reentrega.
- **Idempotência:** o consumer registra os `pedido_id` já processados e ignora
  reentregas (entrega *at-least-once*). O controle é em memória; em produção use
  Redis ou banco para sobreviver a reinícios.
- **Reconexão:** `connect_robust` reabre a conexão automaticamente após quedas de rede.
- **Persistência:** mensagens são publicadas como `PERSISTENT` e as filas são `durable`,
  sobrevivendo a reinícios do broker.

## Desenvolvimento local

Para rodar a API e o worker fora do container (com hot reload), mantendo só o
RabbitMQ no Docker:

```bash
# Ambiente virtual
python -m venv .venv
source .venv/Scripts/activate    # Windows (Git Bash)
# source .venv/bin/activate      # Linux / Mac
pip install -r requirements.txt

# Subir só o RabbitMQ
docker compose up -d rabbitmq

# Em terminais separados:
uvicorn app.main:app --reload        # API
python -m app.workers.consumer       # Worker
```

## Testes

A suíte roda sem RabbitMQ (broker e producer são substituídos por fakes), cobrindo:
schemas, cálculo de total, rotas HTTP (via `httpx`/ASGI), o producer e o caminho
crítico do consumer — idempotência, regra de negócio e desvio para a DLQ.

```bash
pytest
```

Os testes também rodam no CI a cada push e pull request (ver `.github/workflows/ci.yml`).

## Estrutura do projeto

```
app/
├── core/        # configuração e conexão com o broker
├── schemas/     # contratos de dados (Pydantic)
├── services/    # producer (publica mensagens)
├── routes/      # endpoints da API
├── workers/     # consumer (processa as filas)
└── main.py      # ponto de entrada da aplicação
```
