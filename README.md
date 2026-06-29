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

## Endpoints

| Método | Rota | Descrição |
|--------|------|-----------|
| GET | `/health` | Verifica a API e a conexão com o broker. |
| POST | `/pedidos` | Publica um pedido novo (routing key `pedido.criado`). |
| POST | `/pedidos/{id}/pagar` | Publica evento de pagamento (routing key `pedido.pago`). |

### Exemplo de requisição

```bash
curl -X POST http://localhost:8000/pedidos \
  -H "Content-Type: application/json" \
  -d '{"cliente": "Joao Claudio", "itens": [{"produto": "Teclado", "quantidade": 1, "preco_unitario": 5200.0}]}'
```

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

```bash
pytest -v
```

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
