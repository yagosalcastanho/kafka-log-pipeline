cat > /mnt/user-data/outputs/README.md << 'EOF'

# Kafka Log Pipeline

**[Português](#português) • [English](#english)**

![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat-square&logo=python)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5.0-231F20?style=flat-square&logo=apachekafka)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=flat-square&logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

---

## Português

Pipeline de streaming de logs de sistema em tempo real. Um producer simula logs de cinco microsserviços publicando eventos continuamente no Kafka. Um consumer lê esses eventos, processa cada mensagem, persiste no PostgreSQL e gera alertas automáticos para eventos críticos. Todo o ambiente roda em Docker e foi desenvolvido no Windows.

### Por que Kafka

Kafka não é uma fila de mensagens comum e chata, É um log distribuído imutável — mensagens não são deletadas após o consumo, ficam retidas pelo tempo configurado (padrão: 7 dias). Isso permite múltiplos consumers lendo o mesmo tópico de forma independente, reprocessamento de mensagens do passado e auditoria completa de eventos. É o padrão de mercado para ingestão de logs em tempo real em empresas como Netflix, Uber e iFood.

### O que foi construído

O producer gera eventos com dados realistas de cinco microsserviços: order-service, payment-service, auth-service, notification-service e inventory-service. Cada serviço tem taxa de erro, range de latência e endpoints configurados individualmente para simular comportamentos distintos. O notification-service tem 15% de taxa de erro para gerar alertas durante a demonstração. O auth-service tem 2% porque serviços críticos de autenticação devem ser estáveis.

O consumer lê as mensagens do tópico, normaliza os dados, persiste na tabela principal com commit manual de offset, e avalia cada evento contra thresholds configuráveis. Eventos com status 5xx, nível CRITICAL ou ERROR, ou latência acima de 1500ms geram alertas automáticos em uma tabela separada.

### Tecnologias

| Tecnologia         | Uso                                                                 |
| ------------------ | ------------------------------------------------------------------- |
| Apache Kafka 7.5.0 | Message broker para streaming de eventos em tempo real              |
| kafka-python       | Biblioteca Python para producer e consumer                          |
| PostgreSQL 15      | Persistência dos logs processados, alertas e métricas               |
| Kafka UI           | Interface web para inspecionar tópicos, partições e consumer groups |
| Zookeeper          | Coordenação do cluster Kafka (eleição de líder, estado dos brokers) |
| Docker Compose     | Orquestração de todos os containers com healthcheck                 |
| Python 3.11        | Linguagem do producer e consumer                                    |
| Windows            | Ambiente de desenvolvimento                                         |

### Estrutura do Projeto

```
kafka-log-pipeline/
├── producer/
│   ├── __init__.py
│   └── log_producer.py       # gera e publica eventos no Kafka
├── consumer/
│   ├── __init__.py
│   └── log_consumer.py       # processa, persiste e gera alertas
├── sql/
│   └── create_tables.sql     # schema com índices otimizados
├── docker-compose.yml
├── requirements.txt
└── README.md
```

### Fluxo do pipeline

```
Producer (Python)
    |
    | JSON serializado com key = nome do serviço
    | key garante que mensagens do mesmo serviço
    | vão para a mesma partição — ordenação preservada
    v
Kafka Broker (tópico: api-logs)
    |
    | consumer group: log-processor-group
    | múltiplos consumers dividem as partições automaticamente
    v
Consumer (Python)
    |
    |-- normaliza o evento (timestamp, tipos)
    |-- INSERT em api_logs com ON CONFLICT DO NOTHING
    |-- avalia thresholds de alerta
    |-- INSERT em alerts se necessário
    |-- commit manual do offset após sucesso
    v
PostgreSQL 15
    |-- api_logs       (todos os eventos processados)
    |-- alerts         (eventos que ultrapassaram thresholds)
    |-- service_metrics (agregações por janela de 1 minuto)
```

### Como rodar no Windows

**Pré-requisitos:**

- Docker Desktop instalado e rodando
- Python 3.11 instalado com PATH configurado
- PowerShell ou Terminal do Windows

**Passo 1 — Clona e prepara:**

```powershell
git clone https://github.com/yagosalcastanho/kafka-log-pipeline.git
cd kafka-log-pipeline
pip install -r requirements.txt
```

**Passo 2 — Sobe a infraestrutura:**

```powershell
docker compose up -d
```

Aguarda os containers ficarem saudáveis (~30 segundos):

```powershell
docker compose ps
```

Todos devem aparecer como `healthy` antes de continuar.

**Passo 3 — Terminal 1: consumer primeiro**

O consumer precisa estar pronto antes do producer para não perder mensagens:

```powershell
python consumer/log_consumer.py
```

Aguarda: `Consumer iniciado. Aguardando mensagens do tópico api-logs...`

**Passo 4 — Terminal 2: producer**

```powershell
python producer/log_producer.py
```

Você verá logs aparecendo nos dois terminais simultaneamente.

**Passo 5 — Acompanha em tempo real:**

Acesse `http://localhost:8080` para ver o Kafka UI com os tópicos, partições e consumer group.

**Encerramento:**

`Ctrl+C` em cada terminal. O producer faz `flush()` antes de fechar, garantindo que mensagens em buffer sejam enviadas. O consumer loga as estatísticas finais.

```powershell
docker compose down        # para os containers
docker compose down -v     # para os containers e apaga os dados do banco
```

### Schema do banco

```sql
api_logs         -- logs processados (event_id UNIQUE para idempotência)
alerts           -- eventos que ultrapassaram os thresholds configurados
service_metrics  -- métricas agregadas por serviço e janela de 1 minuto
```

Todos os campos de busca frequente têm índice: `service`, `level`, `status_code` e `event_timestamp`.

### Consultas úteis

```powershell
docker exec -it postgres_kafka psql -U kafka_user -d logs_db
```

```sql
-- distribuição por serviço e nível
SELECT service, level, COUNT(*) AS total
FROM api_logs
GROUP BY service, level
ORDER BY service, level;

-- alertas mais recentes
SELECT service, level, message, triggered_at
FROM alerts
ORDER BY triggered_at DESC
LIMIT 20;

-- latência média e p99 por serviço
SELECT
    service,
    COUNT(*)                                        AS total,
    ROUND(AVG(latency_ms), 0)                       AS avg_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP
        (ORDER BY latency_ms)::int                  AS p99_ms,
    SUM(CASE WHEN status_code >= 500 THEN 1
             ELSE 0 END)                            AS erros_5xx
FROM api_logs
GROUP BY service
ORDER BY avg_ms DESC;

-- taxa de erro por serviço
SELECT
    service,
    COUNT(*)                                        AS total,
    ROUND(
        SUM(CASE WHEN status_code >= 500 THEN 1
                 ELSE 0 END)::decimal / COUNT(*) * 100, 2
    )                                               AS pct_erro
FROM api_logs
GROUP BY service
ORDER BY pct_erro DESC;
```

### Conceitos demonstrados

**Particionamento por key** — o producer publica com `key=service_name`. Mensagens da mesma key sempre vão para a mesma partição, preservando a ordenação temporal por serviço. Sem key, a distribuição seria round-robin e perderia a ordenação.

**Consumer group** — `group_id="log-processor-group"` identifica o grupo. Em produção, múltiplas instâncias do consumer com o mesmo group_id dividem as partições automaticamente entre si. Adicionar uma nova instância redistribui as partições sem configuração manual — escala horizontal nativa do Kafka.

**Commit manual de offset** — `enable_auto_commit=False` desativa o commit automático. O offset só é confirmado após o processamento e a persistência bem-sucedidos. Se o consumer falhar entre processar e commitar, a mensagem será reprocessada — padrão at-least-once delivery.

**Idempotência no consumer** — `ON CONFLICT (event_id) DO NOTHING` garante que reprocessamentos não criam duplicatas. Junto com o commit manual, isso forma o padrão correto para sistemas que não podem perder nem duplicar eventos.

**Graceful shutdown** — producer e consumer capturam `SIGINT` e `SIGTERM` para encerrar de forma limpa: o producer faz `flush()` antes de fechar e o consumer loga as estatísticas finais. Sem isso, mensagens em buffer são perdidas ao encerrar com Ctrl+C.

**Healthcheck nos containers** — o `docker-compose.yml` define healthchecks para Kafka, Zookeeper e PostgreSQL. O Kafka demora ~15s para estar operacional após iniciar. Sem healthcheck, `depends_on` só espera o container iniciar, não estar pronto — o consumer tentaria conectar antes do broker estar disponível e falharia.

---

## English

Real-time system log streaming pipeline. A producer simulates logs from five microservices continuously publishing events to Kafka. A consumer reads those events, processes each message, persists to PostgreSQL and automatically generates alerts for critical events. The entire environment runs in Docker and was developed on Windows.

### Why Kafka

Kafka is not a regular and boring message queue. It is an immutable distributed log — messages are not deleted after consumption, they are retained for a configured period (default: 7 days). This allows multiple consumers to independently read the same topic, reprocess past messages and maintain a complete audit trail of events. It is the industry standard for real-time log ingestion at companies like Netflix, Uber and iFood.

### What was built

The producer generates realistic events from five microservices: order-service, payment-service, auth-service, notification-service and inventory-service. Each service has its error rate, latency range and endpoints configured individually to simulate distinct behaviors. The notification-service has a 15% error rate to generate alerts during the demo. The auth-service has 2% because critical authentication services must be stable.

The consumer reads messages from the topic, normalizes the data, persists to the main table with manual offset commit, and evaluates each event against configurable thresholds. Events with 5xx status, CRITICAL or ERROR level, or latency above 1500ms automatically generate alerts in a separate table.

### Technologies

| Technology         | Purpose                                                         |
| ------------------ | --------------------------------------------------------------- |
| Apache Kafka 7.5.0 | Message broker for real-time event streaming                    |
| kafka-python       | Python library for producer and consumer                        |
| PostgreSQL 15      | Persistence of processed logs, alerts and metrics               |
| Kafka UI           | Web interface to inspect topics, partitions and consumer groups |
| Zookeeper          | Kafka cluster coordination (leader election, broker state)      |
| Docker Compose     | Container orchestration with healthchecks                       |
| Python 3.11        | Producer and consumer language                                  |
| Windows            | Development environment                                         |

### Project Structure

```
kafka-log-pipeline/
├── producer/
│   ├── __init__.py
│   └── log_producer.py       # generates and publishes events to Kafka
├── consumer/
│   ├── __init__.py
│   └── log_consumer.py       # processes, persists and generates alerts
├── sql/
│   └── create_tables.sql     # schema with optimized indexes
├── docker-compose.yml
├── requirements.txt
└── README.md
```

### Pipeline flow

```
Producer (Python)
    |
    | serialized JSON with key = service name
    | key ensures messages from the same service
    | go to the same partition — ordering preserved
    v
Kafka Broker (topic: api-logs)
    |
    | consumer group: log-processor-group
    | multiple consumers split partitions automatically
    v
Consumer (Python)
    |
    |-- normalizes the event (timestamp, types)
    |-- INSERT into api_logs with ON CONFLICT DO NOTHING
    |-- evaluates alert thresholds
    |-- INSERT into alerts if triggered
    |-- manual offset commit after success
    v
PostgreSQL 15
    |-- api_logs        (all processed events)
    |-- alerts          (events that exceeded thresholds)
    |-- service_metrics (aggregations per 1-minute window)
```

### How to run on Windows

**Prerequisites:**

- Docker Desktop installed and running
- Python 3.11 installed with PATH configured
- PowerShell or Windows Terminal

**Step 1 — Clone and prepare:**

```powershell
git clone https://github.com/yagosalcastanho/kafka-log-pipeline.git
cd kafka-log-pipeline
pip install -r requirements.txt
```

**Step 2 — Start the infrastructure:**

```powershell
docker compose up -d
```

Wait for containers to become healthy (~30 seconds):

```powershell
docker compose ps
```

All should show as `healthy` before proceeding.

**Step 3 — Terminal 1: consumer first**

The consumer must be ready before the producer to avoid missing messages:

```powershell
python consumer/log_consumer.py
```

Wait for: `Consumer started. Waiting for messages on topic api-logs...`

**Step 4 — Terminal 2: producer**

```powershell
python producer/log_producer.py
```

You will see logs appearing in both terminals simultaneously.

**Step 5 — Monitor in real time:**

Access `http://localhost:8080` to see the Kafka UI with topics, partitions and consumer group.

**Shutdown:**

`Ctrl+C` in each terminal. The producer calls `flush()` before closing, ensuring buffered messages are sent. The consumer logs final statistics.

```powershell
docker compose down        # stop containers
docker compose down -v     # stop containers and delete database data
```

### Database schema

```sql
api_logs         -- processed logs (event_id UNIQUE for idempotency)
alerts           -- events that exceeded configured thresholds
service_metrics  -- metrics aggregated by service and 1-minute window
```

All frequently queried fields are indexed: `service`, `level`, `status_code` and `event_timestamp`.

### Useful queries

```powershell
docker exec -it postgres_kafka psql -U kafka_user -d logs_db
```

```sql
-- distribution by service and level
SELECT service, level, COUNT(*) AS total
FROM api_logs
GROUP BY service, level
ORDER BY service, level;

-- most recent alerts
SELECT service, level, message, triggered_at
FROM alerts
ORDER BY triggered_at DESC
LIMIT 20;

-- average latency and p99 by service
SELECT
    service,
    COUNT(*)                                        AS total,
    ROUND(AVG(latency_ms), 0)                       AS avg_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP
        (ORDER BY latency_ms)::int                  AS p99_ms,
    SUM(CASE WHEN status_code >= 500 THEN 1
             ELSE 0 END)                            AS errors_5xx
FROM api_logs
GROUP BY service
ORDER BY avg_ms DESC;

-- error rate by service
SELECT
    service,
    COUNT(*)                                        AS total,
    ROUND(
        SUM(CASE WHEN status_code >= 500 THEN 1
                 ELSE 0 END)::decimal / COUNT(*) * 100, 2
    )                                               AS error_pct
FROM api_logs
GROUP BY service
ORDER BY error_pct DESC;
```

### Concepts demonstrated

**Key-based partitioning** — the producer publishes with `key=service_name`. Messages with the same key always go to the same partition, preserving temporal ordering per service. Without a key, distribution would be round-robin and ordering would be lost.

**Consumer group** — `group_id="log-processor-group"` identifies the group. In production, multiple consumer instances with the same group_id automatically split partitions between them. Adding a new instance redistributes partitions without manual configuration — Kafka's native horizontal scaling.

**Manual offset commit** — `enable_auto_commit=False` disables automatic commit. The offset is only confirmed after successful processing and persistence. If the consumer fails between processing and committing, the message will be reprocessed — at-least-once delivery pattern.

**Consumer idempotency** — `ON CONFLICT (event_id) DO NOTHING` ensures reprocessing does not create duplicates. Combined with manual commit, this forms the correct pattern for systems that cannot afford to lose or duplicate events.

**Graceful shutdown** — producer and consumer capture `SIGINT` and `SIGTERM` to shut down cleanly: the producer calls `flush()` before closing and the consumer logs final statistics. Without this, buffered messages are lost when pressing Ctrl+C.

**Container healthchecks** — the `docker-compose.yml` defines healthchecks for Kafka, Zookeeper and PostgreSQL. Kafka takes ~15s to be operational after starting. Without healthchecks, `depends_on` only waits for the container to start, not to be ready — the consumer would try to connect before the broker was available and fail.

---

### Contributing | Contribuindo

Contributions are welcome. Fork the project, create a branch, commit your changes and open a pull request.
Contribuições são bem-vindas. Faça um fork, crie uma branch, commite suas alterações e abra um pull request.

### License | Licença

Distributed under the MIT License.
Distribuído sob a licença MIT.

---

<div align="center">
  Developed as a Data Engineering portfolio project<br>
  Desenvolvido como projeto de portfólio de Engenharia de Dados
</div>
EOF
