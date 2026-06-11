## producer de logs de API em tempo real
## simula cinco microserviços publicando eventos no Kafka
## cada um dos eventos represnta uma requisição HTTP (mensagem pro servidor pedindo um recurso)
## isso é uma simulação de um cenário real, afim de controlarmos volume, taxa de erro e padronização dos logs
## este projeto pode ser usado em um cenário real, mas não é o caso

## Importar as bibliotecas necessárias
import json
import time
import uuid
import random
import logging
import signal
import sys
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Optional
from kafka import KafkaProducer  
from kafka.errors import KafkaError  
from faker import Faker 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
faker = Faker()

## dataclass define uma estrutura do evento de forma clara e concisa
## em produção, isso seria um pydantic model com validação de dados automática
@dataclass
class LogEvent:
    event_id:        str
    service:         str
    endpoint:        str
    method:          str
    status_code:     int
    latency_ms:      int
    level:           str
    message:         str
    user_id:         str
    ip_address:      str
    event_timestamp: str

## configuração de cada serviço simulado com comportamento realista ao mercado
## error_rate e latency_range são dois parametros que definem "personalidade" de cada serviço

SERVICES: dict = {
    "order-service": {
        "endpoints":     ["/orders", "/orders/{id}", "/orders/{id}/status", "/orders/search"],
        "methods":       ["GET", "POST", "PUT"],
        "error_rate":    0.05,        ## serviço de pedidos - taxa de erro moderada para simular problemas comuns
        "latency_range": (20, 300),
    },
    "payment-service": {
        "endpoints":     ["/payments", "/payments/{id}/process", "/refunds", "/payments/validate"],
        "methods":       ["POST", "GET"],
        "error_rate":    0.08,        ##  serviço de pagamento - taxa de erro moderada para simular problemas comuns
        "latency_range": (50, 800),
    },
    "auth-service": {
        "endpoints":     ["/login", "/logout", "/token/refresh", "/validate"],
        "methods":       ["POST", "GET"],
        "error_rate":    0.02,        ## serviço crítico — taxa de erro baixa para simular problemas comuns
        "latency_range": (5, 100),
    },
    "notification-service": {
        "endpoints":     ["/notify", "/email/send", "/sms/send", "/push/send"],
        "methods":       ["POST"],
        "error_rate":    0.15,        ## serviço com problemas — para demonstrar alertas
        "latency_range": (100, 2000),
    },
    "inventory-service": {
        "endpoints":     ["/products", "/products/{id}/stock", "/warehouse", "/reservations"],
        "methods":       ["GET", "PUT"],
        "error_rate":    0.03,      ## serviço de estoque - taxa de erro baixa para simular problemas comuns
        "latency_range": (10, 200),
    },
}

## mapeamento de status -> level de log
## replica a lógica que frameworks de observabilidade como Datadog e New Relic usam para categorizar logs

STATUS_TO_LEVEL: dict = {
    range(200, 300): "INFO",
    range(400, 500): "ERROR",
    range(500, 600): "CRITICAL",
}

## função que classifica o nível do log com base no status code e latência

def classify_level(status_code: int, latency_ms: int) -> str:
    ## latência alta com sucesso é WARNING — serviço degradado mas funcional
    if latency_ms > 1000 and status_code < 400:
        return "WARNING"
    for status_range, level in STATUS_TO_LEVEL.items():
        if status_code in status_range:
            return level
    return "INFO"

## função que constrói a mensagem do log de forma padronizada - facilita parsing como Loki e Splunk

def build_message(endpoint: str, status_code: int, latency_ms: int) -> str:
    messages = {
        500: f"Internal Server Error at {endpoint} - latency: {latency_ms}ms",
        502: f"Bad Gateway - upstream timeout on {endpoint} - latency: {latency_ms}ms",
        503: f"Service Unavailable - {endpoint} is down - latency: {latency_ms}ms",
        404: f"Resource not found - {endpoint} - latency: {latency_ms}ms",
        401: f"Unauthorized access attempt to {endpoint} - latency: {latency_ms}ms",
        429: f"Rate Limit exceeded on {endpoint} - latency: {latency_ms}ms",
    }
    if status_code in messages:
        return f"{messages[status_code]} after {latency_ms}ms"
    if latency_ms > 1000:
        return f"Slow response on {endpoint} - {latency_ms}ms"
    return f"Successful request to {endpoint} - latency: {latency_ms}ms"

def generate_event(service_name: str, config: dict) -> LogEvent:
    ## implementação da lógica para gerar eventos de log
    is_error = random.random() < config["error_rate"]
    status_code = (
        random.choice([500, 502, 503, 404, 401, 429])
        if is_error
        else random.choices([200, 201, 204], weights=[80, 15, 5], k=1)[0]
    )
    latency_ms = random.randint(*config["latency_range"])

    ## spike de latência: 2% de chance de latência 3x a 10x maior
    ## simula GC pause, lock de banco, ou chamada lenta a serviço externo
    if random.random() < 0.02:
        latency_ms *= random.randint(3, 10)

    endpoint = random.choice(config["endpoints"])
    method = random.choice(config["methods"])

    return LogEvent(
        event_id        = str(uuid.uuid4()),
        service         = service_name,
        endpoint        = endpoint,
        method          = method,
        status_code     = status_code,
        latency_ms      = latency_ms,
        level           = classify_level(status_code, latency_ms),
        message         = build_message(endpoint, status_code, latency_ms),
        user_id         = f"user_{random.randint(1, 10000):05d}",
        ip_address      = faker.ipv4_public(),
        event_timestamp = datetime.now(timezone.utc).isoformat(),
    )

## função para criar o produtor Kafka com configuração otimizada para alta taxa de eventos
def create_producer() -> KafkaProducer:
    ## configurações explicadas:
    ## acks='all' = broker confirma só após gravar em todos as replicas, in-sync
    ## retries=5 = retenta 5 vezes antes de desistir, lidando com falhas temporárias
    ## linger_ms=100 = aguarda 10ms para agrupar mensagens, em batch antes de enviar\
    ## reduz overhead de rede sem impacto perceptível na latência
    ## batch_size = tamanho máximo no batch em bytes (16KB)

    return KafkaProducer(
        ## FIX: usar 127.0.0.1 em vez de localhost — no Windows o kafka-python
        ## resolve localhost para IPv6 (::1), mas o Kafka no Docker só escuta IPv4
        bootstrap_servers= ["127.0.0.1:9092"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer = lambda k: k.encode("utf-8"),
        acks = "all",
        retries = 5,
        linger_ms = 100,
        batch_size = 16384,
        request_timeout_ms = 30000,
        ## FIX: kafka-python não consegue auto-detectar a versão em brokers Kafka 7.x
        ## sem isso lança NoBrokersAvailable durante o version probe
        api_version = (2, 5, 0),
    )

def main():
    producer = create_producer()
    topic = "api-logs"
    sent = 0
    errors = 0
    running = True

    ## captura Ctrl+C para shutdown
    def signal_handler(sig, frame):
        nonlocal running
        logger.info("Shutdown signal received, stopping producer...")
        running = False

    signal.signal(signal.SIGINT, signal_handler)
    ## FIX: SIGTERM não existe no Windows — protege para evitar ValueError
    if sys.platform != "win32":
        signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Producer inciado. Publicando ~10 eventos/segundo...")
    logger.info("Kafka UI disponível em http://localhost:8080")

    while running:
        service_name = random.choice(list(SERVICES.keys()))
        event = generate_event(service_name, SERVICES[service_name])
        payload = asdict(event)   ## dataclass -> dict para serialização JSON

        future = producer.send(topic, key=event.service, value=payload)

        try:
            ## block(timeout=1) aguarda confirmação do broker por até 1s
            ## em produção, use callbacks assíncronos para não bloquear o loop de geração de eventos

            record = future.get(timeout=1)
            sent += 1

            if sent % 50 == 0:
                logger.info(
                    f"Publicados {sent} eventos com {errors} erros (taxa de erro: {errors/sent:.2%})"
                    f"Útimo: {service_name} {event.method}"
                    f"{event.status_code} {event.latency_ms}ms"  
                )
        except KafkaError as e:
            errors += 1
            logger.error(f"Falha ao publicar evento {event.event_id}: {e}")

        time.sleep(0.1)  ## simula taxa de ~10 eventos/segundo - pode ajustar conforme necessário

    ## flush garante que mensagens em buffer local sjam enviadas antes de fechar o produtor
    producer.flush()
    producer.close()
    ## FIX: proteção contra ZeroDivisionError se o producer encerrar sem enviar nada
    taxa = f"{errors/sent:.2%}" if sent > 0 else "N/A"
    logger.info(f"Encerrado. Total enviados: {sent}, Total erros: {errors}, Taxa de erro final: {taxa}")

if __name__ == "__main__":
    main()