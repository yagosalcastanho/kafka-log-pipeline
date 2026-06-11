## O consumer vai ler os eventos Kafka, processar e classificar no PostgreSQL
## gerando alertas automaticos para erros críticos e monitorando a saúde dos serviços
import json
import logging
import signal
import sys
import psycopg2
from datetime import datetime
from collections import defaultdict
from confluent_kafka import Consumer, KafkaException

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)
## Configurações do PostgreSQL
DB_CONN = {
    "host":     "localhost",
    "port":     5435,
    "dbname":   "logs_db",
    "user":     "kafka_user",
    "password": "kafka_pass"
}
## tresholds para geração de alertas
## em produção, esses valores viriam de um banco ou sistema de configuração centralizado
ALERT_THRESHOLDS = {
    "latency_ms":   1500,
    "status_codes": [500, 502, 503, 504],
    "levels":       ["CRITICAL", "ERROR"]
}
def get_connection():
    return psycopg2.connect(**DB_CONN)
## função insere log processado na tabela principal
def insert_log(cursor, event: dict):
    cursor.execute("""
        INSERT INTO api_logs (
            event_id, service, endpoint, method, status_code,
            latency_ms, level, message, user_id, ip_address, event_timestamp
        ) VALUES (
            %(event_id)s, %(service)s, %(endpoint)s, %(method)s, %(status_code)s,
            %(latency_ms)s, %(level)s, %(message)s, %(user_id)s, %(ip_address)s, %(event_timestamp)s
        )
        ON CONFLICT (event_id) DO NOTHING
    """, event)
## função gera alerta para eventos criticos — em produção dispararia webhook, email ou PagerDuty
def insert_alert(cursor, event: dict):
    cursor.execute("""
        INSERT INTO alerts (event_id, service, level, message, status_code, latency_ms)
        VALUES (%(event_id)s, %(service)s, %(level)s, %(message)s, %(status_code)s, %(latency_ms)s)
    """, event)
    logger.warning(
        f"ALERT: {event['service']} {event['endpoint']} {event['method']} "
        f"status={event['status_code']} latency={event['latency_ms']}ms level={event['level']}"
    )
## função decide se o evento deve gerar um alerta
def should_alert(event: dict) -> bool:
    if event["status_code"] in ALERT_THRESHOLDS["status_codes"]:
        return True
    if event["level"] in ALERT_THRESHOLDS["levels"]:
        return True
    if event["latency_ms"] > ALERT_THRESHOLDS["latency_ms"]:
        return True
    return False
## função processa um evento, persiste no banco e gera alerta se necessário
## retorna resultado do processamento para logging
## FIX: trabalha em uma cópia do evento para não mutar o dict original
def process_event(event: dict, cursor) -> str:
    ## cria cópia para não mutar o dict original recebido pelo caller
    event = dict(event)
    ## normaliza timestamp para objeto datetime
    event["event_timestamp"] = datetime.fromisoformat(event["event_timestamp"])
    insert_log(cursor, event)
    if should_alert(event):
        insert_alert(cursor, event)
        return "WARNING"
    return "OK"
## função principal do projeto
def main():
    logger.info("Consumer iniciado. Aguardando Mensagens...")
    ## flag de controle do loop — capturada pelo signal handler para encerrar graciosamente
    running = True
    def handle_shutdown(sig, frame):
        nonlocal running
        running = False
        logger.info("Sinal de encerramento recebido, aguardando ciclo atual...")
    signal.signal(signal.SIGINT, handle_shutdown)
    ## FIX: SIGTERM não existe no Windows — protege para evitar ValueError na plataforma
    if sys.platform != "win32":
        signal.signal(signal.SIGTERM, handle_shutdown)
    ## confluent-kafka usa dicionário de configuração em vez de parâmetros nomeados
    ## group.id identifica o consumer group — múltiplos consumers dividem as partições
    ## auto.offset.reset = earliest lê desde o início na primeira execução
    ## enable.auto.commit = false — commit manual após processamento bem sucedido
    consumer = Consumer({
        ## FIX: usar 127.0.0.1 em vez de localhost — no Windows o confluent-kafka
        ## alterna entre IPv4 e IPv6 indefinidamente causando loop de FAIL no boot
        "bootstrap.servers":  "127.0.0.1:9092",
        "group.id":           "log-processor-group",
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": False,
    })
    ## inscreve o consumer no tópico api-logs
    consumer.subscribe(["api-logs"])
    ## FIX: inicializa conn e cursor como None antes do try para que o finally
    ## consiga verificar se foram criados antes de tentar fechá-los
    conn   = None
    cursor = None
    ## contadores para log de progresso
    stats           = defaultdict(int)
    processed_total = 0
    try:
        ## FIX: conexão dentro do try — se falhar, finally fecha apenas o consumer
        conn   = get_connection()
        cursor = conn.cursor()
        while running:
            ## poll com timeout 1.0s — retorna None se não há mensagens
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Erro Kafka: {msg.error()}")
                continue
            try:
                event = json.loads(msg.value().decode("utf-8"))
                result = process_event(event, cursor)
                conn.commit()
                ## FIX: commit síncrono (asynchronous=False) garante que o offset
                ## só avança após confirmação do broker — semântica at-least-once real
                consumer.commit(message=msg, asynchronous=False)
                stats[event["service"]] += 1
                stats[f"level_{event['level']}"] += 1
                processed_total += 1
                logger.info(
                    f"[{result}] {event['service']} | "
                    f"{event['method']} {event['endpoint']} | "
                    f"status: {event['status_code']} | "
                    f"latência: {event['latency_ms']}ms | "
                    f"offset: {msg.offset()}"
                )
                ## relatório de progresso a cada 100 mensagens
                if processed_total % 100 == 0:
                    logger.info(
                        f"=== Progresso: {processed_total} mensagens processadas | "
                        f"ERRORs: {stats['level_ERROR']} | "
                        f"CRITICALs: {stats['level_CRITICAL']} ==="
                    )
            ## em caso de erro no processamento, faz rollback e loga
            ## mensagem não recebe commit — será reprocessada
            except Exception as e:
                conn.rollback()
                logger.error(f"Erro ao processar mensagem: {e}")
    except KafkaException as e:
        logger.error(f"Erro fatal Kafka: {e}")
    finally:
        logger.info(f"Consumer encerrado. Total processado: {processed_total}")
        logger.info(f"Estatísticas: {dict(stats)}")
        ## FIX: fecha cursor antes de conn, e só se ambos foram inicializados
        ## encapsula cada close em try separado para garantir que todos executam
        if cursor is not None:
            try:
                cursor.close()
            except Exception as e:
                logger.error(f"Erro ao fechar cursor: {e}")
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                logger.error(f"Erro ao fechar conexão PostgreSQL: {e}")
        try:
            consumer.close()
        except Exception as e:
            logger.error(f"Erro ao fechar consumer Kafka: {e}")
if __name__ == "__main__":
    main()
