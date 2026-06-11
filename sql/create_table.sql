-- logs brutos produzidos pelo consumer
-- event_id UNIQUE garante producao do mesmo resultado independente de quantas vezes for executado
-- mesmo que o consumer processe a mesma mensagem várias vezes (indepotencia), não criaá duplicatas na tabela

CREATE TABLE IF NOT EXISTS api_logs (
    id              SERIAL PRIMARY KEY,
    event_id        VARCHAR(36)  UNIQUE NOT NULL,
    service         VARCHAR(100) NOT NULL,
    endpoint        VARCHAR(200) NOT NULL,
    method          VARCHAR(10)  NOT NULL,
    status_code     SMALLINT     NOT NULL,
    latency_ms      INTEGER      NOT NULL,
    level           VARCHAR(10)  NOT NULL,
    message         TEXT,
    user_id         VARCHAR(50),
    ip_address      VARCHAR(45),
    event_timestamp TIMESTAMPTZ  NOT NULL,    -- TIMESTAMPTZ guarda timezone, mais correto que TIMESTAMP
    processed_at    TIMESTAMPTZ  DEFAULT NOW()
);

-- indices para as queries mais comuns sistemas monitoramente
-- sem índice, SELECT com WHERE service = 'X' varre a tabela inteira 

CREATE INDEX IF NOT EXISTS idx_api_logs_service   ON api_logs (service);
CREATE INDEX IF NOT EXISTS idx_api_logs_level     ON api_logs (level);
CREATE INDEX IF NOT EXISTS idx_api_logs_timestamp ON api_logs (event_timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_api_logs_status    ON api_logs (status_code);

-- alertas gerados pelo consumer, para eventos que ultrapassam thresholds definidos (ex: latência > 500ms)

CREATE TABLE IF NOT EXISTS alerts (
    id              SERIAL PRIMARY KEY,
    event_id        VARCHAR(36)  UNIQUE NOT NULL,
    service         VARCHAR(100) NOT NULL,
    level           VARCHAR(10)  NOT NULL,
    message         TEXT,
    status_code     SMALLINT,
    latency_ms      INTEGER,
    triggered_at TIMESTAMPTZ  DEFAULT NOW()
);

-- indices para consultas de alertas, como "quais serviços estão gerando mais alertas?"
CREATE INDEX IF NOT EXISTS idx_alerts_service     ON alerts (service);
CREATE INDEX IF NOT EXISTS idx_alerts_triggered   ON alerts (triggered_at DESC);

-- métricas agregadas por serviço em janela de 1 minuto
-- alimenta dashborads sem precisar recalcular métricas a cada consulta sobre tela principal
CREATE TABLE IF NOT EXISTS service_metrics (
    id              SERIAL PRIMARY KEY,
    service         VARCHAR(100) NOT NULL,
    minute_window   TIMESTAMPTZ  NOT NULL,
    total_requests  INTEGER      NOT NULL DEFAULT 0,
    error_count     INTEGER      NOT NULL DEFAULT 0,
    avg_latency_ms  NUMERIC(10,2),
    p99_latency_ms  INTEGER,
    calculated_at   TIMESTAMPTZ  DEFAULT NOW(),
    UNIQUE (service, minute_window)    -- evita duplicata métricas da mesma janela
);