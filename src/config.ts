// ====================================================================================================
//                              CONFIGURAÇÃO LIDA DAS VARIÁVEIS DE AMBIENTE
// ====================================================================================================
// Todo número marcado como [ajustável] na especificação mora aqui, com o valor inicial como padrão.

export interface AppConfig {
  port: number;
  database: {
    url: string;
    poolMax: number;
    lockTimeout: string;                     // SET LOCAL lock_timeout de cada transação
    statementTimeout: string;                // SET LOCAL statement_timeout de cada transação
    idleInTransactionTimeoutMilliseconds: number;
    connectTimeoutMilliseconds: number;
  };
  sqs: {
    endpoint: string;
    region: string;
    accessKeyId: string;
    secretAccessKey: string;
    clientTimeoutMilliseconds: number;
    inputQueueName: string;
    deadLetterQueueName: string;
    eventsQueueName: string;
    visibilityTimeoutSeconds: number;
    maxReceiveCount: number;
    waitTimeSeconds: number;
  };
  consumer: {
    enabled: boolean;
    name: string;                            // consumer_name da inbox
    retryBaseMilliseconds: number;
    retryCapMilliseconds: number;
    shutdownTimeoutMilliseconds: number;     // tempo para concluir mensagens em andamento no SIGTERM
  };
  pendingReference: {
    enabled: boolean;
    intervalMilliseconds: number;
    ttlMilliseconds: number;
    retryBaseMilliseconds: number;
    retryCapMilliseconds: number;
  };
  outbox: {
    enabled: boolean;
    intervalMilliseconds: number;
    batchSize: number;
    retryBaseMilliseconds: number;
    retryCapMilliseconds: number;
    stuckAfterAttempts: number;
    gaugeIntervalMilliseconds: number;
  };
  dlqGaugeIntervalMilliseconds: number;
  acceptedCurrencies: string[];
  ledgerPage: { defaultLimit: number; maxLimit: number };
  logLevel: string;
}

function readString(name: string, fallback: string): string {
  const value = process.env[name];
  return value === undefined || value === "" ? fallback : value;                               // variável vazia vale o padrão
}

function readInteger(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isInteger(parsed) || String(parsed) !== raw.trim()) throw new Error(`variável ${name} precisa ser um inteiro, veio "${raw}"`);
  return parsed;
}

function readBoolean(name: string, fallback: boolean): boolean {
  const raw = process.env[name];
  if (raw === undefined || raw === "") return fallback;
  if (raw !== "true" && raw !== "false") throw new Error(`variável ${name} precisa ser true ou false, veio "${raw}"`);
  return raw === "true";
}

export function loadConfig(): AppConfig {
  return {
    port: readInteger("PORT", 3000),
    database: {
      url: readString("DATABASE_URL", "postgresql://wagering:wagering@localhost:5432/wagering"),
      poolMax: readInteger("DATABASE_POOL_MAX", 20),
      lockTimeout: readString("DATABASE_LOCK_TIMEOUT", "2s"),
      statementTimeout: readString("DATABASE_STATEMENT_TIMEOUT", "5s"),
      idleInTransactionTimeoutMilliseconds: readInteger("DATABASE_IDLE_IN_TRANSACTION_TIMEOUT_MS", 10_000),
      connectTimeoutMilliseconds: readInteger("DATABASE_CONNECT_TIMEOUT_MS", 3_000),
    },
    sqs: {
      endpoint: readString("SQS_ENDPOINT", "http://localhost:4566"),
      region: readString("AWS_REGION", "us-east-1"),
      accessKeyId: readString("AWS_ACCESS_KEY_ID", "test"),
      secretAccessKey: readString("AWS_SECRET_ACCESS_KEY", "test"),
      clientTimeoutMilliseconds: readInteger("SQS_CLIENT_TIMEOUT_MS", 5_000),
      inputQueueName: readString("SQS_INPUT_QUEUE", "wager-transactions.fifo"),
      deadLetterQueueName: readString("SQS_DEAD_LETTER_QUEUE", "wager-transactions-dlq.fifo"),
      eventsQueueName: readString("SQS_EVENTS_QUEUE", "wagering-events.fifo"),
      visibilityTimeoutSeconds: readInteger("SQS_VISIBILITY_TIMEOUT_SECONDS", 30),
      maxReceiveCount: readInteger("SQS_MAX_RECEIVE_COUNT", 5),
      waitTimeSeconds: readInteger("SQS_WAIT_TIME_SECONDS", 20),
    },
    consumer: {
      enabled: readBoolean("CONSUMER_ENABLED", true),
      name: readString("CONSUMER_NAME", "wager-transactions-consumer"),
      retryBaseMilliseconds: readInteger("CONSUMER_RETRY_BASE_MS", 2_000),
      retryCapMilliseconds: readInteger("CONSUMER_RETRY_CAP_MS", 60_000),
      shutdownTimeoutMilliseconds: readInteger("SHUTDOWN_IN_FLIGHT_TIMEOUT_MS", 20_000),
    },
    pendingReference: {
      enabled: readBoolean("PENDING_WORKER_ENABLED", true),
      intervalMilliseconds: readInteger("PENDING_WORKER_INTERVAL_MS", 1_000),
      ttlMilliseconds: readInteger("PENDING_REFERENCE_TTL_MS", 600_000),
      retryBaseMilliseconds: readInteger("PENDING_RETRY_BASE_MS", 1_000),
      retryCapMilliseconds: readInteger("PENDING_RETRY_CAP_MS", 60_000),
    },
    outbox: {
      enabled: readBoolean("OUTBOX_PUBLISHER_ENABLED", true),
      intervalMilliseconds: readInteger("OUTBOX_INTERVAL_MS", 500),
      batchSize: readInteger("OUTBOX_BATCH_SIZE", 10),
      retryBaseMilliseconds: readInteger("OUTBOX_RETRY_BASE_MS", 1_000),
      retryCapMilliseconds: readInteger("OUTBOX_RETRY_CAP_MS", 300_000),
      stuckAfterAttempts: readInteger("OUTBOX_STUCK_AFTER_ATTEMPTS", 10),
      gaugeIntervalMilliseconds: readInteger("OUTBOX_GAUGE_INTERVAL_MS", 5_000),
    },
    dlqGaugeIntervalMilliseconds: readInteger("DLQ_GAUGE_INTERVAL_MS", 30_000),
    acceptedCurrencies: readString("ACCEPTED_CURRENCIES", "BRL").split(",").map((currency) => currency.trim()),
    ledgerPage: { defaultLimit: readInteger("LEDGER_PAGE_DEFAULT_LIMIT", 50), maxLimit: readInteger("LEDGER_PAGE_MAX_LIMIT", 100) },
    logLevel: readString("LOG_LEVEL", "info"),
  };
}
