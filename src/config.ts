// ====================================================================================================
//                              CONFIGURAÇÃO LIDA DAS VARIÁVEIS DE AMBIENTE
// ====================================================================================================
// Todo número ajustável mora aqui, com o valor inicial como padrão.

export interface AppConfig {
  port: number;
  database: {
    url: string;
    poolMax: number;
    lockTimeout: string;                     // SET LOCAL lock_timeout das transações do HTTP: alguém espera a resposta
    statementTimeout: string;                // SET LOCAL statement_timeout das transações do HTTP
    backgroundLockTimeout: string;           // o mesmo para a fila, o worker e o publisher: ninguém espera resposta, então espera mais
    backgroundStatementTimeout: string;
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
    retryBudgetMilliseconds: number;         // o limite de tentativas: quanto tempo uma mensagem com falha transitória tenta de novo no consumidor antes da DLQ
    shutdownTimeoutMilliseconds: number;     // tempo para concluir mensagens em andamento no SIGTERM
    maxInFlightMessages: number;             // quantas mensagens o consumidor segura ao mesmo tempo; a próxima busca não espera as outras terminarem
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
  backgroundLoopFailureCapMilliseconds: number;   // a maior espera de um laço de fundo depois de falhas seguidas
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
  const config: AppConfig = {
    port: readInteger("PORT", 3000),
    database: {
      url: readString("DATABASE_URL", "postgresql://wagering:wagering@localhost:5432/wagering"),
      poolMax: readInteger("DATABASE_POOL_MAX", 20),
      lockTimeout: readString("DATABASE_LOCK_TIMEOUT", "2s"),
      statementTimeout: readString("DATABASE_STATEMENT_TIMEOUT", "5s"),
      backgroundLockTimeout: readString("DATABASE_BACKGROUND_LOCK_TIMEOUT", "10s"),
      backgroundStatementTimeout: readString("DATABASE_BACKGROUND_STATEMENT_TIMEOUT", "15s"),
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
      maxReceiveCount: readInteger("SQS_MAX_RECEIVE_COUNT", 10),                              // rede de segurança: só desligamento e processo morto gastam recebimento
      waitTimeSeconds: readInteger("SQS_WAIT_TIME_SECONDS", 20),
    },
    consumer: {
      enabled: readBoolean("CONSUMER_ENABLED", true),
      name: readString("CONSUMER_NAME", "wager-transactions-consumer"),
      retryBaseMilliseconds: readInteger("CONSUMER_RETRY_BASE_MS", 2_000),
      retryCapMilliseconds: readInteger("CONSUMER_RETRY_CAP_MS", 60_000),
      retryBudgetMilliseconds: readInteger("CONSUMER_RETRY_BUDGET_MS", 300_000),             // 5 minutos desde a primeira falha
      shutdownTimeoutMilliseconds: readInteger("SHUTDOWN_IN_FLIGHT_TIMEOUT_MS", 20_000),
      maxInFlightMessages: readInteger("CONSUMER_MAX_IN_FLIGHT_MESSAGES", 10),                 // o mesmo que cabe num recebimento
    },
    pendingReference: {
      enabled: readBoolean("PENDING_WORKER_ENABLED", true),
      intervalMilliseconds: readInteger("PENDING_WORKER_INTERVAL_MS", 1_000),
      ttlMilliseconds: readInteger("PENDING_REFERENCE_TTL_MS", 600_000),
      retryBaseMilliseconds: readInteger("PENDING_RETRY_BASE_MS", 1_000),
      retryCapMilliseconds: readInteger("PENDING_RETRY_CAP_MS", 10_000),                         // a referência chegou: a pendente espera no máximo isso pela próxima tentativa
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
    backgroundLoopFailureCapMilliseconds: readInteger("BACKGROUND_LOOP_FAILURE_CAP_MS", 10_000),   // com o banco fora, no máximo 10 s entre uma tentativa e a próxima
    acceptedCurrencies: readString("ACCEPTED_CURRENCIES", "BRL").split(",").map((currency) => currency.trim()),
    ledgerPage: { defaultLimit: readInteger("LEDGER_PAGE_DEFAULT_LIMIT", 50), maxLimit: readInteger("LEDGER_PAGE_MAX_LIMIT", 100) },
    logLevel: readString("LOG_LEVEL", "info"),
  };
  assertTimeoutOrder(config);
  if (config.consumer.maxInFlightMessages < 1) throw new Error("CONSUMER_MAX_IN_FLIGHT_MESSAGES precisa ser pelo menos 1");   // com 0 o consumidor nunca buscaria nada
  return config;
}

// ----------------------------------------------------------------------------------------------------
//                       A ORDEM DOS TIMEOUTS: ESPERA DO LOCK < COMANDO < VISIBILIDADE
// ----------------------------------------------------------------------------------------------------
// O statement_timeout precisa ser maior que o lock_timeout, senão corta a espera do lock antes dela.
// E a transação da fila precisa caber na visibilidade da mensagem: se passar, a mensagem volta para a
// fila no meio do processamento e gasta um recebimento à toa (não duplica nada, mas aproxima da DLQ).
export function intervalMilliseconds(name: string, text: string): number {
  const match = /^([0-9]+)(ms|s|min)?$/.exec(text);                                       // o formato que o PostgreSQL aceita e o database.ts confere
  if (!match) throw new Error(`${name} precisa ser um intervalo como 2s ou 500ms, veio "${text}"`);
  const value = Number(match[1]);
  if (match[2] === "min") return value * 60_000;
  if (match[2] === "s") return value * 1_000;
  return value;                                                                                 // sem unidade, o PostgreSQL lê milissegundos
}

function assertTimeoutOrder(config: AppConfig): void {
  const httpLock = intervalMilliseconds("DATABASE_LOCK_TIMEOUT", config.database.lockTimeout);
  const httpStatement = intervalMilliseconds("DATABASE_STATEMENT_TIMEOUT", config.database.statementTimeout);
  const backgroundLock = intervalMilliseconds("DATABASE_BACKGROUND_LOCK_TIMEOUT", config.database.backgroundLockTimeout);
  const backgroundStatement = intervalMilliseconds("DATABASE_BACKGROUND_STATEMENT_TIMEOUT", config.database.backgroundStatementTimeout);
  const visibility = config.sqs.visibilityTimeoutSeconds * 1_000;
  if (httpLock >= httpStatement) throw new Error("DATABASE_LOCK_TIMEOUT precisa ser menor que DATABASE_STATEMENT_TIMEOUT");
  if (backgroundLock >= backgroundStatement) throw new Error("DATABASE_BACKGROUND_LOCK_TIMEOUT precisa ser menor que DATABASE_BACKGROUND_STATEMENT_TIMEOUT");
  if (backgroundStatement >= visibility) throw new Error("DATABASE_BACKGROUND_STATEMENT_TIMEOUT precisa ser menor que SQS_VISIBILITY_TIMEOUT_SECONDS");
}
