// ====================================================================================================
//            PEÇAS DOS TESTES DE INTEGRAÇÃO: CONFIG, FILAS ÚNICAS, APP EM PROCESSO, ESPERAS
// ====================================================================================================
// Banco e fila são sempre os reais do docker compose; o que muda entre testes é só a composição.
import { SQL } from "bun";
import { ReceiveMessageCommand, DeleteMessageCommand, SendMessageCommand, type SQSClient } from "@aws-sdk/client-sqs";
import type { INestApplication } from "@nestjs/common";
import { composeApplication, startBackgroundWork, stopBackgroundWork, type Application, type CompositionOverrides } from "../../src/composition";
import { loadConfig, type AppConfig } from "../../src/config";
import { createHttpApp } from "../../src/infrastructure/http/http-app";
import { createQueues } from "../../src/infrastructure/messaging/create-queues";
import { createSqsClient } from "../../src/infrastructure/messaging/sqs";

// ----------------------------------------------------------------------------------------------------
//                                ALEATÓRIO COM SEMENTE FIXA (42)
// ----------------------------------------------------------------------------------------------------
export function seededRandom(seed = 42): () => number {
  let state = seed >>> 0;
  return () => {                                                                                // mulberry32: pequeno e reprodutível
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = Math.imul(state ^ (state >>> 15), 1 | state);
    mixed = (mixed + Math.imul(mixed ^ (mixed >>> 7), 61 | mixed)) ^ mixed;
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 4294967296;
  };
}

// ----------------------------------------------------------------------------------------------------
//                                  ESPERA POR CONDIÇÃO, NUNCA SLEEP FIXO
// ----------------------------------------------------------------------------------------------------
export async function waitUntil<T>(description: string, condition: () => Promise<T | undefined | false> | T | undefined | false, timeoutMilliseconds = 20_000, pollMilliseconds = 100): Promise<T> {
  const deadline = Date.now() + timeoutMilliseconds;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      const value = await condition();
      if (value !== undefined && value !== false) return value as T;
    } catch (error) {
      lastError = error;
    }
    await Bun.sleep(pollMilliseconds);
  }
  throw new Error(`esperei ${timeoutMilliseconds} ms e não aconteceu: ${description}${lastError ? ` (último erro: ${String(lastError)})` : ""}`);
}

// ----------------------------------------------------------------------------------------------------
//                                CONFIG E FILAS ÚNICAS POR ARQUIVO
// ----------------------------------------------------------------------------------------------------
export interface TestQueues {
  inputQueueName: string;
  deadLetterQueueName: string;
  eventsQueueName: string;
}

export function uniqueSuffix(): string {
  return `${Date.now().toString(36)}${Math.floor(Math.random() * 1e6).toString(36)}`;
}

export const TEST_QUEUE_SETTINGS = { visibilityTimeoutSeconds: 3, maxReceiveCount: 3, waitTimeSeconds: 1 };   // curtos para o teste não demorar

export async function createTestQueues(prefix: string, settings = TEST_QUEUE_SETTINGS): Promise<TestQueues> {
  const suffix = uniqueSuffix();
  const queues = { inputQueueName: `${prefix}-in-${suffix}.fifo`, deadLetterQueueName: `${prefix}-dlq-${suffix}.fifo`, eventsQueueName: `${prefix}-ev-${suffix}.fifo` };
  await createQueues(testSqsClient(), queues, settings);
  return queues;
}

// valores curtos para TTL, visibilidade e backoff, como a seção 13 pede
export function testEnvironment(queues: TestQueues, extra: Record<string, string> = {}): Record<string, string> {
  return {
    DATABASE_URL: process.env.DATABASE_URL ?? "",
    SQS_ENDPOINT: process.env.SQS_ENDPOINT ?? "http://localhost:4566",
    SQS_INPUT_QUEUE: queues.inputQueueName,
    SQS_DEAD_LETTER_QUEUE: queues.deadLetterQueueName,
    SQS_EVENTS_QUEUE: queues.eventsQueueName,
    SQS_WAIT_TIME_SECONDS: "1",
    SQS_VISIBILITY_TIMEOUT_SECONDS: "3",
    SQS_MAX_RECEIVE_COUNT: "3",
    DATABASE_BACKGROUND_LOCK_TIMEOUT: "2s",                                                     // abaixo da visibilidade de 3 s dos testes
    DATABASE_BACKGROUND_STATEMENT_TIMEOUT: "2500ms",
    CONSUMER_RETRY_BASE_MS: "200",
    CONSUMER_RETRY_CAP_MS: "1000",
    CONSUMER_RETRY_BUDGET_MS: "10000",                                                          // 5 min em produção
    SHUTDOWN_IN_FLIGHT_TIMEOUT_MS: "5000",
    PENDING_WORKER_INTERVAL_MS: "200",
    PENDING_REFERENCE_TTL_MS: "4000",
    PENDING_RETRY_BASE_MS: "100",
    PENDING_RETRY_CAP_MS: "500",
    OUTBOX_INTERVAL_MS: "100",
    OUTBOX_RETRY_BASE_MS: "100",
    OUTBOX_RETRY_CAP_MS: "1000",
    OUTBOX_GAUGE_INTERVAL_MS: "500",
    DLQ_GAUGE_INTERVAL_MS: "500",
    DATABASE_POOL_MAX: "10",
    ACCEPTED_CURRENCIES: "BRL,USD",
    LOG_LEVEL: "warn",
    ...extra,
  };
}

export function testConfig(queues: TestQueues, extra: Record<string, string> = {}): AppConfig {
  const environment = testEnvironment(queues, extra);
  const previous = { ...process.env };
  Object.assign(process.env, environment);
  try {
    return loadConfig();
  } finally {
    for (const key of Object.keys(environment)) {
      if (previous[key] === undefined) delete process.env[key];
      else process.env[key] = previous[key];
    }
  }
}

let sharedSqsClient: SQSClient | undefined;
export function testSqsClient(): SQSClient {
  sharedSqsClient ??= createSqsClient({ endpoint: process.env.SQS_ENDPOINT ?? "http://localhost:4566", region: "us-east-1", accessKeyId: "test", secretAccessKey: "test", clientTimeoutMilliseconds: 30_000 });
  return sharedSqsClient;
}

let sharedSql: SQL | undefined;
export function testSql(): SQL {
  sharedSql ??= new SQL(process.env.DATABASE_URL ?? "");                                        // SQL direto, fora do ORM da aplicação
  return sharedSql;
}

// ----------------------------------------------------------------------------------------------------
//                                    APLICAÇÃO NO MESMO PROCESSO
// ----------------------------------------------------------------------------------------------------
export interface RunningApp {
  application: Application;
  httpApp: INestApplication;
  baseUrl: string;
  stop(): Promise<void>;
}

export async function startInProcessApp(config: AppConfig, overrides: CompositionOverrides = {}, options: { background?: boolean } = {}): Promise<RunningApp> {
  const application = await composeApplication(config, overrides);
  const httpApp = await createHttpApp(application);
  await httpApp.listen(0);                                                                      // porta livre qualquer
  const address = httpApp.getHttpServer().address() as { port: number };
  if (options.background !== false) startBackgroundWork(application);
  return {
    application,
    httpApp,
    baseUrl: `http://localhost:${address.port}`,
    async stop() {
      await stopBackgroundWork(application);
      await httpApp.close();                                                                    // fecha o HTTP e o pool (hooks de desligamento)
    },
  };
}

// ----------------------------------------------------------------------------------------------------
//                                       CHAMADAS HTTP
// ----------------------------------------------------------------------------------------------------
export interface HttpResult {
  status: number;
  body: any;
  headers: Headers;
}

export async function httpRequest(baseUrl: string, method: string, path: string, body?: unknown, headers: Record<string, string> = {}): Promise<HttpResult> {
  const response = await fetch(`${baseUrl}${path}`, {
    method,
    headers: { "content-type": "application/json", ...headers },
    body: body === undefined ? undefined : typeof body === "string" ? body : JSON.stringify(body),
  });
  const text = await response.text();
  let parsed: unknown = text;
  try { parsed = JSON.parse(text); } catch {}
  return { status: response.status, body: parsed, headers: response.headers };
}

export async function createWallet(baseUrl: string, initialBalance: string, currency = "BRL", playerId = Bun.randomUUIDv7()): Promise<{ walletId: string; playerId: string }> {
  const response = await httpRequest(baseUrl, "POST", "/wallets", { playerId, initialBalance: { amount: initialBalance, currency } });
  if (response.status !== 201) throw new Error(`criar wallet falhou: ${response.status} ${JSON.stringify(response.body)}`);
  return { walletId: response.body.id, playerId };
}

export interface WagerInput {
  walletId: string;
  playerId: string;
  kind: string;
  amount: string;
  currency?: string;
  externalTransactionId?: string;
  reference?: string;
  roundId?: string;
  providerId?: string;
}

export type WagerBody = Record<string, unknown> & { providerId: string; externalTransactionId: string; walletId: string };

export function wagerBody(input: WagerInput): WagerBody {
  const body: WagerBody = {
    providerId: input.providerId ?? "provider-a",
    externalTransactionId: input.externalTransactionId ?? `ext-${Bun.randomUUIDv7()}`,
    playerId: input.playerId,
    walletId: input.walletId,
    roundId: input.roundId ?? "round-1",
    gameId: "fortune-chimp",
    kind: input.kind,
    money: { amount: input.amount, currency: input.currency ?? "BRL" },
  };
  if (input.reference !== undefined) body.referenceExternalTransactionId = input.reference;
  return body;
}

export async function submitWager(baseUrl: string, input: WagerInput, idempotencyKey?: string): Promise<HttpResult> {
  const body = wagerBody(input);
  return httpRequest(baseUrl, "POST", "/wagering/transactions", body, { "Idempotency-Key": idempotencyKey ?? `${body.providerId}:${body.externalTransactionId}` });
}

// O provedor reenvia com a mesma key quando recebe 503 (lock_timeout numa wallet disputada, seção 8)
// ou quando a conexão cai antes da resposta: as duas situações são seguras por causa da idempotência.
// Os testes de concorrência fazem o papel do provedor e contam quantas vezes isso aconteceu.
export interface ProviderRetryCounter {
  serviceUnavailable: number;
  networkErrors: number;
}

export async function submitLikeProvider(baseUrls: string[], input: WagerInput, random: () => number, counter: ProviderRetryCounter): Promise<HttpResult> {
  const fixedInput = { ...input, externalTransactionId: input.externalTransactionId ?? `ext-${Bun.randomUUIDv7()}` };   // o reenvio é a mesma operação, com a mesma key
  for (let attempt = 0; attempt < 30; attempt++) {
    try {
      const response = await submitWager(baseUrls[attempt % baseUrls.length]!, fixedInput);
      if (response.status !== 503) return response;
      counter.serviceUnavailable = counter.serviceUnavailable + 1;
    } catch {
      counter.networkErrors = counter.networkErrors + 1;                                        // conexão caiu: não dá para saber se processou, então reenvia
    }
    await Bun.sleep(Math.floor(random() * 200 * Math.min(2 ** attempt, 8)));                   // backoff com jitter, como um provedor faria
  }
  throw new Error(`operação ${fixedInput.externalTransactionId} não terminou depois de 30 tentativas`);
}

// ----------------------------------------------------------------------------------------------------
//                                          FILA
// ----------------------------------------------------------------------------------------------------
export async function queueUrl(queueName: string): Promise<string> {
  const { GetQueueUrlCommand } = await import("@aws-sdk/client-sqs");
  const response = await testSqsClient().send(new GetQueueUrlCommand({ QueueName: queueName }));
  return response.QueueUrl!;
}

export function wagerMessage(messageId: string, input: WagerInput, idempotencyKey?: string) {
  const data = wagerBody(input);
  return { messageId, type: "WagerTransactionRequested", occurredAt: new Date().toISOString(), data: { ...data, idempotencyKey: idempotencyKey ?? `${data.providerId}:${data.externalTransactionId}` } };
}

// contrato da fila: MessageGroupId = walletId, MessageDeduplicationId = messageId
export async function sendWagerMessage(queueName: string, message: { messageId: string; data: { walletId: string } } | Record<string, unknown>, options: { groupId?: string; deduplicationId?: string; body?: string } = {}): Promise<void> {
  const typed = message as { messageId?: string; data?: { walletId?: string } };
  await testSqsClient().send(new SendMessageCommand({
    QueueUrl: await queueUrl(queueName),
    MessageBody: options.body ?? JSON.stringify(message),
    MessageGroupId: options.groupId ?? typed.data?.walletId ?? "group",
    MessageDeduplicationId: options.deduplicationId ?? typed.messageId ?? Bun.randomUUIDv7(),
  }));
}

// lê e apaga tudo que chegou numa fila (eventos ou DLQ), até ficar um tempo sem chegar nada
export async function drainQueue(queueName: string, quietMilliseconds = 1500): Promise<{ body: string; attributes: Record<string, string | undefined> }[]> {
  const url = await queueUrl(queueName);
  const received: { body: string; attributes: Record<string, string | undefined> }[] = [];
  let quietSince = Date.now();
  while (Date.now() - quietSince < quietMilliseconds) {
    const response = await testSqsClient().send(new ReceiveMessageCommand({ QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 0, MessageAttributeNames: ["All"] }));
    const messages = response.Messages ?? [];
    if (messages.length === 0) {
      await Bun.sleep(100);
      continue;
    }
    for (const message of messages) {
      const attributes: Record<string, string | undefined> = {};
      for (const [name, value] of Object.entries(message.MessageAttributes ?? {})) attributes[name] = value.StringValue;
      received.push({ body: message.Body ?? "", attributes });
      await testSqsClient().send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: message.ReceiptHandle }));
    }
    quietSince = Date.now();                                                                    // o silêncio conta do fim do lote: apagar 10 mensagens pode levar mais que o prazo
  }
  return received;
}
