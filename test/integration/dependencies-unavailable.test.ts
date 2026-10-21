// ====================================================================================================
//            POSTGRESQL E SQS TEMPORARIAMENTE INDISPONÍVEIS (PREMISSA DA SEÇÃO 3)
// ====================================================================================================
// A aplicação fala com o banco e com a fila reais através de um proxy TCP que o teste corta e religa.
//   SQS fora: o HTTP continua processando, os eventos esperam na outbox e saem quando ele volta.
//   PostgreSQL fora: 503 com Retry-After e nada gravado; quando ele volta, o reenvio com a mesma key processa uma vez.
//   PostgreSQL sem as tabelas: a readiness acusa, em vez de deixar a instância "healthy" respondendo 500.
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { SQL } from "bun";                                                                      // cliente PostgreSQL do Bun, só para criar o banco vazio
import { createTestQueues, createWallet, drainQueue, httpRequest, startInProcessApp, submitWager, testConfig, testSql, uniqueSuffix, waitUntil, type RunningApp, type TestQueues } from "../support/harness";
import { expectWalletInvariants } from "../support/invariants";
import { TcpProxy } from "../support/tcp-proxy";

let queues: TestQueues;
let app: RunningApp;
let databaseProxy: TcpProxy;
let sqsProxy: TcpProxy;
const touchedWallets: string[] = [];

beforeAll(async () => {
  queues = await createTestQueues("unavailable");
  const databaseUrl = new URL(process.env.DATABASE_URL!);
  const sqsUrl = new URL(process.env.SQS_ENDPOINT ?? "http://localhost:4566");
  databaseProxy = new TcpProxy(databaseUrl.hostname, Number(databaseUrl.port || 5432));
  sqsProxy = new TcpProxy(sqsUrl.hostname, Number(sqsUrl.port || 4566));
  await databaseProxy.start();
  await sqsProxy.start();

  databaseUrl.hostname = "127.0.0.1";
  databaseUrl.port = String(databaseProxy.port);
  app = await startInProcessApp(testConfig(queues, {
    DATABASE_URL: databaseUrl.toString(),
    SQS_ENDPOINT: `http://127.0.0.1:${sqsProxy.port}`,
    CONSUMER_ENABLED: "false",
    PENDING_WORKER_ENABLED: "false",
  }));
});

afterAll(async () => {
  await expectWalletInvariants(touchedWallets);
  await app.stop();
  await databaseProxy.cut();
  await sqsProxy.cut();
});

describe("SQS fora do ar", () => {
  test("o HTTP continua processando, a readiness acusa o SQS, e os eventos saem quando ele volta", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    await waitUntil("eventos da abertura publicados", async () => {
      const [row] = await testSql()`select count(*)::int as total from outbox_messages where aggregate_id = ${walletId} and published_at is null`;
      return row.total === 0;
    });

    await sqsProxy.cut();
    const bet = await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "10.00" });
    expect(bet.status).toBe(200);                                                               // a aposta não depende da fila
    const ready = await httpRequest(app.baseUrl, "GET", "/health/ready");
    expect(ready.status).toBe(503);
    expect(ready.body.checks).toEqual({ postgres: "ok", sqs: "unavailable" });
    const failedAttempt = await waitUntil("publisher registrar a falha", async () => {
      const [row] = await testSql()`select max(attempts)::int as attempts, max(last_error) as last_error from outbox_messages where aggregate_id = ${walletId} and published_at is null`;
      return row.attempts >= 1 ? row : undefined;
    });
    expect(failedAttempt.last_error).toBeTruthy();                                              // o evento continua na outbox, com o erro anotado

    await sqsProxy.restore();
    await waitUntil("eventos publicados depois que o SQS voltou", async () => {
      const [row] = await testSql()`select count(*)::int as total from outbox_messages where aggregate_id = ${walletId} and published_at is null`;
      return row.total === 0;
    }, 20_000);
    const published = (await drainQueue(queues.eventsQueueName)).map((message) => JSON.parse(message.body)).filter((event) => event.aggregateId === walletId);
    expect(published.map((event) => event.eventType).sort()).toEqual(["WagerTransactionProcessed", "WagerTransactionProcessed", "WalletBalanceChanged", "WalletBalanceChanged"]);
    expect((await httpRequest(app.baseUrl, "GET", "/health/ready")).status).toBe(200);
  });
});

describe("PostgreSQL fora do ar", () => {
  test("503 com Retry-After e nada gravado; quando o banco volta, o reenvio com a mesma key processa uma vez", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    const bet = { walletId, playerId, kind: "BET", amount: "30.00", externalTransactionId: `bet-${Bun.randomUUIDv7()}` };

    await databaseProxy.cut();
    const unavailable = await submitWager(app.baseUrl, bet);
    expect(unavailable.status).toBe(503);
    expect(unavailable.body.code).toBe("SERVICE_UNAVAILABLE");
    expect(unavailable.headers.get("retry-after")).toBe("1");
    const ready = await httpRequest(app.baseUrl, "GET", "/health/ready");
    expect(ready.status).toBe(503);
    expect(ready.body.checks.postgres).toBe("unavailable");
    expect((await httpRequest(app.baseUrl, "GET", "/health/live")).status).toBe(200);          // liveness não olha dependências

    await databaseProxy.restore();
    const processed = await waitUntil("o reenvio passar depois que o banco voltou", async () => {
      const response = await submitWager(app.baseUrl, bet);
      return response.status === 200 ? response : undefined;
    }, 20_000);
    expect(processed.body).toMatchObject({ status: "PROCESSED", balance: { amount: "70.00", currency: "BRL" }, idempotentReplay: false });   // o 503 não tinha gravado nada
    const [counts] = await testSql()`select count(*)::int as total from wager_transactions where external_transaction_id = ${bet.externalTransactionId}`;
    expect(counts.total).toBe(1);
  });
});

describe("PostgreSQL de pé, mas sem as tabelas", () => {
  test("a readiness responde 503 com schema_missing, em vez de dizer que está pronta", async () => {
    const emptyDatabaseUrl = new URL(process.env.DATABASE_URL!);
    const databaseName = `wagering_empty_${uniqueSuffix()}`;
    const admin = new SQL(emptyDatabaseUrl.toString());
    await admin.unsafe(`create database ${databaseName}`);                                      // banco novo, sem migrations
    await admin.close();
    emptyDatabaseUrl.pathname = `/${databaseName}`;

    const withoutSchema = await startInProcessApp(testConfig(queues, { DATABASE_URL: emptyDatabaseUrl.toString() }), {}, { background: false });
    const ready = await httpRequest(withoutSchema.baseUrl, "GET", "/health/ready");
    await withoutSchema.stop();
    expect(ready.status).toBe(503);
    expect(ready.body.checks).toEqual({ postgres: "schema_missing", sqs: "ok" });
  });
});
