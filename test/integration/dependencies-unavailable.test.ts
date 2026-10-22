// ====================================================================================================
//          POSTGRESQL E SQS TEMPORARIAMENTE INDISPONÍVEIS (PREMISSA DA SEÇÃO 3 DO DESAFIO)
// ====================================================================================================
// A aplicação fala com o banco e com a fila reais através de um proxy TCP que o teste corta e religa.
//   SQS fora: o HTTP continua processando, os eventos esperam na outbox e saem quando ele volta.
//   PostgreSQL fora: 503 com Retry-After e nada gravado; quando ele volta, o reenvio com a mesma key processa uma vez.
//   PostgreSQL fora com mensagens na fila: o consumidor pausa e segura o que tem em mãos, sem gastar recebimentos.
//   PostgreSQL sem as tabelas: a readiness acusa, em vez de deixar a instância "healthy" respondendo 500.
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { SQL } from "bun";                                                                      // cliente PostgreSQL do Bun, só para criar o banco vazio
import { createTestQueues, createWallet, drainQueue, httpRequest, sendWagerMessage, startInProcessApp, submitWager, testConfig, testSql, uniqueSuffix, wagerMessage, waitUntil, type RunningApp, type TestQueues } from "../support/harness";
import { expectWalletInvariants } from "../support/invariants";
import { newQueueRecord, receiveCountsOf, recordingQueue, type QueueRecord } from "../support/recording-queue";
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

  // O rollback de uma transação cortada pela rede falha, e o BEGIN numa conexão que acabou de morrer
  // também. Sem o patch do kysely (patches/), nos dois casos a conexão nunca voltava ao pool.
  test("transação cortada no meio: depois que o banco volta, nenhuma conexão fica emprestada no pool", async () => {
    const pool = await app.application.orm.em.getConnection().getNativeClient();               // o pool do pg, por baixo do MikroORM e do kysely
    const borrowedConnections = () => pool.totalCount - pool.idleCount;                         // abertas menos as paradas no pool
    const database = app.application.database;

    const cutInTheMiddle = database.run(async () => {
      await database.transactionEntityManager().execute("select 1");                           // a transação está aberta nesta conexão
      await databaseProxy.cut();                                                                // a rede cai no meio dela
      await database.transactionEntityManager().execute("select 1");                           // falha, e o rollback que vem depois também
    });
    await expect(cutInTheMiddle).rejects.toThrow();

    await databaseProxy.restore();
    await waitUntil("nenhuma conexão emprestada no pool", () => borrowedConnections() === 0, 10_000);   // o publisher da outbox usa e devolve no meio do caminho
    expect((await httpRequest(app.baseUrl, "GET", "/health/ready")).status).toBe(200);
  });
});

// ----------------------------------------------------------------------------------------------------
//                      PostgreSQL FORA COM MENSAGENS CHEGANDO PELA FILA
// ----------------------------------------------------------------------------------------------------
// Um consumidor próprio, falando com o banco pelo proxy. As quedas abaixo duram bem mais do que 3
// recebimentos com o backoff de até 1 s dos testes durariam (uns 2 s), então só terminam sem DLQ porque a
// mensagem com falha transitória fica com o consumidor, tentando de novo, em vez de voltar para a fila.
describe("PostgreSQL fora com mensagens na fila", () => {
  async function startConsumerThroughProxy(consumerQueues: TestQueues, record: QueueRecord, retryBudgetMilliseconds: string): Promise<RunningApp> {
    const databaseUrl = new URL(process.env.DATABASE_URL!);
    databaseUrl.hostname = "127.0.0.1";
    databaseUrl.port = String(databaseProxy.port);
    return startInProcessApp(testConfig(consumerQueues, { DATABASE_URL: databaseUrl.toString(), CONSUMER_RETRY_BUDGET_MS: retryBudgetMilliseconds, OUTBOX_PUBLISHER_ENABLED: "false", PENDING_WORKER_ENABLED: "false" }), { inputQueue: recordingQueue(record) });
  }

  async function processedCount(externalIds: string[]): Promise<number> {
    const [row] = await testSql()`select count(*)::int as total from wager_transactions where external_transaction_id in ${testSql()(externalIds)} and status = 'PROCESSED'`;
    return row.total;
  }

  async function pausesOf(consumerApp: RunningApp): Promise<number> {
    const metrics = (await httpRequest(consumerApp.baseUrl, "GET", "/metrics")).body as string;
    return Number(/wagering_consumer_pauses_total\{reason="database_unavailable"\} (\S+)/.exec(metrics)?.[1] ?? 0);
  }

  test("queda de 6 s com um lote de uma wallet na fila: o consumidor pausa, segura o lote, e ao voltar as cinco viram transação no mesmo recebimento, sem DLQ", async () => {
    const consumerQueues = await createTestQueues("outage");
    const record = newQueueRecord();
    const consumerApp = await startConsumerThroughProxy(consumerQueues, record, "20000");
    const { walletId, playerId } = await createWallet(consumerApp.baseUrl, "100.00");
    touchedWallets.push(walletId);

    await databaseProxy.cut();
    const messages = [1, 2, 3, 4, 5].map(() => wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "1.00" }));
    for (const message of messages) await sendWagerMessage(consumerQueues.inputQueueName, message);
    await waitUntil("o consumidor pausar", async () => (await pausesOf(consumerApp)) > 0);
    await Bun.sleep(6_000);                                                                     // bem mais que os uns 2 s de 3 recebimentos com backoff
    await databaseProxy.restore();

    const externalIds = messages.map((message) => message.data.externalTransactionId);
    await waitUntil("as cinco processadas", async () => (await processedCount(externalIds)) === 5, 30_000);
    const pauses = await pausesOf(consumerApp);
    await consumerApp.stop();

    for (const message of messages) expect(receiveCountsOf(record, message.messageId)).toEqual([1]);   // nenhuma voltou para a fila
    expect(record.visibilityChanges.filter((change) => change.seconds === 0)).toHaveLength(0);
    expect(pauses).toBe(1);                                                                     // uma pausa para a queda inteira
    expect(await drainQueue(consumerQueues.deadLetterQueueName, 300)).toHaveLength(0);
    const [wallet] = await testSql()`select balance from wallets where id = ${walletId}`;
    expect(wallet.balance).toBe("95.00");
  });

  test("queda mais longa que o orçamento: a mensagem em mãos vai para a DLQ com o motivo, e as que chegam depois ficam na fila até o banco voltar", async () => {
    const consumerQueues = await createTestQueues("long-outage");
    const record = newQueueRecord();
    const consumerApp = await startConsumerThroughProxy(consumerQueues, record, "1500");
    const first = await createWallet(consumerApp.baseUrl, "100.00");
    const second = await createWallet(consumerApp.baseUrl, "100.00");
    touchedWallets.push(first.walletId, second.walletId);

    await databaseProxy.cut();
    const inHand = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { ...first, kind: "BET", amount: "1.00" });
    await sendWagerMessage(consumerQueues.inputQueueName, inHand);
    const deadLetters = await waitUntil("a mensagem em mãos na DLQ", async () => {
      const received = await drainQueue(consumerQueues.deadLetterQueueName, 300);
      return received.length > 0 ? received : undefined;
    }, 20_000);
    expect(deadLetters[0]!.attributes.reason).toBe("RETRY_BUDGET_EXHAUSTED");

    const arrivedLater = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { ...second, kind: "BET", amount: "1.00" });
    await sendWagerMessage(consumerQueues.inputQueueName, arrivedLater);
    await Bun.sleep(3_000);                                                                     // pausado: não busca nada com o banco fora
    expect(receiveCountsOf(record, arrivedLater.messageId)).toEqual([]);

    await databaseProxy.restore();
    await waitUntil("a que chegou depois processada", async () => (await processedCount([arrivedLater.data.externalTransactionId])) === 1, 20_000);
    await consumerApp.stop();
    expect(receiveCountsOf(record, inHand.messageId)).toEqual([1]);
    expect(receiveCountsOf(record, arrivedLater.messageId)).toEqual([1]);                      // recebida só depois da volta, na primeira entrega
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
