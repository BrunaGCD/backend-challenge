// ====================================================================================================
//    RETRY E DLQ: TRANSITÓRIO VOLTA COM BACKOFF; TRANSITÓRIO QUE NÃO PASSA VAI PARA A DLQ PELO SQS
// ====================================================================================================
// O erro transitório entra de dois jeitos: injetado na porta do repositório, e um lock_timeout de
// verdade, com um teste segurando o lock da wallet por fora. A fila espera o lock mais que o HTTP
// (DATABASE_BACKGROUND_LOCK_TIMEOUT), e um teste mostra a diferença com a mesma wallet travada.
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { Repositories } from "../../src/application/ports";
import { createTestQueues, createWallet, drainQueue, httpRequest, sendWagerMessage, startInProcessApp, submitWager, testConfig, testSql, wagerMessage, waitUntil, type RunningApp, type TestQueues } from "../support/harness";
import { expectWalletInvariants } from "../support/invariants";

let queues: TestQueues;
let app: RunningApp;
const touchedWallets: string[] = [];
const transientFailuresLeftByWallet = new Map<string, number>();                               // walletId -> quantas falhas ainda injetar
const lockAttemptsByWallet = new Map<string, number>();

function withTransientFailures(real: Repositories): Repositories {
  return {
    ...real,
    wallets: {
      findById: (walletId) => real.wallets.findById(walletId),
      findByPlayerAndCurrency: (playerId, currency) => real.wallets.findByPlayerAndCurrency(playerId, currency),
      insert: (wallet) => real.wallets.insert(wallet),
      updateBalance: (wallet, expectedVersion) => real.wallets.updateBalance(wallet, expectedVersion),
      findByIdForUpdate: async (walletId) => {
        lockAttemptsByWallet.set(walletId, (lockAttemptsByWallet.get(walletId) ?? 0) + 1);
        const failuresLeft = transientFailuresLeftByWallet.get(walletId) ?? 0;
        if (failuresLeft > 0) {
          transientFailuresLeftByWallet.set(walletId, failuresLeft - 1);
          throw Object.assign(new Error("canceling statement due to lock timeout (injetado)"), { code: "55P03" });   // mesmo SQLSTATE do lock_timeout
        }
        return real.wallets.findByIdForUpdate(walletId);
      },
    },
  };
}

async function metricValue(name: string, labels = ""): Promise<number> {
  const metrics = await httpRequest(app.baseUrl, "GET", "/metrics");
  const line = (metrics.body as string).split("\n").find((candidate) => candidate.startsWith(`${name}${labels}`));
  return line ? Number(line.split(" ").at(-1)) : 0;
}

beforeAll(async () => {
  queues = await createTestQueues("retry");                                                     // maxReceiveCount = 3 nos testes
  app = await startInProcessApp(testConfig(queues, { DATABASE_LOCK_TIMEOUT: "500ms" }), { repositories: withTransientFailures });
});

afterAll(async () => {
  await expectWalletInvariants(touchedWallets);
  await app.stop();
});

describe("fila", () => {
  test("transitório que passa: a mensagem volta pela visibilidade, é processada uma vez e conta como retry", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    const retriesBefore = await metricValue("wagering_retries_total", '{component="sqs_consumer"}');
    transientFailuresLeftByWallet.set(walletId, 2);                                             // falha nas duas primeiras tentativas

    const message = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "25.00" });
    await sendWagerMessage(queues.inputQueueName, message);
    const [transaction] = await waitUntil("processada depois dos retries", async () => {
      const rows = await testSql()`select status from wager_transactions where external_transaction_id = ${message.data.externalTransactionId}`;
      return rows.length > 0 ? rows : undefined;
    });
    expect(transaction.status).toBe("PROCESSED");
    expect(lockAttemptsByWallet.get(walletId)).toBe(3);                                          // duas falhas e o sucesso
    expect(await metricValue("wagering_retries_total", '{component="sqs_consumer"}')).toBe(retriesBefore + 2);
    const [wallet] = await testSql()`select balance from wallets where id = ${walletId}`;
    expect(wallet.balance).toBe("75.00");
  });

  test("transitório que não passa: depois do maxReceiveCount o próprio SQS move para a DLQ, sem nada gravado", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    transientFailuresLeftByWallet.set(walletId, 1_000);

    const message = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "25.00" });
    await sendWagerMessage(queues.inputQueueName, message);
    const deadLetters = await waitUntil("redrive para a DLQ", async () => {
      const received = await drainQueue(queues.deadLetterQueueName, 300);
      return received.length > 0 ? received : undefined;
    }, 40_000);
    transientFailuresLeftByWallet.set(walletId, 0);

    expect(JSON.parse(deadLetters[0]!.body).messageId).toBe(message.messageId);
    expect(deadLetters[0]!.attributes.reason).toBeUndefined();                                  // quem moveu foi o SQS, não o consumidor
    expect(lockAttemptsByWallet.get(walletId)).toBe(3);                                          // maxReceiveCount = 3
    const [counts] = await testSql()`select count(*)::int as total from wager_transactions where external_transaction_id = ${message.data.externalTransactionId}`;
    expect(counts.total).toBe(0);                                                               // DLQ não cria FAILED: nada foi gravado
  });

  test("a fila espera o lock mais que o HTTP: com a wallet travada por fora, o HTTP desiste com 503 e a mensagem espera e processa na primeira tentativa", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    const retriesBefore = await metricValue("wagering_retries_total", '{component="sqs_consumer"}');

    let releaseLock: () => void = () => {};
    let lockHeld: () => void = () => {};
    const lockHeldPromise = new Promise<void>((resolve) => { lockHeld = resolve; });
    const holder = testSql().begin(async (transaction) => {
      await transaction`select id from wallets where id = ${walletId} for update`;
      lockHeld();
      await new Promise<void>((resolve) => { releaseLock = resolve; });
    });
    await lockHeldPromise;

    const httpBet = await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "10.00" });   // espera 500 ms e desiste
    expect(httpBet.status).toBe(503);

    const attemptsBefore = lockAttemptsByWallet.get(walletId) ?? 0;
    const message = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "25.00" });
    await sendWagerMessage(queues.inputQueueName, message);
    await waitUntil("o consumidor começar a esperar o lock", () => (lockAttemptsByWallet.get(walletId) ?? 0) > attemptsBefore);
    await Bun.sleep(1_000);                                                                     // o dobro do que o HTTP aguentou, e a mensagem continua esperando
    releaseLock();
    await holder;

    const [transaction] = await waitUntil("a mensagem processada", async () => {
      const rows = await testSql()`select status from wager_transactions where external_transaction_id = ${message.data.externalTransactionId}`;
      return rows.length > 0 ? rows : undefined;
    });
    expect(transaction.status).toBe("PROCESSED");
    expect(lockAttemptsByWallet.get(walletId)).toBe(attemptsBefore + 1);                         // uma tentativa só, sem lock_timeout
    expect(await metricValue("wagering_retries_total", '{component="sqs_consumer"}')).toBe(retriesBefore);
  });

  test("a métrica de DLQ lê o tamanho da fila, inclusive o que o SQS moveu", async () => {
    const { walletId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    await sendWagerMessage(queues.inputQueueName, {}, { body: "{quebrado", groupId: walletId, deduplicationId: Bun.randomUUIDv7() });
    await waitUntil("gauge da DLQ maior que zero", async () => (await metricValue("wagering_dlq_messages")) > 0);
    await drainQueue(queues.deadLetterQueueName, 300);
  });
});

describe("HTTP", () => {
  test("transitório injetado: 503 SERVICE_UNAVAILABLE com Retry-After, nada gravado, e reenviar funciona", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    transientFailuresLeftByWallet.set(walletId, 1);
    const bet = { walletId, playerId, kind: "BET", amount: "10.00", externalTransactionId: `bet-${Bun.randomUUIDv7()}` };
    const unavailable = await submitWager(app.baseUrl, bet);
    expect(unavailable.status).toBe(503);
    expect(unavailable.headers.get("retry-after")).toBe("1");
    expect(unavailable.body.code).toBe("SERVICE_UNAVAILABLE");
    const retried = await submitWager(app.baseUrl, bet);
    expect(retried.status).toBe(200);
    expect(retried.body.idempotentReplay).toBe(false);                                          // o 503 não gravou nada, então foi a primeira de verdade
  });

  test("lock_timeout real: com a wallet travada por fora, 503 e métrica de lock_timeout; solto o lock, passa", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    const lockTimeoutsBefore = await metricValue("wagering_concurrency_conflicts_total", '{type="lock_timeout"}');

    let releaseLock: () => void = () => {};
    let lockHeld: () => void = () => {};
    const lockHeldPromise = new Promise<void>((resolve) => { lockHeld = resolve; });
    const holder = testSql().begin(async (transaction) => {
      await transaction`select id from wallets where id = ${walletId} for update`;
      lockHeld();
      await new Promise<void>((resolve) => { releaseLock = resolve; });
    });
    await lockHeldPromise;

    const bet = { walletId, playerId, kind: "BET", amount: "10.00", externalTransactionId: `bet-${Bun.randomUUIDv7()}` };
    const blocked = await submitWager(app.baseUrl, bet);                                        // espera 500 ms e desiste
    expect(blocked.status).toBe(503);
    expect(await metricValue("wagering_concurrency_conflicts_total", '{type="lock_timeout"}')).toBe(lockTimeoutsBefore + 1);

    releaseLock();
    await holder;
    const afterRelease = await submitWager(app.baseUrl, bet);
    expect(afterRelease.status).toBe(200);
  });

  test("outra wallet não espera o lock de uma wallet travada (não existe lock global)", async () => {
    const locked = await createWallet(app.baseUrl, "100.00");
    const free = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(locked.walletId, free.walletId);
    let releaseLock: () => void = () => {};
    let lockHeld: () => void = () => {};
    const lockHeldPromise = new Promise<void>((resolve) => { lockHeld = resolve; });
    const holder = testSql().begin(async (transaction) => {
      await transaction`select id from wallets where id = ${locked.walletId} for update`;
      lockHeld();
      await new Promise<void>((resolve) => { releaseLock = resolve; });
    });
    await lockHeldPromise;

    const startedAt = performance.now();
    const response = await submitWager(app.baseUrl, { walletId: free.walletId, playerId: free.playerId, kind: "BET", amount: "1.00" });
    const elapsedMilliseconds = performance.now() - startedAt;
    releaseLock();
    await holder;
    expect(response.status).toBe(200);
    expect(elapsedMilliseconds).toBeLessThan(500);                                              // não chegou nem perto do lock_timeout
  });
});
