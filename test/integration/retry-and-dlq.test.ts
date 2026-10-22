// ====================================================================================================
//    RETRY E DLQ: TRANSITÓRIO TENTA DE NOVO NO PRÓPRIO CONSUMIDOR; ESGOTADO O ORÇAMENTO, VAI PARA A DLQ
// ====================================================================================================
// O erro transitório entra de dois jeitos: injetado na porta do repositório, e um lock_timeout de
// verdade, com um teste segurando o lock da wallet por fora. Em todos os testes da fila, o
// ApproximateReceiveCount anotado em cada recebimento mostra que nenhum recebimento foi gasto: a
// mensagem que falha fica com o consumidor, e as seguintes da mesma wallet também.
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { GetQueueAttributesCommand } from "@aws-sdk/client-sqs";
import type { Repositories } from "../../src/application/ports";
import type { InputQueue } from "../../src/infrastructure/messaging/sqs";                       // a fila de entrada, embrulhada para anotar recebimentos e devoluções
import { createTestQueues, createWallet, drainQueue, httpRequest, queueUrl, sendWagerMessage, startInProcessApp, submitWager, testConfig, testSql, testSqsClient, wagerMessage, waitUntil, type RunningApp, type TestQueues } from "../support/harness";
import { expectWalletInvariants } from "../support/invariants";
import { newQueueRecord, receiveCountsOf, recordingQueue, releasesOf } from "../support/recording-queue";

let queues: TestQueues;
let app: RunningApp;
const queueRecord = newQueueRecord();                                                           // recebimentos e visibilidade da fila do arquivo
const touchedWallets: string[] = [];
const transientFailuresLeftByWallet = new Map<string, number>();                               // walletId -> quantas falhas ainda injetar
const slowLockByWallet = new Map<string, number>();                                            // walletId -> quanto a tentativa demora antes do lock
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
        const slowMilliseconds = slowLockByWallet.get(walletId) ?? 0;
        if (slowMilliseconds > 0) await Bun.sleep(slowMilliseconds);                            // processamento mais longo que a estimativa
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

async function queueIsEmpty(queueName: string): Promise<boolean> {
  const response = await testSqsClient().send(new GetQueueAttributesCommand({ QueueUrl: await queueUrl(queueName), AttributeNames: ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"] }));
  return Number(response.Attributes?.ApproximateNumberOfMessages ?? 0) + Number(response.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0) === 0;
}

async function transactionRows(externalTransactionId: string) {
  return testSql()`select status from wager_transactions where external_transaction_id = ${externalTransactionId}`;
}

// segura o lock da wallet numa transação por fora, até o teste soltar
async function holdWalletLock(walletId: string): Promise<{ release: () => Promise<void> }> {
  let releaseLock: () => void = () => {};
  let lockHeld: () => void = () => {};
  const lockHeldPromise = new Promise<void>((resolve) => { lockHeld = resolve; });
  const holder = testSql().begin(async (transaction) => {
    await transaction`select id from wallets where id = ${walletId} for update`;
    lockHeld();
    await new Promise<void>((resolve) => { releaseLock = resolve; });
  });
  await lockHeldPromise;
  return { release: async () => { releaseLock(); await holder; } };
}

beforeAll(async () => {
  queues = await createTestQueues("retry");                                                     // visibilidade de 3 s nos testes
  app = await startInProcessApp(testConfig(queues, { DATABASE_LOCK_TIMEOUT: "500ms", CONSUMER_RETRY_BUDGET_MS: "4000" }), { repositories: withTransientFailures, inputQueue: recordingQueue(queueRecord) });
});

afterAll(async () => {
  await expectWalletInvariants(touchedWallets);
  await app.stop();
});

describe("fila", () => {
  test("transitório que passa: tenta de novo no próprio consumidor, sem voltar para a fila, é processada uma vez e conta como retry", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    const retriesBefore = await metricValue("wagering_retries_total", '{component="sqs_consumer"}');
    transientFailuresLeftByWallet.set(walletId, 2);                                             // falha nas duas primeiras tentativas

    const message = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "25.00" });
    await sendWagerMessage(queues.inputQueueName, message);
    const [transaction] = await waitUntil("processada depois dos retries", async () => {
      const rows = await transactionRows(message.data.externalTransactionId);
      return rows.length > 0 ? rows : undefined;
    });
    expect(transaction.status).toBe("PROCESSED");
    expect(lockAttemptsByWallet.get(walletId)).toBe(3);                                          // duas falhas e o sucesso
    expect(await metricValue("wagering_retries_total", '{component="sqs_consumer"}')).toBe(retriesBefore + 2);
    expect(receiveCountsOf(queueRecord, message.messageId)).toEqual([1]);                       // recebida uma vez só: as falhas não gastaram recebimento
    expect(releasesOf(queueRecord, message.messageId)).toBe(0);
    const [wallet] = await testSql()`select balance from wallets where id = ${walletId}`;
    expect(wallet.balance).toBe("75.00");
  });

  test("transitório que não passa: esgotado o orçamento do consumidor, ele manda para a DLQ com o motivo, sem nada gravado e sem gastar recebimentos", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    transientFailuresLeftByWallet.set(walletId, 1_000);
    const forwardedBefore = await metricValue("wagering_dlq_forwarded_total", '{reason="RETRY_BUDGET_EXHAUSTED"}');

    const message = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "25.00" });
    const sentAt = performance.now();
    await sendWagerMessage(queues.inputQueueName, message);
    const deadLetters = await waitUntil("a mensagem na DLQ", async () => {
      const received = await drainQueue(queues.deadLetterQueueName, 300);
      return received.length > 0 ? received : undefined;
    }, 30_000);
    const elapsedMilliseconds = performance.now() - sentAt;
    transientFailuresLeftByWallet.set(walletId, 0);

    expect(JSON.parse(deadLetters[0]!.body).messageId).toBe(message.messageId);
    expect(deadLetters[0]!.attributes.reason).toBe("RETRY_BUDGET_EXHAUSTED");                  // quem mandou foi o consumidor, com o motivo
    expect(elapsedMilliseconds).toBeGreaterThanOrEqual(4_000);                                  // só depois do orçamento de 4 s do arquivo
    expect(lockAttemptsByWallet.get(walletId)).toBeGreaterThanOrEqual(3);                        // várias tentativas dentro do orçamento
    expect(receiveCountsOf(queueRecord, message.messageId)).toEqual([1]);                       // todas no mesmo recebimento
    expect(await metricValue("wagering_dlq_forwarded_total", '{reason="RETRY_BUDGET_EXHAUSTED"}')).toBe(forwardedBefore + 1);
    const [counts] = await testSql()`select count(*)::int as total from wager_transactions where external_transaction_id = ${message.data.externalTransactionId}`;
    expect(counts.total).toBe(0);                                                               // DLQ não cria FAILED: nada foi gravado
  });

  test("a fila espera o lock mais que o HTTP: com a wallet travada por fora, o HTTP desiste com 503 e a mensagem espera e processa na primeira tentativa", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    const retriesBefore = await metricValue("wagering_retries_total", '{component="sqs_consumer"}');
    const lock = await holdWalletLock(walletId);

    const httpBet = await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "10.00" });   // espera 500 ms e desiste
    expect(httpBet.status).toBe(503);

    const attemptsBefore = lockAttemptsByWallet.get(walletId) ?? 0;
    const message = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "25.00" });
    await sendWagerMessage(queues.inputQueueName, message);
    await waitUntil("o consumidor começar a esperar o lock", () => (lockAttemptsByWallet.get(walletId) ?? 0) > attemptsBefore);
    await Bun.sleep(1_000);                                                                     // o dobro do que o HTTP aguentou, e a mensagem continua esperando
    await lock.release();

    const [transaction] = await waitUntil("a mensagem processada", async () => {
      const rows = await transactionRows(message.data.externalTransactionId);
      return rows.length > 0 ? rows : undefined;
    });
    expect(transaction.status).toBe("PROCESSED");
    expect(lockAttemptsByWallet.get(walletId)).toBe(attemptsBefore + 1);                         // uma tentativa só, sem lock_timeout
    expect(await metricValue("wagering_retries_total", '{component="sqs_consumer"}')).toBe(retriesBefore);
  });

  test("processamento mais longo que o prazo de visibilidade: o prazo é estendido durante o processamento, e a mensagem não volta para a fila", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    slowLockByWallet.set(walletId, 5_000);                                                      // mais que os 3 s de visibilidade dos testes

    const message = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "5.00" });
    await sendWagerMessage(queues.inputQueueName, message);
    const [transaction] = await waitUntil("a mensagem processada", async () => {
      const rows = await transactionRows(message.data.externalTransactionId);
      return rows.length > 0 ? rows : undefined;
    });
    slowLockByWallet.set(walletId, 0);
    await waitUntil("mensagem apagada da fila", () => queueIsEmpty(queues.inputQueueName));

    expect(transaction.status).toBe("PROCESSED");
    const extensions = queueRecord.visibilityChanges.filter((change) => change.messageId === message.messageId && change.seconds === 3);
    expect(extensions.length).toBeGreaterThanOrEqual(1);                                        // estendida enquanto processava
    expect(receiveCountsOf(queueRecord, message.messageId)).toEqual([1]);                       // nenhuma outra entrega no meio
    expect(lockAttemptsByWallet.get(walletId)).toBe(1);                                         // processada uma vez, sem disputa consigo mesma
  });

  test("DeleteMessage que falha depois do commit: só o delete é repetido, ali mesmo, sem reprocessar, sem DLQ e sem gastar recebimento", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    const message = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "7.00" });
    queueRecord.deleteFailuresLeft.set(message.messageId, 1);                                   // o primeiro DeleteMessage falha

    await sendWagerMessage(queues.inputQueueName, message);
    await waitUntil("mensagem apagada da fila", async () => (await transactionRows(message.data.externalTransactionId)).length > 0 && (await queueIsEmpty(queues.inputQueueName)));

    expect(queueRecord.deleteFailuresLeft.get(message.messageId)).toBe(0);                     // a falha aconteceu
    expect(receiveCountsOf(queueRecord, message.messageId)).toEqual([1]);
    expect(lockAttemptsByWallet.get(walletId)).toBe(1);                                         // o commit não foi refeito
    const [counts] = await testSql()`select count(*)::int as total from wager_transactions where external_transaction_id = ${message.data.externalTransactionId}`;
    expect(counts.total).toBe(1);
    expect(await drainQueue(queues.deadLetterQueueName, 300)).toHaveLength(0);
  });

  test("a primeira de um lote da mesma wallet falha: as seguintes ficam com o consumidor e as quatro são processadas no mesmo recebimento", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    const batchQueues = await createTestQueues("follow");                                      // fila própria: o consumidor do arquivo não pega este lote
    const record = newQueueRecord();
    const messages = [1, 2, 3, 4].map(() => wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "1.00" }));
    for (const message of messages) await sendWagerMessage(batchQueues.inputQueueName, message);   // todas na fila antes do consumidor ligar: vêm num lote só
    transientFailuresLeftByWallet.set(walletId, 3);                                             // a primeira falha três vezes

    const consumerApp = await startInProcessApp(testConfig(batchQueues, { OUTBOX_PUBLISHER_ENABLED: "false", PENDING_WORKER_ENABLED: "false" }), { repositories: withTransientFailures, inputQueue: recordingQueue(record) });
    const externalIds = messages.map((message) => message.data.externalTransactionId);
    await waitUntil("as quatro processadas", async () => {
      const [row] = await testSql()`select count(*)::int as total from wager_transactions where external_transaction_id in ${testSql()(externalIds)} and status = 'PROCESSED'`;
      return row.total === 4;
    });
    await waitUntil("fila vazia", () => queueIsEmpty(batchQueues.inputQueueName));
    await consumerApp.stop();

    expect(record.receives[0]!.messages).toHaveLength(4);                                       // o lote inteiro da mesma wallet num recebimento
    for (const message of messages) {
      expect(receiveCountsOf(record, message.messageId)).toEqual([1]);                          // nenhuma recebida de novo, nem a que falhou
      expect(releasesOf(record, message.messageId)).toBe(0);                                    // nenhuma devolvida
    }
    expect(lockAttemptsByWallet.get(walletId)).toBe(4 + 3);                                     // as três falhas e uma tentativa de cada
    const [wallet] = await testSql()`select balance from wallets where id = ${walletId}`;
    expect(wallet.balance).toBe("96.00");
    expect(await drainQueue(batchQueues.deadLetterQueueName, 300)).toHaveLength(0);
  });

  test("desligamento durante a espera entre tentativas: a espera é interrompida, a mensagem volta para a fila e outra instância a processa (o único recebimento gasto)", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    const shutdownQueues = await createTestQueues("stop");
    const leavingRecord = newQueueRecord();
    const stayingRecord = newQueueRecord();
    const slowBackoff = { CONSUMER_RETRY_BASE_MS: "8000", CONSUMER_RETRY_CAP_MS: "8000", CONSUMER_RETRY_BUDGET_MS: "60000", OUTBOX_PUBLISHER_ENABLED: "false", PENDING_WORKER_ENABLED: "false" };
    transientFailuresLeftByWallet.set(walletId, 1_000);

    const leaving = await startInProcessApp(testConfig(shutdownQueues, slowBackoff), { repositories: withTransientFailures, inputQueue: recordingQueue(leavingRecord) });
    const message = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "2.00" });
    await sendWagerMessage(shutdownQueues.inputQueueName, message);
    await waitUntil("a primeira tentativa falhar", () => (lockAttemptsByWallet.get(walletId) ?? 0) >= 1);
    await Bun.sleep(200);                                                                       // já na espera de até 8 s

    const stopStartedAt = performance.now();
    await leaving.stop();
    const stopMilliseconds = performance.now() - stopStartedAt;
    expect(stopMilliseconds).toBeLessThan(3_000);                                               // não esperou o backoff
    expect(releasesOf(leavingRecord, message.messageId)).toBe(1);                              // devolvida com visibilidade 0

    transientFailuresLeftByWallet.set(walletId, 0);
    const staying = await startInProcessApp(testConfig(shutdownQueues, { OUTBOX_PUBLISHER_ENABLED: "false", PENDING_WORKER_ENABLED: "false" }), { repositories: withTransientFailures, inputQueue: recordingQueue(stayingRecord) });
    await waitUntil("processada pela outra instância", async () => (await transactionRows(message.data.externalTransactionId)).length > 0 && (await queueIsEmpty(shutdownQueues.inputQueueName)));
    await staying.stop();

    expect(receiveCountsOf(leavingRecord, message.messageId)).toEqual([1]);
    expect(receiveCountsOf(stayingRecord, message.messageId)).toEqual([2]);                     // a devolução do desligamento gasta um, e só ela
    const [counts] = await testSql()`select count(*)::int as total from wager_transactions where external_transaction_id = ${message.data.externalTransactionId} and status = 'PROCESSED'`;
    expect(counts.total).toBe(1);
  });

  // Fila própria com visibilidade de 10 s e statement_timeout de 8 s (a mesma proporção dos 2,5 s em 3 s
  // dos outros testes): só cabe começar uma mensagem até 2 s depois do recebimento. A primeira do lote
  // espera o lock por 3 s; as outras três já não cabem, e a extensão tem uns 7 s de folga até o prazo vencer.
  test("lote da mesma wallet com a wallet travada por fora: o prazo das mensagens que não cabem mais é estendido, e as quatro são processadas no mesmo recebimento", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    const batchQueues = await createTestQueues("visibility", { visibilityTimeoutSeconds: 10, maxReceiveCount: 3, waitTimeSeconds: 1 });   // fila própria: o consumidor do arquivo não pega este lote
    const messages = [1, 2, 3, 4].map(() => wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "1.00" }));
    for (const message of messages) await sendWagerMessage(batchQueues.inputQueueName, message);   // todas na fila antes do consumidor ligar: vêm num lote só

    const lock = await holdWalletLock(walletId);
    const receives: { at: number; messages: { id: string; receiveCount: number }[] }[] = [];
    const visibilityChanges: { at: number; id: string; seconds: number }[] = [];
    const recordingBatchQueue = (real: InputQueue): InputQueue => ({
      async receive(maxMessages, waitTimeSeconds, abortSignal) {
        const received = await real.receive(maxMessages, waitTimeSeconds, abortSignal);
        if (received.length > 0) receives.push({ at: performance.now(), messages: received.map((message) => ({ id: message.sqsMessageId, receiveCount: message.receiveCount })) });
        return received;
      },
      delete: (message) => real.delete(message),
      async changeVisibility(message, seconds) {
        visibilityChanges.push({ at: performance.now(), id: message.sqsMessageId, seconds });
        await real.changeVisibility(message, seconds);
      },
      sendToDeadLetter: (message, reason) => real.sendToDeadLetter(message, reason),
    });
    const tenSecondVisibility = { SQS_VISIBILITY_TIMEOUT_SECONDS: "10", DATABASE_BACKGROUND_LOCK_TIMEOUT: "6s", DATABASE_BACKGROUND_STATEMENT_TIMEOUT: "8s", OUTBOX_PUBLISHER_ENABLED: "false", PENDING_WORKER_ENABLED: "false" };
    const consumerApp = await startInProcessApp(testConfig(batchQueues, tenSecondVisibility), { inputQueue: recordingBatchQueue });

    await waitUntil("o primeiro lote chegar", () => receives.length > 0);
    await Bun.sleep(3_000);                                                                     // a primeira espera o lock; o prazo das outras se esgota
    await lock.release();

    const externalIds = messages.map((message) => message.data.externalTransactionId);
    await waitUntil("as quatro processadas", async () => {
      const [row] = await testSql()`select count(*)::int as total from wager_transactions where external_transaction_id in ${testSql()(externalIds)} and status = 'PROCESSED'`;
      return row.total === 4;
    });
    await consumerApp.stop();

    const firstBatch = receives[0]!;
    expect(firstBatch.messages).toHaveLength(4);                                                // o lote inteiro da mesma wallet num recebimento
    const batchIds = firstBatch.messages.map((message) => message.id);
    const receiveCounts = receives.flatMap((batch) => batch.messages).filter((message) => batchIds.includes(message.id)).map((message) => message.receiveCount);
    expect(receiveCounts).toEqual([1, 1, 1, 1]);                                                // nenhuma recebida de novo: nenhum recebimento gasto
    expect(visibilityChanges.filter((change) => change.seconds === 0)).toHaveLength(0);         // nenhuma devolvida
    const extensions = visibilityChanges.filter((change) => change.seconds === 10);             // prazo cheio desta fila
    expect(extensions.length).toBeGreaterThanOrEqual(1);
    expect(extensions.slice(0, 3).map((change) => change.id)).toEqual(batchIds.slice(1));       // a primeira extensão cobre as três que não tinham começado
    expect(extensions[0]!.at - firstBatch.at).toBeLessThan(10_000);                             // antes de a visibilidade vencer sozinha
    const [counts] = await testSql()`select count(*)::int as total from wager_transactions where external_transaction_id in ${testSql()(externalIds)}`;
    expect(counts.total).toBe(4);                                                               // uma transação por mensagem
    const [wallet] = await testSql()`select balance from wallets where id = ${walletId}`;
    expect(wallet.balance).toBe("96.00");
    expect(await drainQueue(batchQueues.deadLetterQueueName, 300)).toHaveLength(0);
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
