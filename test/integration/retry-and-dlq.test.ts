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
import { createTestQueues, createWallet, drainQueue, holdWalletLock, httpRequest, queueUrl, sendWagerMessage, startInProcessApp, submitWager, testConfig, testSql, testSqsClient, wagerMessage, waitUntil, type RunningApp, type TestQueues } from "../support/harness";
import { expectWalletInvariants } from "../support/invariants";
import { newQueueRecord, receiveCountsOf, recordingQueue, releasesOf, type QueueRecord } from "../support/recording-queue";

let queues: TestQueues;
let app: RunningApp;
const queueRecord = newQueueRecord();                                                           // recebimentos e visibilidade da fila do arquivo
const touchedWallets: string[] = [];
const transientFailuresLeftByWallet = new Map<string, number>();                               // walletId -> quantas falhas ainda injetar
const slowLockByWallet = new Map<string, number>();                                            // walletId -> quanto a tentativa demora antes do lock
const lockAttemptsByWallet = new Map<string, number>();
const neverEndingReplayReads = new Set<string>();                                              // idempotencyKey cuja leitura de replay nunca termina
let hangingReplayReads = 0;                                                                     // quantas dessas leituras já começaram

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

// a leitura de replay é a primeira chamada ao banco, fora da transação; pendurada, a mensagem não termina nunca
function withHangingReplayRead(real: Repositories): Repositories {
  const transactions = real.transactions;
  return {
    ...real,
    transactions: {
      findById: (transactionId) => transactions.findById(transactionId),
      findByIdForUpdate: (transactionId) => transactions.findByIdForUpdate(transactionId),
      findByProviderAndExternalId: (providerId, externalTransactionId) => transactions.findByProviderAndExternalId(providerId, externalTransactionId),
      hasProcessedReversal: (referenceTransactionId) => transactions.hasProcessedReversal(referenceTransactionId),
      lockNextDuePending: (now) => transactions.lockNextDuePending(now),
      countPending: () => transactions.countPending(),
      insert: (transaction) => transactions.insert(transaction),
      updateDecision: (transaction) => transactions.updateDecision(transaction),
      findByIdempotencyKey: async (idempotencyKey) => {
        if (!neverEndingReplayReads.has(idempotencyKey)) return transactions.findByIdempotencyKey(idempotencyKey);
        hangingReplayReads = hangingReplayReads + 1;
        return new Promise<never>(() => {});                                                    // como uma consulta numa conexão pendurada: sem resposta e sem erro
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

// o waitUntil dos testes da wallet presa: se a espera estourar, imprime o que já foi anotado, para a falha vir com diagnóstico
function reportingWait(record: QueueRecord, walletIds: string[]): typeof waitUntil {
  return async (description, condition, timeoutMilliseconds) => {
    try {
      return await waitUntil(description, condition, timeoutMilliseconds);
    } catch (error) {
      const lockAttempts = walletIds.map((walletId) => ({ walletId, attempts: lockAttemptsByWallet.get(walletId) ?? 0 }));   // tentativas de lock de cada wallet do teste
      console.log(`espera estourada (${description}):`, JSON.stringify({ now: performance.now(), receives: record.receives, visibilityChanges: record.visibilityChanges, lockAttempts }, null, 2));
      throw error;
    }
  };
}

// Uma instância só, com fila própria e orçamento longo, e a wallet A travada por fora, com a mensagem
// dela já tentando de novo: a primeira tentativa estourou o lock_timeout de 2 s dos testes.
async function stuckWalletScenario(extra: Record<string, string> = {}) {
  const stuck = await createWallet(app.baseUrl, "100.00");
  const free = await createWallet(app.baseUrl, "100.00");
  touchedWallets.push(stuck.walletId, free.walletId);
  const ownQueues = await createTestQueues("inflight");                                         // fila própria: o consumidor do arquivo não pega estas
  const record = newQueueRecord();
  const settings = { CONSUMER_RETRY_BUDGET_MS: "60000", OUTBOX_PUBLISHER_ENABLED: "false", PENDING_WORKER_ENABLED: "false", ...extra };   // A não chega à DLQ durante o teste
  const consumerApp = await startInProcessApp(testConfig(ownQueues, settings), { repositories: withTransientFailures, inputQueue: recordingQueue(record) });
  const lock = await holdWalletLock(stuck.walletId);
  const waitOrReport = reportingWait(record, [stuck.walletId, free.walletId]);                  // as esperas do cenário e dos dois testes
  const stuckMessage = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId: stuck.walletId, playerId: stuck.playerId, kind: "BET", amount: "1.00" });
  await sendWagerMessage(ownQueues.inputQueueName, stuckMessage);
  await waitOrReport("a mensagem da wallet A tentando de novo", () => (lockAttemptsByWallet.get(stuck.walletId) ?? 0) >= 2, 30_000);   // a segunda tentativa já começou; costuma levar 2 a 3 s
  const freeMessage = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId: free.walletId, playerId: free.playerId, kind: "BET", amount: "5.00" });
  return { stuck, ownQueues, record, consumerApp, lock, stuckMessage, freeMessage, waitOrReport };
}

async function processedCount(externalTransactionIds: string[]): Promise<number> {
  const [row] = await testSql()`select count(*)::int as total from wager_transactions where external_transaction_id in ${testSql()(externalTransactionIds)} and status = 'PROCESSED'`;
  return row.total;
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

  // Se a busca só saísse de novo quando o lote inteiro terminasse, a mensagem de uma wallet presa, tentando
  // de novo dentro do orçamento, seguraria as mensagens de todas as outras wallets da instância.
  test("uma wallet presa não segura as outras: com a mensagem da wallet A tentando de novo, a da wallet B, livre, vira transação em poucos segundos, e a seguinte de A só chega depois da primeira", async () => {
    const { stuck, ownQueues, record, consumerApp, lock, stuckMessage, freeMessage, waitOrReport } = await stuckWalletScenario();
    const nextStuckMessage = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId: stuck.walletId, playerId: stuck.playerId, kind: "BET", amount: "2.00" });
    let freeMilliseconds = Number.POSITIVE_INFINITY;
    let stuckProcessedWhileLocked = -1;
    let nextStuckReceivesWhileLocked = -1;
    try {
      await sendWagerMessage(ownQueues.inputQueueName, nextStuckMessage);                       // a seguinte de A: o FIFO não entrega com a primeira em mãos
      const sentAt = performance.now();
      await sendWagerMessage(ownQueues.inputQueueName, freeMessage);
      await waitOrReport("a mensagem da wallet B processada", async () => (await processedCount([freeMessage.data.externalTransactionId])) === 1, 15_000);
      freeMilliseconds = performance.now() - sentAt;
      stuckProcessedWhileLocked = await processedCount([stuckMessage.data.externalTransactionId]);
      nextStuckReceivesWhileLocked = receiveCountsOf(record, nextStuckMessage.messageId).length;
    } finally {
      await lock.release();                                                                     // solta A mesmo se B não passou, para o teste não ficar pendurado
    }
    const stuckIds = [stuckMessage.data.externalTransactionId, nextStuckMessage.data.externalTransactionId];
    await waitOrReport("as duas de A processadas", async () => (await processedCount(stuckIds)) === 2);
    await waitOrReport("fila vazia", () => queueIsEmpty(ownQueues.inputQueueName));
    await consumerApp.stop();

    expect(freeMilliseconds).toBeLessThan(5_000);                                               // B não esperou A
    expect(stuckProcessedWhileLocked).toBe(0);                                                  // A continuava presa enquanto B passou
    expect(nextStuckReceivesWhileLocked).toBe(0);                                               // ordem por wallet: a seguinte de A não veio com a primeira em mãos
    for (const message of [stuckMessage, nextStuckMessage, freeMessage]) {
      expect(receiveCountsOf(record, message.messageId)).toEqual([1]);                          // nenhum recebimento gasto
      expect(releasesOf(record, message.messageId)).toBe(0);
    }
    const stuckLedger = await testSql()`select t.external_transaction_id from wallet_ledger_entries e join wager_transactions t on t.id = e.transaction_id where e.wallet_id = ${stuck.walletId} and t.external_transaction_id in ${testSql()(stuckIds)} order by e.wallet_version`;
    expect(stuckLedger.map((row: { external_transaction_id: string }) => row.external_transaction_id)).toEqual(stuckIds);   // A em ordem
    expect(await drainQueue(ownQueues.deadLetterQueueName, 300)).toHaveLength(0);
  });

  test("o limite de mensagens em mãos é respeitado: com o limite em 1 e a mensagem da wallet A em mãos, a da wallet B só é recebida depois que A sai", async () => {
    const { ownQueues, record, consumerApp, lock, stuckMessage, freeMessage, waitOrReport } = await stuckWalletScenario({ CONSUMER_MAX_IN_FLIGHT_MESSAGES: "1" });
    let freeReceivesWhileLocked = -1;
    try {
      await sendWagerMessage(ownQueues.inputQueueName, freeMessage);
      await Bun.sleep(3_000);                                                                   // tempo de sobra para B ser buscada, se houvesse vaga
      freeReceivesWhileLocked = receiveCountsOf(record, freeMessage.messageId).length;
    } finally {
      await lock.release();
    }
    await waitOrReport("as duas processadas", async () => (await processedCount([stuckMessage.data.externalTransactionId, freeMessage.data.externalTransactionId])) === 2);
    await waitOrReport("fila vazia", () => queueIsEmpty(ownQueues.inputQueueName));
    await consumerApp.stop();

    expect(freeReceivesWhileLocked).toBe(0);                                                    // sem vaga, sem busca
    expect(record.receives.every((batch) => batch.messages.length <= 1)).toBe(true);           // cada busca pede só o que cabe
    expect(receiveCountsOf(record, stuckMessage.messageId)).toEqual([1]);
    expect(receiveCountsOf(record, freeMessage.messageId)).toEqual([1]);
    expect(await drainQueue(ownQueues.deadLetterQueueName, 300)).toHaveLength(0);
  });

  // Com o orçamento de 1 s, a espera de 2,5 s e a visibilidade de 3 s dos testes, o teto de tempo em mãos
  // é de 1 + 2,5 + (2,5 + 3) = 9 s. Sem ele, o vigia estenderia o prazo até a instância reiniciar.
  test("chamada que nunca termina: passado o teto de tempo em mãos, o prazo deixa de ser estendido, a mensagem volta para a fila e outra instância a processa", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    const hangQueues = await createTestQueues("hang");                                         // fila própria: o consumidor do arquivo não pega esta
    const stuckRecord = newQueueRecord();
    const otherRecord = newQueueRecord();
    const settings = { CONSUMER_RETRY_BUDGET_MS: "1000", SHUTDOWN_IN_FLIGHT_TIMEOUT_MS: "1000", OUTBOX_PUBLISHER_ENABLED: "false", PENDING_WORKER_ENABLED: "false" };
    const stuckSettings = { ...settings, CONSUMER_MAX_IN_FLIGHT_MESSAGES: "1" };               // a vaga fica com a pendurada: esta instância não busca a mensagem de volta
    const inHandsLimitMilliseconds = 1_000 + 2_500 + 2_500 + 3_000;
    const message = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "3.00" });
    neverEndingReplayReads.add(message.data.idempotencyKey);
    const hangingBefore = hangingReplayReads;

    const stuckApp = await startInProcessApp(testConfig(hangQueues, stuckSettings), { repositories: withHangingReplayRead, inputQueue: recordingQueue(stuckRecord) });
    await sendWagerMessage(hangQueues.inputQueueName, message);
    await waitUntil("a leitura de replay pendurada", () => hangingReplayReads > hangingBefore);
    const otherApp = await startInProcessApp(testConfig(hangQueues, settings), { inputQueue: recordingQueue(otherRecord) });   // a outra instância, sem nada pendurado
    await waitUntil("processada pela outra instância", async () => (await transactionRows(message.data.externalTransactionId)).length > 0 && (await queueIsEmpty(hangQueues.inputQueueName)), 30_000);
    await otherApp.stop();
    await stuckApp.stop();                                                                      // a mensagem pendurada não termina: o desligamento desiste depois de 1 s

    const firstReceiveAt = stuckRecord.receives.find((batch) => batch.messages.some((received) => received.messageId === message.messageId))!.at;
    const secondReceiveAt = otherRecord.receives.find((batch) => batch.messages.some((received) => received.messageId === message.messageId))!.at;
    const extensions = stuckRecord.visibilityChanges.filter((change) => change.messageId === message.messageId && change.seconds === 3);
    expect(extensions.length).toBeGreaterThanOrEqual(2);                                        // estendida enquanto estava dentro do teto
    expect(extensions.at(-1)!.at - firstReceiveAt).toBeLessThan(inHandsLimitMilliseconds);     // nenhuma extensão depois do teto
    expect(secondReceiveAt - firstReceiveAt).toBeGreaterThanOrEqual(inHandsLimitMilliseconds); // só voltou depois do teto, não no primeiro prazo de 3 s
    expect(releasesOf(stuckRecord, message.messageId)).toBe(0);                                 // não foi devolvida: voltou porque o prazo venceu
    expect(receiveCountsOf(stuckRecord, message.messageId)).toEqual([1]);
    expect(receiveCountsOf(otherRecord, message.messageId)).toEqual([2]);                       // a volta pelo prazo gasta um recebimento, o certo para uma chamada travada
    const [counts] = await testSql()`select count(*)::int as total from wager_transactions where external_transaction_id = ${message.data.externalTransactionId} and status = 'PROCESSED'`;
    expect(counts.total).toBe(1);
    expect(await drainQueue(hangQueues.deadLetterQueueName, 300)).toHaveLength(0);
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
