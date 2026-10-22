// ====================================================================================================
//     OUTBOX: PUBLISHERS CONCORRENTES, RETRY, CRASH ANTES E DEPOIS DO COMMIT, CÓPIA SEGURA
// ====================================================================================================
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { EventPublisher } from "../../src/application/ports";
import type { OutboxMessage } from "../../src/domain/messaging/outbox-message";
import { SendMessageBatchCommand } from "@aws-sdk/client-sqs";                                 // a cópia publicada depois da janela de deduplicação
import { consumeEventsUntilIdle, createExampleConsumerTables, movedTotal } from "../support/example-event-consumer";
import { createTestQueues, createWallet, drainQueue, queueUrl, startInProcessApp, submitWager, testConfig, testSql, testSqsClient, waitUntil, type RunningApp, type TestQueues } from "../support/harness";
import { expectWalletInvariants } from "../support/invariants";

let queues: TestQueues;
let writerApp: RunningApp;                                                                      // grava eventos, sem publisher
const touchedWallets: string[] = [];
const offloadedApps: RunningApp[] = [];

const NO_BACKGROUND = { CONSUMER_ENABLED: "false", OUTBOX_PUBLISHER_ENABLED: "false", PENDING_WORKER_ENABLED: "false" };

// embrulha o publisher real e anota cada id que ele tentou publicar
function recordingPublisher(attemptedIds: string[], behaviour: { failNextCalls: number; crashAfterPublishOnce: boolean } = { failNextCalls: 0, crashAfterPublishOnce: false }) {
  return (real: EventPublisher): EventPublisher => ({
    async publishBatch(messages: OutboxMessage[]) {
      attemptedIds.push(...messages.map((message) => message.id));
      if (behaviour.failNextCalls > 0) {
        behaviour.failNextCalls = behaviour.failNextCalls - 1;
        return { publishedIds: [], failures: messages.map((message) => ({ id: message.id, error: "SQS fora do ar (injetado)" })) };
      }
      const outcome = await real.publishBatch(messages);
      if (behaviour.crashAfterPublishOnce) {
        behaviour.crashAfterPublishOnce = false;
        throw new Error("processo morreu depois de publicar e antes do commit (injetado)");    // a transação é desfeita, published_at não é gravado
      }
      return outcome;
    },
  });
}

async function unpublishedCount(): Promise<number> {
  const [row] = await testSql()`select count(*)::int as total from outbox_messages where published_at is null`;
  return row.total;
}

async function publishEverything(app: RunningApp): Promise<void> {
  while ((await app.application.outboxPublisher.publishOnce()) > 0) {}
}

beforeAll(async () => {
  queues = await createTestQueues("outbox");
  writerApp = await startInProcessApp(testConfig(queues, NO_BACKGROUND));
});

afterAll(async () => {
  await expectWalletInvariants(touchedWallets);
  for (const app of offloadedApps) await app.stop();
  await writerApp.stop();
});

async function generateEvents(wallets: number, betsPerWallet: number): Promise<void> {
  for (let walletIndex = 0; walletIndex < wallets; walletIndex++) {
    const { walletId, playerId } = await createWallet(writerApp.baseUrl, "100.00");
    touchedWallets.push(walletId);
    for (let betIndex = 0; betIndex < betsPerWallet; betIndex++) await submitWager(writerApp.baseUrl, { walletId, playerId, kind: "BET", amount: "1.00" });
  }
}

describe("publishers concorrentes sobre a mesma outbox", () => {
  test("dois publishers ao mesmo tempo: cada evento é tentado uma vez só e todos saem publicados", async () => {
    await publishEverything(writerApp);                                                         // começa com a outbox vazia
    await drainQueue(queues.eventsQueueName, 300);
    await generateEvents(6, 4);                                                                 // 6 × (2 da abertura + 4 × 2 das apostas) = 60 eventos
    expect(await unpublishedCount()).toBe(60);

    const attemptedByFirst: string[] = [];
    const attemptedBySecond: string[] = [];
    const first = await startInProcessApp(testConfig(queues, NO_BACKGROUND), { eventPublisher: recordingPublisher(attemptedByFirst) }, { background: false });
    const second = await startInProcessApp(testConfig(queues, NO_BACKGROUND), { eventPublisher: recordingPublisher(attemptedBySecond) }, { background: false });
    offloadedApps.push(first, second);

    await Promise.all([publishEverything(first), publishEverything(second)]);

    const allAttempts = [...attemptedByFirst, ...attemptedBySecond];
    expect(new Set(allAttempts).size).toBe(allAttempts.length);                                 // SKIP LOCKED: nenhuma linha pega pelos dois
    expect(allAttempts).toHaveLength(60);
    expect(attemptedByFirst.length).toBeGreaterThan(0);                                         // os dois trabalharam
    expect(attemptedBySecond.length).toBeGreaterThan(0);
    expect(await unpublishedCount()).toBe(0);

    const received = (await drainQueue(queues.eventsQueueName)).map((message) => JSON.parse(message.body).eventId);
    expect(new Set(received).size).toBe(60);
    expect(received).toHaveLength(60);
  });
});

describe("falhas na publicação", () => {
  test("SQS recusando: attempts sobe, last_error é gravado, next_attempt_at vai para frente, e depois publica", async () => {
    await publishEverything(writerApp);
    await drainQueue(queues.eventsQueueName, 300);
    await generateEvents(1, 0);                                                                 // 2 eventos da abertura
    const attempted: string[] = [];
    const behaviour = { failNextCalls: 1, crashAfterPublishOnce: false };
    const failing = await startInProcessApp(testConfig(queues, NO_BACKGROUND), { eventPublisher: recordingPublisher(attempted, behaviour) }, { background: false });
    offloadedApps.push(failing);

    await failing.application.outboxPublisher.publishOnce();
    const rows = await testSql()`select attempts, last_error, next_attempt_at, published_at from outbox_messages where published_at is null`;
    expect(rows).toHaveLength(2);
    for (const row of rows) {
      expect(row.attempts).toBe(1);
      expect(row.last_error).toContain("SQS fora do ar");
    }

    await waitUntil("publicar depois do backoff", async () => { await failing.application.outboxPublisher.publishOnce(); return (await unpublishedCount()) === 0; });   // o backoff tem teto de 1 s no teste
    await publishEverything(failing);
    expect(await unpublishedCount()).toBe(0);
    expect((await drainQueue(queues.eventsQueueName)).length).toBe(2);
  });

  test("publicou e morreu antes do commit: a outra tentativa publica o mesmo eventId e a cópia é descartada", async () => {
    await publishEverything(writerApp);
    await drainQueue(queues.eventsQueueName, 300);
    await generateEvents(1, 0);
    const attempted: string[] = [];
    const crashing = await startInProcessApp(testConfig(queues, NO_BACKGROUND), { eventPublisher: recordingPublisher(attempted, { failNextCalls: 0, crashAfterPublishOnce: true }) }, { background: false });
    offloadedApps.push(crashing);

    await expect(crashing.application.outboxPublisher.publishOnce()).rejects.toThrow("antes do commit");
    expect(await unpublishedCount()).toBe(2);                                                   // published_at não foi gravado
    await publishEverything(crashing);                                                          // republica os mesmos eventIds
    expect(new Set(attempted).size).toBe(2);
    expect(attempted).toHaveLength(4);                                                          // cada um foi enviado duas vezes

    const received = (await drainQueue(queues.eventsQueueName)).map((message) => JSON.parse(message.body).eventId);
    expect(received.sort()).toEqual([...new Set(attempted)].sort());                            // MessageDeduplicationId = eventId: a cópia não chegou
  });

  test("commit confirmado e o processo morre antes de publicar: outra instância publica", async () => {
    await publishEverything(writerApp);
    await drainQueue(queues.eventsQueueName, 300);
    const committer = await startInProcessApp(testConfig(queues, NO_BACKGROUND));             // sem publisher: o evento fica só na outbox
    const { walletId, playerId } = await createWallet(committer.baseUrl, "50.00");
    touchedWallets.push(walletId);
    await submitWager(committer.baseUrl, { walletId, playerId, kind: "BET", amount: "5.00" });
    await committer.stop();                                                                     // "morre" sem ter publicado
    expect(await unpublishedCount()).toBe(4);

    const survivor = await startInProcessApp(testConfig(queues, { CONSUMER_ENABLED: "false", PENDING_WORKER_ENABLED: "false" }));   // com o publisher em laço de fundo
    offloadedApps.push(survivor);
    await waitUntil("a outra instância publicar", async () => (await unpublishedCount()) === 0);
    const received = (await drainQueue(queues.eventsQueueName)).map((message) => JSON.parse(message.body));
    expect(received.filter((event) => event.aggregateId === walletId)).toHaveLength(4);
    await survivor.stop();                                                                      // senão o publisher dela pega os eventos do próximo teste
    offloadedApps.splice(offloadedApps.indexOf(survivor), 1);                                   // já parada, o afterAll não para de novo
  });
});

describe("publicação duplicada continua segura para o consumidor (passo 5 do cenário da outbox)", () => {
  // O publisher publica e morre antes do commit, e a nova tentativa só acontece depois da janela de
  // 5 minutos de deduplicação do FIFO: a fila aceita a cópia como mensagem nova. Para não esperar os
  // 5 minutos, a segunda publicação usa outro MessageDeduplicationId; o corpo (e o eventId) é o mesmo.
  function crashThenRepublishAfterDedupWindow() {
    let calls = 0;
    return (real: EventPublisher): EventPublisher => ({
      async publishBatch(messages: OutboxMessage[]) {
        calls = calls + 1;
        if (calls === 1) {
          await real.publishBatch(messages);
          throw new Error("processo morreu depois de publicar e antes do commit (injetado)");
        }
        await testSqsClient().send(new SendMessageBatchCommand({
          QueueUrl: await queueUrl(queues.eventsQueueName),
          Entries: messages.map((message) => ({ Id: message.id, MessageBody: JSON.stringify(message.payload), MessageGroupId: message.aggregateId, MessageDeduplicationId: `${message.id}-depois-da-janela` })),
        }));
        return { publishedIds: messages.map((message) => message.id), failures: [] };
      },
    });
  }

  test("cada evento chega duas vezes de verdade na fila, e o consumidor de exemplo aplica uma vez só pelo eventId", async () => {
    await publishEverything(writerApp);
    await drainQueue(queues.eventsQueueName, 300);
    const { walletId, playerId } = await createWallet(writerApp.baseUrl, "100.00");
    touchedWallets.push(walletId);
    await submitWager(writerApp.baseUrl, { walletId, playerId, kind: "BET", amount: "30.00" });
    await submitWager(writerApp.baseUrl, { walletId, playerId, kind: "WIN", amount: "12.50" });   // 6 eventos: 2 da abertura e 2 de cada operação
    expect(await unpublishedCount()).toBe(6);

    const republishing = await startInProcessApp(testConfig(queues, NO_BACKGROUND), { eventPublisher: crashThenRepublishAfterDedupWindow() }, { background: false });
    offloadedApps.push(republishing);
    await expect(republishing.application.outboxPublisher.publishOnce()).rejects.toThrow("antes do commit");
    expect(await unpublishedCount()).toBe(6);                                                   // published_at não foi gravado
    await publishEverything(republishing);                                                      // republica os mesmos eventos
    expect(await unpublishedCount()).toBe(0);

    await createExampleConsumerTables();
    const stats = await consumeEventsUntilIdle(queues.eventsQueueName);
    expect(stats).toEqual({ received: 12, applied: 6, duplicates: 6 });                         // a fila entregou as cópias; o consumidor ignorou todas
    const [ledger] = await testSql()`select sum(amount)::text as moved from wallet_ledger_entries where wallet_id = ${walletId}`;
    expect(await movedTotal(walletId)).toBe(ledger.moved);                                       // 100.00 + 30.00 + 12.50, uma vez cada
    expect(ledger.moved).toBe("142.50");
  });
});
