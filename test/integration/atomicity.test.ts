// ====================================================================================================
//      ATOMICIDADE ENTRE WALLET, LEDGER, INBOX E OUTBOX, E EVENTO PUBLICADO SÓ DEPOIS DO COMMIT
// ====================================================================================================
// A falha entra pela composição das portas: a outbox real é embrulhada por uma que força o flush do
// Unit of Work e lança erro (as linhas já foram escritas na transação, falta só o commit), ou que para
// antes do commit. Banco e fila continuam reais.
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import type { Repositories } from "../../src/application/ports";
import type { WagerCommand } from "../../src/application/wager-command";
import { Money } from "../../src/domain/money";
import { WagerTransactionKind } from "../../src/domain/wager-transaction";
import { createTestQueues, createWallet, drainQueue, startInProcessApp, submitWager, testConfig, testSql, type RunningApp, type TestQueues } from "../support/harness";
import { expectWalletInvariants } from "../support/invariants";

let queues: TestQueues;
let app: RunningApp;
let publisherApp: RunningApp;
const touchedWallets: string[] = [];

const injection = { failAfterFlush: false, pauseBeforeCommit: false };
let reachedOutbox: () => void = () => {};
let releaseCommit: () => void = () => {};

function withInjectedOutbox(real: Repositories): Repositories {
  return {
    ...real,
    outbox: {
      ...real.outbox,
      insert: async (messages) => {
        await real.outbox.insert(messages);
        if (injection.failAfterFlush) {
          await app.application.database.transactionEntityManager().flush();                  // wallet, transação, lançamento e outbox já escritos no banco
          throw new Error("falha injetada depois do flush e antes do commit");
        }
        if (injection.pauseBeforeCommit) {
          reachedOutbox();
          await new Promise<void>((resolve) => { releaseCommit = resolve; });                   // segura a transação aberta, antes do commit
        }
      },
      lockDueBatch: (now, limit) => real.outbox.lockDueBatch(now, limit),
      saveAttempt: (message) => real.outbox.saveAttempt(message),
      stats: (stuck) => real.outbox.stats(stuck),
    },
  };
}

async function snapshot(walletId: string) {
  const sql = testSql();
  const [wallet] = await sql`select balance, version from wallets where id = ${walletId}`;
  const [counts] = await sql`select
      (select count(*)::int from wager_transactions where wallet_id = ${walletId}) as transactions,
      (select count(*)::int from wallet_ledger_entries where wallet_id = ${walletId}) as entries,
      (select count(*)::int from outbox_messages where aggregate_id = ${walletId}) as events`;
  return { ...wallet, ...counts };
}

beforeAll(async () => {
  queues = await createTestQueues("atomic");
  const config = testConfig(queues, { CONSUMER_ENABLED: "false", OUTBOX_PUBLISHER_ENABLED: "false", PENDING_WORKER_ENABLED: "false" });
  app = await startInProcessApp(config, { repositories: withInjectedOutbox });
  publisherApp = await startInProcessApp(config, {}, { background: false });                  // outro pool: só para rodar o publisher à mão
});

afterAll(async () => {
  await expectWalletInvariants(touchedWallets);
  await app.stop();
  await publisherApp.stop();
});

describe("atomicidade", () => {
  test("HTTP: erro com tudo já escrito na transação (depois do flush) e antes do commit desfaz tudo (transação, saldo, version, ledger, outbox)", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    const before = await snapshot(walletId);

    injection.failAfterFlush = true;
    const response = await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "40.00" });
    injection.failAfterFlush = false;

    expect(response.status).toBe(500);
    expect(response.body.code).toBe("INTERNAL_ERROR");
    expect(await snapshot(walletId)).toEqual(before);                                           // nada ficou pela metade
  });

  test("fila: o mesmo erro também desfaz a linha da inbox (a mensagem pode ser reprocessada)", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    const before = await snapshot(walletId);
    const command: WagerCommand = {
      providerId: "provider-a", externalTransactionId: `ext-${Bun.randomUUIDv7()}`, playerId, walletId, roundId: "round-1", gameId: "game-1",
      kind: WagerTransactionKind.Bet, money: Money.from({ amount: "40.00", currency: "BRL" }), referenceExternalTransactionId: undefined,
    };
    const messageId = `msg-${Bun.randomUUIDv7()}`;
    const context = { channel: "sqs" as const, correlationId: messageId, causationId: messageId, inbox: { consumerName: "atomicity-test", messageId } };

    injection.failAfterFlush = true;
    await expect(app.application.processWager.execute(command, `key-${messageId}`, context)).rejects.toThrow("falha injetada");
    injection.failAfterFlush = false;

    expect(await snapshot(walletId)).toEqual(before);
    const [inbox] = await testSql()`select count(*)::int as total from inbox_messages where message_id = ${messageId}`;
    expect(inbox.total).toBe(0);

    const retried = await app.application.processWager.execute(command, `key-${messageId}`, context);   // a nova tentativa processa uma vez
    expect(retried.kind).toBe("decided");
    const [inboxAfter] = await testSql()`select count(*)::int as total from inbox_messages where message_id = ${messageId}`;
    expect(inboxAfter.total).toBe(1);
  });
});

describe("evento só depois do commit", () => {
  test("com a transação parada antes do commit, o publisher não enxerga o evento; depois do commit, publica", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    while ((await publisherApp.application.outboxPublisher.publishOnce()) > 0) {}               // limpa o que já estava pendente
    await drainQueue(queues.eventsQueueName, 500);

    const outboxReached = new Promise<void>((resolve) => { reachedOutbox = resolve; });
    injection.pauseBeforeCommit = true;
    const pendingResponse = submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "10.00" });
    await outboxReached;                                                                        // eventos inseridos, commit ainda não

    const publishedWhileOpen = await publisherApp.application.outboxPublisher.publishOnce();
    expect(publishedWhileOpen).toBe(0);                                                         // linha não confirmada é invisível
    expect(await drainQueue(queues.eventsQueueName, 500)).toHaveLength(0);

    injection.pauseBeforeCommit = false;
    releaseCommit();
    const response = await pendingResponse;
    expect(response.status).toBe(200);

    expect(await publisherApp.application.outboxPublisher.publishOnce()).toBe(2);              // WagerTransactionProcessed e WalletBalanceChanged
    const published = (await drainQueue(queues.eventsQueueName)).map((message) => JSON.parse(message.body));
    expect(published.map((event) => event.eventType).sort()).toEqual(["WagerTransactionProcessed", "WalletBalanceChanged"]);
    expect(published.every((event) => event.aggregateId === walletId)).toBe(true);
    const balanceChanged = published.find((event) => event.eventType === "WalletBalanceChanged");
    expect(balanceChanged.data).toMatchObject({ walletId, direction: "DEBIT", money: { amount: "10.00", currency: "BRL" }, balanceAfter: { amount: "90.00", currency: "BRL" }, walletVersion: 2 });
  });
});
