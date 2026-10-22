// ====================================================================================================
//     UNIT OF WORK E IDENTITY MAP DO MIKRORM: O QUE A PERSISTÊNCIA PROMETE, CONFERIDO NO BANCO REAL
// ====================================================================================================
// Os repositórios só registram as mudanças; o SQL sai no flush do commit. Aqui ficam provados: nada é
// escrito antes do commit, a ordem dos INSERTs vem das relações, a alteração usa o registro do
// Identity Map, o UPDATE da wallet leva o optimistic lock (version) e só acontece quando o saldo muda,
// e a violação de unicidade que aparece no flush continua levando à recuperação das cópias.
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { ConcurrentUpdateError } from "../../src/application/errors";
import { computeOpeningPayloadHash } from "../../src/application/wagering/payload-hash";
import type { Repositories } from "../../src/application/ports";
import { WagerTransactionProcessed, WalletBalanceChanged } from "../../src/domain/events";
import { FailureCode } from "../../src/domain/wagering/failure-code";
import { Money } from "../../src/domain/money";
import { OutboxMessage } from "../../src/domain/messaging/outbox-message";
import { Wallet } from "../../src/domain/wallet/wallet";
import { WagerTransaction, WagerTransactionStatus } from "../../src/domain/wagering/wager-transaction";
import { WalletRecord } from "../../src/infrastructure/persistence/records";
import { createTestQueues, createWallet, httpRequest, startInProcessApp, submitWager, testConfig, testSql, type RunningApp, type TestQueues } from "../support/harness";
import { expectWalletInvariants } from "../support/invariants";

let queues: TestQueues;
let app: RunningApp;
const touchedWallets: string[] = [];

// segura a primeira transação que chegar na outbox, antes do commit (e antes do flush)
const pause = { next: false };
let reachedOutbox: () => void = () => {};
let releaseCommit: () => void = () => {};

function withPausableOutbox(real: Repositories): Repositories {
  return {
    ...real,
    outbox: {
      ...real.outbox,
      insert: async (messages) => {
        await real.outbox.insert(messages);
        if (pause.next) {
          pause.next = false;
          reachedOutbox();
          await new Promise<void>((resolve) => { releaseCommit = resolve; });
        }
      },
      lockDueBatch: (now, limit) => real.outbox.lockDueBatch(now, limit),
      saveAttempt: (message) => real.outbox.saveAttempt(message),
      stats: (stuck) => real.outbox.stats(stuck),
    },
  };
}

async function walletRow(walletId: string) {
  const [row] = await testSql()`select balance, version, updated_at from wallets where id = ${walletId}`;
  return row;
}

async function metricValue(name: string, labels: string): Promise<number> {
  const metrics = await httpRequest(app.baseUrl, "GET", "/metrics");
  const line = (metrics.body as string).split("\n").find((candidate) => candidate.startsWith(`${name}${labels}`));
  return line ? Number(line.split(" ").at(-1)) : 0;
}

beforeAll(async () => {
  queues = await createTestQueues("uow");
  const config = testConfig(queues, { CONSUMER_ENABLED: "false", OUTBOX_PUBLISHER_ENABLED: "false", PENDING_WORKER_ENABLED: "false" });
  app = await startInProcessApp(config, { repositories: withPausableOutbox }, { background: false });
});

afterAll(async () => {
  await expectWalletInvariants(touchedWallets);
  await app.stop();
});

describe("flush no commit", () => {
  test("dentro da transação nada foi escrito ainda; no commit sai tudo, na ordem das chaves estrangeiras", async () => {
    const { database, repositories } = app.application;
    const now = new Date();
    const walletId = Bun.randomUUIDv7();
    const openingId = Bun.randomUUIDv7();
    const playerId = Bun.randomUUIDv7();
    const initialBalance = Money.from({ amount: "40.00", currency: "BRL" });
    const { wallet, openingEntry } = Wallet.open({ id: walletId, playerId, initialBalance, openingTransactionId: openingId, openingLedgerEntryId: Bun.randomUUIDv7(), openedAt: now });
    const opening = WagerTransaction.createOpening({ id: openingId, walletId, playerId, money: initialBalance, payloadHash: computeOpeningPayloadHash(walletId, initialBalance), correlationId: "uow-test", createdAt: now });
    opening.markProcessed(undefined, wallet.balance, now);

    const rowsSeenInsideTransaction = await database.run(async () => {
      const eventContext = () => ({ eventId: Bun.randomUUIDv7(), correlationId: "uow-test", causationId: "uow-test", occurredAt: now });
      await repositories.outbox.insert([OutboxMessage.enqueue(WagerTransactionProcessed.from(opening, eventContext())), OutboxMessage.enqueue(WalletBalanceChanged.from(wallet, openingEntry!, eventContext()))]);
      await repositories.ledger.insert(openingEntry!);                                          // registrados na ordem contrária à das FKs
      await repositories.transactions.insert(opening);
      await repositories.wallets.insert(wallet);
      const [seen] = await database.entityManager().execute<{ wallets: number; transactions: number; entries: number }[]>(
        "select (select count(*)::int from wallets where id = ?) as wallets, (select count(*)::int from wager_transactions where wallet_id = ?) as transactions, (select count(*)::int from wallet_ledger_entries where wallet_id = ?) as entries",
        [walletId, walletId, walletId],
      );
      return seen;
    });
    touchedWallets.push(walletId);

    expect(rowsSeenInsideTransaction).toEqual({ wallets: 0, transactions: 0, entries: 0 });  // a própria transação ainda não tinha escrito nada
    const [afterCommit] = await testSql()`select
        (select count(*)::int from wallets where id = ${walletId}) as wallets,
        (select count(*)::int from wager_transactions where wallet_id = ${walletId}) as transactions,
        (select count(*)::int from wallet_ledger_entries where wallet_id = ${walletId}) as entries`;
    expect(afterCommit).toEqual({ wallets: 1, transactions: 1, entries: 1 });                  // a wallet antes da transação, a transação antes do lançamento
    expect((await walletRow(walletId)).version).toBe(1);                                        // o banco grava a versão inicial (default) e o ORM lê de volta
  });
});

describe("Identity Map", () => {
  test("a mesma wallet lida duas vezes na transação é o mesmo registro, e alterar sem ter lido é recusado", async () => {
    const { walletId } = await createWallet(app.baseUrl, "10.00");
    touchedWallets.push(walletId);
    const { database, repositories } = app.application;

    const sameRecord = await database.run(async () => {
      await repositories.wallets.findByIdForUpdate(walletId);
      const loaded = database.loadedRecord(WalletRecord, walletId);
      const readAgain = await database.entityManager().findOne(WalletRecord, { id: walletId });
      return loaded === readAgain;
    });
    expect(sameRecord).toBe(true);

    const wallet = (await repositories.wallets.findById(walletId))!;                            // lida fora da transação abaixo
    await expect(database.run(() => repositories.wallets.updateBalance(wallet, wallet.version))).rejects.toThrow("não foi carregado nesta transação");
  });
});

describe("optimistic lock da wallet (version: true)", () => {
  test("se outra transação mudou o saldo depois da leitura, o UPDATE do flush não acha a versão e nada é gravado", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    const { database, repositories } = app.application;

    const staleUpdate = database.run(async () => {
      const wallet = (await repositories.wallets.findById(walletId))!;                          // sem lock, de propósito: é o caso que a version pega
      const otherBet = await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "10.00" });   // outra transação muda o saldo e faz commit
      expect(otherBet.body.status).toBe("PROCESSED");
      const versionRead = wallet.version;
      wallet.debit(Money.from({ amount: "5.00", currency: "BRL" }), { transactionId: Bun.randomUUIDv7(), ledgerEntryId: Bun.randomUUIDv7(), at: new Date() });
      await repositories.wallets.updateBalance(wallet, versionRead);
    });

    await expect(staleUpdate).rejects.toBeInstanceOf(ConcurrentUpdateError);
    const row = await walletRow(walletId);
    expect(row.balance).toBe("90.00");                                                          // só a outra aposta valeu
    expect(row.version).toBe(2);
  });

  test("a version só sobe quando o saldo muda: LOSS, rejeição e leitura com lock sem mudança não mexem nela", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "50.00");
    touchedWallets.push(walletId);
    const initial = await walletRow(walletId);
    expect(initial.version).toBe(1);

    await submitWager(app.baseUrl, { walletId, playerId, kind: "LOSS", amount: "0.00" });
    expect((await walletRow(walletId)).version).toBe(1);

    const bet = await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "20.00" });
    expect(bet.body.status).toBe("PROCESSED");
    const afterBet = await walletRow(walletId);
    expect(afterBet).toMatchObject({ balance: "30.00", version: 2 });                          // um movimento, uma versão
    expect((await httpRequest(app.baseUrl, "GET", `/wallets/${walletId}`)).body.version).toBe(2);   // o domínio e o banco contam igual

    const rejected = await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "99.00" });
    expect(rejected.body.failureCode).toBe("INSUFFICIENT_FUNDS");
    expect((await walletRow(walletId)).version).toBe(2);

    const { database, repositories } = app.application;
    await database.run(async () => { await repositories.wallets.findByIdForUpdate(walletId); });   // travou, leu e não mudou nada
    const untouched = await walletRow(walletId);
    expect(untouched.version).toBe(2);
    expect(untouched.updated_at).toEqual(afterBet.updated_at);                                  // nenhum UPDATE saiu
  });
});

describe("pendente decidida por outra transação depois da leitura", () => {
  test("o UPDATE do flush esbarra no trigger que protege a transação terminal, e a primeira decisão fica", async () => {
    const { walletId, playerId } = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(walletId);
    const pendingResponse = await submitWager(app.baseUrl, { walletId, playerId, kind: "REFUND", amount: "5.00", reference: `bet-${Bun.randomUUIDv7()}` });
    expect(pendingResponse.status).toBe(202);
    const pendingId = pendingResponse.body.transactionId as string;
    const { database, repositories } = app.application;

    const staleDecision = database.run(async () => {
      const pending = (await repositories.transactions.findById(pendingId))!;                   // sem lock, de propósito
      await testSql()`update wager_transactions set status = 'REJECTED', failure_code = 'REFERENCE_NOT_FOUND', updated_at = now() where id = ${pendingId}`;   // outra decisão chegou antes
      pending.fail(FailureCode.ProcessingFailed, new Date());
      await repositories.transactions.updateDecision(pending);
    });

    await expect(staleDecision).rejects.toThrow("terminal");
    const [row] = await testSql()`select status, failure_code from wager_transactions where id = ${pendingId}`;
    expect(row).toEqual({ status: WagerTransactionStatus.Rejected, failure_code: "REFERENCE_NOT_FOUND" });   // a primeira decisão ficou
  });
});

describe("violação de unicidade no flush", () => {
  async function raceOnSameIdentity(secondDiffers: { sameKey: boolean }) {
    const first = await createWallet(app.baseUrl, "100.00");
    const second = await createWallet(app.baseUrl, "100.00");
    touchedWallets.push(first.walletId, second.walletId);
    const externalTransactionId = `bet-${Bun.randomUUIDv7()}`;
    const sharedKey = `provider-a:${externalTransactionId}`;

    const outboxReached = new Promise<void>((resolve) => { reachedOutbox = resolve; });
    pause.next = true;
    const firstResponse = submitWager(app.baseUrl, { ...first, kind: "BET", amount: "10.00", externalTransactionId }, sharedKey);
    await outboxReached;                                                                        // a primeira decidiu e está parada antes do commit

    const secondKey = secondDiffers.sameKey ? sharedKey : `other-key-${Bun.randomUUIDv7()}`;
    const secondResponse = await submitWager(app.baseUrl, { ...second, kind: "BET", amount: "10.00", externalTransactionId }, secondKey);   // outra wallet, outro lock: passa e faz commit
    expect(secondResponse.status).toBe(200);

    const conflictsBefore = await metricValue("wagering_concurrency_conflicts_total", '{type="unique_violation"}');
    releaseCommit();                                                                            // o flush da primeira esbarra na UNIQUE
    const loser = await firstResponse;
    expect(await metricValue("wagering_concurrency_conflicts_total", '{type="unique_violation"}')).toBe(conflictsBefore + 1);
    const [rows] = await testSql()`select count(*)::int as total from wager_transactions where external_transaction_id = ${externalTransactionId}`;
    expect(rows.total).toBe(1);
    expect((await walletRow(first.walletId)).balance).toBe("100.00");                           // a perdedora não gravou nada
    return loser;
  }

  test("mesma key em wallets diferentes: a perdedora consulta de novo e vê o conflito (hash diferente)", async () => {
    const loser = await raceOnSameIdentity({ sameKey: true });
    expect(loser.status).toBe(409);
    expect(loser.body.code).toBe("IDEMPOTENCY_KEY_CONFLICT");
  });

  test("mesmo (providerId, externalTransactionId) com outra key: a perdedora vê o id externo já usado", async () => {
    const loser = await raceOnSameIdentity({ sameKey: false });
    expect(loser.status).toBe(409);
    expect(loser.body.code).toBe("EXTERNAL_TRANSACTION_ID_CONFLICT");
  });
});
