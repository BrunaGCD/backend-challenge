// ====================================================================================================
//        API HTTP: IDEMPOTÊNCIA PERSISTENTE, REPLAY, CONFLITOS, REGRAS E O MAPA DE STATUS
// ====================================================================================================
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { createTestQueues, createWallet, httpRequest, startInProcessApp, submitWager, testConfig, testSql, wagerBody, waitUntil, type RunningApp, type TestQueues } from "../support/harness";
import { expectWalletInvariants } from "../support/invariants";

let queues: TestQueues;
let app: RunningApp;
const touchedWallets: string[] = [];

async function newWallet(initialBalance = "100.00", currency = "BRL") {
  const wallet = await createWallet(app.baseUrl, initialBalance, currency);
  touchedWallets.push(wallet.walletId);
  return wallet;
}

async function countRows(table: "wager_transactions" | "wallet_ledger_entries" | "outbox_messages", walletId: string): Promise<number> {
  const sql = testSql();
  const column = table === "outbox_messages" ? "aggregate_id" : "wallet_id";
  const [row] = await sql.unsafe(`select count(*)::int as total from ${table} where ${column} = $1`, [walletId]);
  return row.total;
}

beforeAll(async () => {
  queues = await createTestQueues("http");
  app = await startInProcessApp(testConfig(queues, { CONSUMER_ENABLED: "false" }));
});

afterAll(async () => {
  await expectWalletInvariants(touchedWallets);
  await app.stop();
});

describe("POST /wallets", () => {
  test("cria com OPENING, lançamento e os dois eventos na mesma transação", async () => {
    const { walletId, playerId } = await newWallet("1000.00");
    const wallet = await httpRequest(app.baseUrl, "GET", `/wallets/${walletId}`);
    expect(wallet.status).toBe(200);
    expect(wallet.body).toMatchObject({ id: walletId, playerId, balance: { amount: "1000.00", currency: "BRL" }, version: 1 });
    expect(typeof wallet.body.updatedAt).toBe("string");
    const [opening] = await testSql()`select kind, status, provider_id, round_id, idempotency_key from wager_transactions where wallet_id = ${walletId}`;
    expect(opening).toEqual({ kind: "OPENING", status: "PROCESSED", provider_id: null, round_id: null, idempotency_key: `opening:${walletId}` });
    const events = await testSql()`select event_type from outbox_messages where aggregate_id = ${walletId} order by event_type`;
    expect(events.map((event: any) => event.event_type)).toEqual(["WagerTransactionProcessed", "WalletBalanceChanged"]);
  });

  test("saldo inicial 0.00: sem OPENING, sem lançamento, sem evento", async () => {
    const { walletId } = await newWallet("0.00");
    expect(await countRows("wager_transactions", walletId)).toBe(0);
    expect(await countRows("wallet_ledger_entries", walletId)).toBe(0);
    expect(await countRows("outbox_messages", walletId)).toBe(0);
  });

  test("wallet duplicada para o mesmo player e moeda: 409 WALLET_ALREADY_EXISTS com o walletId existente", async () => {
    const { walletId, playerId } = await newWallet("10.00");
    const duplicate = await httpRequest(app.baseUrl, "POST", "/wallets", { playerId, initialBalance: { amount: "5.00", currency: "BRL" } });
    expect(duplicate.status).toBe(409);
    expect(duplicate.headers.get("content-type")).toContain("application/problem+json");
    expect(duplicate.body).toMatchObject({ status: 409, code: "WALLET_ALREADY_EXISTS", walletId });
  });

  test("payload inválido: 400 VALIDATION_ERROR com os campos", async () => {
    const response = await httpRequest(app.baseUrl, "POST", "/wallets", { playerId: "nao-e-uuid", initialBalance: { amount: "10.5", currency: "BRL" }, extra: 1 });
    expect(response.status).toBe(400);
    expect(response.body.code).toBe("VALIDATION_ERROR");
    expect(response.body.errors.map((error: any) => error.field).sort()).toEqual(["extra", "initialBalance", "playerId"]);
  });
});

describe("POST /wagering/transactions: idempotência", () => {
  test("requisição idêntica devolve a mesma resposta com idempotentReplay e o saldo observado na época", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    const bet = { walletId, playerId, kind: "BET", amount: "25.00", externalTransactionId: `bet-${Bun.randomUUIDv7()}` };
    const first = await submitWager(app.baseUrl, bet);
    expect(first.status).toBe(200);
    expect(first.body).toEqual({ transactionId: first.body.transactionId, status: "PROCESSED", balance: { amount: "75.00", currency: "BRL" }, idempotentReplay: false });

    await submitWager(app.baseUrl, { walletId, playerId, kind: "WIN", amount: "50.00" });   // o saldo muda depois
    const replay = await submitWager(app.baseUrl, bet);
    expect(replay.status).toBe(200);
    expect(replay.body).toEqual({ ...first.body, idempotentReplay: true });                  // saldo da época, não o atual
    expect(await countRows("wallet_ledger_entries", walletId)).toBe(3);                         // abertura, aposta e prêmio: nada duplicado
  });

  test("a idempotência é persistente: sobrevive a reiniciar a aplicação", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    const bet = { walletId, playerId, kind: "BET", amount: "10.00", externalTransactionId: `bet-${Bun.randomUUIDv7()}` };
    const first = await submitWager(app.baseUrl, bet);
    await app.stop();
    app = await startInProcessApp(testConfig(queues, { CONSUMER_ENABLED: "false" }));          // nova instância, nada em memória
    const replay = await submitWager(app.baseUrl, bet);
    expect(replay.body).toEqual({ ...first.body, idempotentReplay: true });
  });

  test("mesma key com payload diferente: 409 IDEMPOTENCY_KEY_CONFLICT e nada gravado", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    const externalTransactionId = `bet-${Bun.randomUUIDv7()}`;
    await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "10.00", externalTransactionId }, "shared-key-1");
    const transactionsBefore = await countRows("wager_transactions", walletId);
    const conflict = await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "11.00", externalTransactionId }, "shared-key-1");
    expect(conflict.status).toBe(409);
    expect(conflict.body.code).toBe("IDEMPOTENCY_KEY_CONFLICT");
    expect(await countRows("wager_transactions", walletId)).toBe(transactionsBefore);
  });

  test("mesma key vinda de outro providerId é conflito, não replay", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    const externalTransactionId = `bet-${Bun.randomUUIDv7()}`;
    await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "10.00", externalTransactionId }, "shared-key-2");
    const conflict = await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "10.00", externalTransactionId, providerId: "provider-b" }, "shared-key-2");
    expect(conflict.status).toBe(409);
    expect(conflict.body.code).toBe("IDEMPOTENCY_KEY_CONFLICT");
  });

  test("mesmo (providerId, externalTransactionId) com outra key: 409 EXTERNAL_TRANSACTION_ID_CONFLICT", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    const externalTransactionId = `bet-${Bun.randomUUIDv7()}`;
    await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "10.00", externalTransactionId }, "key-a");
    const conflict = await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "10.00", externalTransactionId }, "key-b");
    expect(conflict.status).toBe(409);
    expect(conflict.body.code).toBe("EXTERNAL_TRANSACTION_ID_CONFLICT");
  });

  test("header ausente: 400 IDEMPOTENCY_KEY_MISSING; header fora do formato: 400 VALIDATION_ERROR", async () => {
    const { walletId, playerId } = await newWallet("10.00");
    const body = wagerBody({ walletId, playerId, kind: "BET", amount: "1.00" });
    const missing = await httpRequest(app.baseUrl, "POST", "/wagering/transactions", body);
    expect(missing.status).toBe(400);
    expect(missing.body.code).toBe("IDEMPOTENCY_KEY_MISSING");
    const tooLong = await httpRequest(app.baseUrl, "POST", "/wagering/transactions", body, { "Idempotency-Key": "x".repeat(256) });
    expect(tooLong.status).toBe(400);
    expect(tooLong.body.code).toBe("VALIDATION_ERROR");
  });
});

describe("POST /wagering/transactions: validação da entrada (400, nada gravado)", () => {
  const invalidCases: [string, (body: Record<string, any>) => void, string][] = [
    ["valor sem 2 casas", (body) => { body.money.amount = "25.5"; }, "money"],
    ["valor com 3 casas", (body) => { body.money.amount = "10.005"; }, "money"],
    ["valor como number", (body) => { body.money.amount = 25; }, "money"],
    ["valor negativo", (body) => { body.money.amount = "-1.00"; }, "money"],
    ["notação científica", (body) => { body.money.amount = "1e3"; }, "money"],
    ["moeda não aceita", (body) => { body.money.currency = "EUR"; }, "money.currency"],
    ["kind OPENING", (body) => { body.kind = "OPENING"; }, "kind"],
    ["BET com valor zero", (body) => { body.money.amount = "0.00"; }, "money.amount"],
    ["BET com referência", (body) => { body.referenceExternalTransactionId = "bet-x"; }, "referenceExternalTransactionId"],
    ["campo desconhecido", (body) => { body.referenceExternalTransactionID = "typo"; }, "referenceExternalTransactionID"],
    ["providerId reservado", (body) => { body.providerId = "internal"; }, "providerId"],
    ["providerId com caractere fora do padrão", (body) => { body.providerId = "provider a"; }, "providerId"],
    ["walletId que não é UUID", (body) => { body.walletId = "123"; }, "walletId"],
  ];

  test.each(invalidCases)("%s", async (_name, mutate, expectedField) => {
    const { walletId, playerId } = await newWallet("10.00");
    const body = wagerBody({ walletId, playerId, kind: "BET", amount: "1.00" }) as Record<string, any>;
    mutate(body);
    const response = await httpRequest(app.baseUrl, "POST", "/wagering/transactions", body, { "Idempotency-Key": `k-${Bun.randomUUIDv7()}` });
    expect(response.status).toBe(400);
    expect(response.body.code).toBe("VALIDATION_ERROR");
    expect(response.body.errors.map((error: any) => error.field)).toContain(expectedField);
    expect(await countRows("wager_transactions", walletId)).toBe(1);                            // só o OPENING
  });

  test("REFUND sem referência", async () => {
    const { walletId, playerId } = await newWallet("10.00");
    const response = await submitWager(app.baseUrl, { walletId, playerId, kind: "REFUND", amount: "1.00" });
    expect(response.status).toBe(400);
  });

  test("UUID em maiúsculas é normalizado (e o replay continua batendo)", async () => {
    const { walletId, playerId } = await newWallet("10.00");
    const externalTransactionId = `bet-${Bun.randomUUIDv7()}`;
    const lower = await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "1.00", externalTransactionId }, "upper-case-key");
    const upper = await submitWager(app.baseUrl, { walletId: walletId.toUpperCase(), playerId: playerId.toUpperCase(), kind: "BET", amount: "1.00", externalTransactionId }, "upper-case-key");
    expect(lower.status).toBe(200);
    expect(upper.body).toEqual({ ...lower.body, idempotentReplay: true });
  });
});

describe("POST /wagering/transactions: regras de negócio e status", () => {
  test("saldo insuficiente: 422 com failureCode, transação REJECTED gravada, replay devolve a mesma rejeição", async () => {
    const { walletId, playerId } = await newWallet("10.00");
    const bet = { walletId, playerId, kind: "BET", amount: "10.01", externalTransactionId: `bet-${Bun.randomUUIDv7()}` };
    const rejected = await submitWager(app.baseUrl, bet);
    expect(rejected.status).toBe(422);
    expect(rejected.body).toMatchObject({ status: "REJECTED", failureCode: "INSUFFICIENT_FUNDS", balance: { amount: "10.00", currency: "BRL" }, idempotentReplay: false });
    const replay = await submitWager(app.baseUrl, bet);
    expect(replay.status).toBe(422);
    expect(replay.body).toEqual({ ...rejected.body, idempotentReplay: true });
    expect(await countRows("wallet_ledger_entries", walletId)).toBe(1);                         // só a abertura
  });

  test("moeda diferente da wallet: 422 CURRENCY_MISMATCH, com o saldo observado na moeda da wallet", async () => {
    const { walletId, playerId } = await newWallet("10.00");
    const response = await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "1.00", currency: "USD" });
    expect(response.status).toBe(422);
    expect(response.body).toMatchObject({ failureCode: "CURRENCY_MISMATCH", balance: { amount: "10.00", currency: "BRL" } });
  });

  test("wallet inexistente: 422 WALLET_NOT_FOUND em Problem Details, sem gravar", async () => {
    const response = await submitWager(app.baseUrl, { walletId: Bun.randomUUIDv7(), playerId: Bun.randomUUIDv7(), kind: "BET", amount: "1.00" });
    expect(response.status).toBe(422);
    expect(response.body).toMatchObject({ code: "WALLET_NOT_FOUND", status: 422 });
  });

  test("LOSS: 200 sem mover saldo", async () => {
    const { walletId, playerId } = await newWallet("10.00");
    const response = await submitWager(app.baseUrl, { walletId, playerId, kind: "LOSS", amount: "0.00" });
    expect(response.status).toBe(200);
    expect(response.body.balance).toEqual({ amount: "10.00", currency: "BRL" });
  });

  test("REFUND antes da BET: 202 PENDING_REFERENCE; a BET chega e o worker processa o REFUND", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    const betExternalId = `bet-${Bun.randomUUIDv7()}`;
    const refundInput = { walletId, playerId, kind: "REFUND", amount: "30.00", reference: betExternalId, externalTransactionId: `refund-${Bun.randomUUIDv7()}` };
    const refund = await submitWager(app.baseUrl, refundInput);
    expect(refund.status).toBe(202);
    expect(refund.body).toMatchObject({ status: "PENDING_REFERENCE", balance: { amount: "100.00", currency: "BRL" } });

    const pendingReplay = await submitWager(app.baseUrl, refundInput);
    expect(pendingReplay.status).toBe(202);
    expect(pendingReplay.body.idempotentReplay).toBe(true);

    const bet = await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "30.00", externalTransactionId: betExternalId });
    expect(bet.body.balance.amount).toBe("70.00");
    const processed = await waitUntil("worker processar o REFUND", async () => {
      const transaction = await httpRequest(app.baseUrl, "GET", `/wagering/transactions/${refund.body.transactionId}`);
      return transaction.body.status === "PROCESSED" ? transaction.body : undefined;
    });
    expect(processed).toMatchObject({ kind: "REFUND", referenceTransactionId: bet.body.transactionId, balance: { amount: "100.00", currency: "BRL" } });
    const finalReplay = await submitWager(app.baseUrl, refundInput);
    expect(finalReplay.status).toBe(200);                                                       // repetição depois que terminou: o resultado final
    const events = await testSql()`select event_type from outbox_messages where payload->'data'->>'transactionId' = ${refund.body.transactionId} order by event_type`;
    expect(events.map((event: any) => event.event_type)).toEqual(["WagerTransactionPendingReference", "WagerTransactionProcessed", "WalletBalanceChanged"]);   // o PendingReference sai uma vez só
  });

  test("referência que nunca chega: depois do TTL, REJECTED com REFERENCE_NOT_FOUND e evento", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    const rollback = await submitWager(app.baseUrl, { walletId, playerId, kind: "ROLLBACK", amount: "5.00", reference: `nunca-${Bun.randomUUIDv7()}` });
    expect(rollback.status).toBe(202);
    const rejected = await waitUntil("TTL vencer", async () => {
      const transaction = await httpRequest(app.baseUrl, "GET", `/wagering/transactions/${rollback.body.transactionId}`);
      return transaction.body.status === "REJECTED" ? transaction.body : undefined;
    }, 15_000);
    expect(rejected.failureCode).toBe("REFERENCE_NOT_FOUND");
    const [event] = await testSql()`select count(*)::int as total from outbox_messages where event_type = 'WagerTransactionRejected' and payload->'data'->>'transactionId' = ${rollback.body.transactionId}`;
    expect(event.total).toBe(1);
  });
});

describe("consultas", () => {
  test("transação por id e por provedor, e 404 NOT_FOUND", async () => {
    const { walletId, playerId } = await newWallet("10.00");
    const externalTransactionId = `bet-${Bun.randomUUIDv7()}`;
    const bet = await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "2.00", externalTransactionId });
    const byId = await httpRequest(app.baseUrl, "GET", `/wagering/transactions/${bet.body.transactionId}`);
    const byProvider = await httpRequest(app.baseUrl, "GET", `/providers/provider-a/wagering/transactions/${externalTransactionId}`);
    expect(byId.body).toEqual(byProvider.body);
    expect(byId.body).toMatchObject({ id: bet.body.transactionId, kind: "BET", status: "PROCESSED", money: { amount: "2.00", currency: "BRL" }, failureCode: null, referenceTransactionId: null, balance: { amount: "8.00", currency: "BRL" } });
    expect((await httpRequest(app.baseUrl, "GET", `/wagering/transactions/${Bun.randomUUIDv7()}`)).status).toBe(404);
    expect((await httpRequest(app.baseUrl, "GET", `/providers/provider-a/wagering/transactions/nao-existe`)).body.code).toBe("NOT_FOUND");
    expect((await httpRequest(app.baseUrl, "GET", `/wallets/${Bun.randomUUIDv7()}`)).status).toBe(404);
  });

  test("ledger com cursor estável e opaco, em ordem crescente de versão", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    for (let index = 0; index < 4; index++) await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "1.00" });
    const versions: number[] = [];
    let cursor: string | null = null;
    do {
      const page: any = await httpRequest(app.baseUrl, "GET", `/wallets/${walletId}/ledger?limit=2${cursor ? `&cursor=${cursor}` : ""}`);
      expect(page.status).toBe(200);
      versions.push(...page.body.entries.map((entry: any) => entry.walletVersion));
      cursor = page.body.nextCursor;
    } while (cursor);
    expect(versions).toEqual([1, 2, 3, 4, 5]);

    expect((await httpRequest(app.baseUrl, "GET", `/wallets/${walletId}/ledger?cursor=bad!cursor`)).body.code).toBe("INVALID_CURSOR");
    expect((await httpRequest(app.baseUrl, "GET", `/wallets/${walletId}/ledger?cursor=${Buffer.from("{\"x\":1}").toString("base64url")}`)).body.code).toBe("INVALID_CURSOR");
    expect((await httpRequest(app.baseUrl, "GET", `/wallets/${walletId}/ledger?limit=0`)).body.code).toBe("VALIDATION_ERROR");
    expect((await httpRequest(app.baseUrl, "GET", `/wallets/${walletId}/ledger?limit=101`)).status).toBe(400);
  });

  test("reconciliação consistente, e divergência sinalizada sem corrigir (log e métrica)", async () => {
    const { walletId, playerId } = await newWallet("50.00");
    await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "20.00" });
    const consistent = await httpRequest(app.baseUrl, "POST", `/wallets/${walletId}/reconciliation`);
    expect(consistent.status).toBe(200);
    expect(consistent.body).toEqual({ walletId, storedBalance: { amount: "30.00", currency: "BRL" }, calculatedBalance: { amount: "30.00", currency: "BRL" }, difference: { amount: "0.00", currency: "BRL" }, consistent: true, checkedEntries: 2 });

    const divergent = await newWallet("50.00");
    touchedWallets.pop();                                                                       // esta wallet é estragada de propósito
    await testSql()`update wallets set balance = 45.00 where id = ${divergent.walletId}`;        // simula uma divergência por fora da aplicação
    const metricsBefore = await httpRequest(app.baseUrl, "GET", "/metrics");
    const reconciliation = await httpRequest(app.baseUrl, "POST", `/wallets/${divergent.walletId}/reconciliation`);
    expect(reconciliation.status).toBe(200);
    expect(reconciliation.body).toMatchObject({ consistent: false, difference: { amount: "-5.00", currency: "BRL" }, checkedEntries: 1 });
    const [stillDivergent] = await testSql()`select balance from wallets where id = ${divergent.walletId}`;
    expect(stillDivergent.balance).toBe("45.00");                                               // nada foi corrigido em silêncio
    const metricsAfter = await httpRequest(app.baseUrl, "GET", "/metrics");
    const counter = (text: string) => Number(/wagering_reconciliation_divergences_total (\d+)/.exec(text)?.[1] ?? "0");
    expect(counter(metricsAfter.body)).toBe(counter(metricsBefore.body) + 1);
  });
});

describe("transversal", () => {
  test("X-Correlation-Id é aceito e devolvido; sem ele, um novo é gerado", async () => {
    const withHeader = await httpRequest(app.baseUrl, "GET", "/health/live", undefined, { "X-Correlation-Id": "corr-123" });
    expect(withHeader.headers.get("x-correlation-id")).toBe("corr-123");
    const withoutHeader = await httpRequest(app.baseUrl, "GET", "/health/live");
    expect(withoutHeader.headers.get("x-correlation-id")).toMatch(/^[0-9a-f-]{36}$/);
  });

  test("health e /metrics respondem sem autenticação", async () => {
    expect((await httpRequest(app.baseUrl, "GET", "/health/live")).status).toBe(200);
    const ready = await httpRequest(app.baseUrl, "GET", "/health/ready");
    expect(ready.status).toBe(200);
    expect(ready.body.checks).toEqual({ postgres: "ok", sqs: "ok" });
    const metrics = await httpRequest(app.baseUrl, "GET", "/metrics");
    for (const name of ["wagering_transactions_total", "wagering_duplicates_total", "wagering_retries_total", "wagering_dlq_messages", "wagering_wallet_lock_wait_seconds", "wagering_concurrency_conflicts_total", "wagering_outbox_lag_seconds", "wagering_processing_duration_seconds"]) {
      expect(metrics.body).toContain(name);
    }
  });
});
