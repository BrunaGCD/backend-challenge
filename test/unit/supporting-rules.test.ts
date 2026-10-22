// ====================================================================================================
//   BACKOFF COM ALEATÓRIO INJETADO, CLASSIFICAÇÃO DE ERROS, CURSOR DO LEDGER, OUTBOX E INBOX
// ====================================================================================================
import { describe, expect, test } from "bun:test";
import { concurrencyConflictType, isDatabaseUnavailableError, isTransientInfrastructureError, uniqueViolationConstraint } from "../../src/application/infrastructure-errors";
import { ConcurrentUpdateError, InvalidCursorError } from "../../src/application/errors";
import { loadConfig } from "../../src/config";                                                  // ordem dos timeouts
import { decodeLedgerCursor, encodeLedgerCursor } from "../../src/application/queries";
import { fullJitterDelayMilliseconds } from "../../src/domain/backoff";
import { WagerTransactionProcessed, WalletBalanceChanged } from "../../src/domain/events";
import { InboxMessage } from "../../src/domain/inbox-message";
import { OutboxMessage } from "../../src/domain/outbox-message";
import { LedgerDirection } from "../../src/domain/wallet-ledger-entry";
import { NOW, newTransaction, openWallet } from "../support/domain-fixtures";

describe("backoff exponencial com full jitter", () => {
  const policy = (value: number) => ({ baseMilliseconds: 1000, capMilliseconds: 60_000, random: () => value });

  test("espera = aleatório × min(teto, base × 2^tentativa)", () => {
    expect(fullJitterDelayMilliseconds(0, policy(0.5))).toBe(500);
    expect(fullJitterDelayMilliseconds(3, policy(0.5))).toBe(4000);                             // 0.5 × 8 s
    expect(fullJitterDelayMilliseconds(10, policy(0.5))).toBe(30_000);                          // 0.5 × teto de 60 s
  });

  test("os extremos do aleatório dão 0 e quase o teto", () => {
    expect(fullJitterDelayMilliseconds(5, policy(0))).toBe(0);
    expect(fullJitterDelayMilliseconds(30, policy(0.999999))).toBe(59_999);
  });
});

describe("classificação de erros de infraestrutura", () => {
  const withCode = (code: string, extra: Record<string, unknown> = {}) => Object.assign(new Error("x"), { code, ...extra });

  test.each(["40001", "40P01", "55P03", "57014", "08006", "08001", "57P01", "53300", "ECONNREFUSED", "ECONNRESET"])("%s é transitório", (code) => {
    expect(isTransientInfrastructureError(withCode(code))).toBe(true);
  });

  test("mensagens de conexão do pg sem code também são transitórias", () => {
    expect(isTransientInfrastructureError(new Error("Connection terminated unexpectedly"))).toBe(true);
    expect(isTransientInfrastructureError(new Error("timeout exceeded when trying to connect"))).toBe(true);
  });

  test.each(["23505", "23514", "22003", "P0001"])("%s não é transitório", (code) => {
    expect(isTransientInfrastructureError(withCode(code))).toBe(false);
  });

  test("erro comum não é transitório", () => {
    expect(isTransientInfrastructureError(new TypeError("bug"))).toBe(false);
  });

  // banco fora ou saturado pausa o consumidor; lock, statement_timeout e deadlock são da operação
  test.each(["08006", "08001", "57P01", "57P03", "53300", "ECONNREFUSED", "ENOTFOUND"])("%s é banco indisponível", (code) => {
    expect(isDatabaseUnavailableError(withCode(code))).toBe(true);
  });

  test.each(["55P03", "57014", "40P01", "40001", "23505"])("%s não é banco indisponível", (code) => {
    expect(isDatabaseUnavailableError(withCode(code))).toBe(false);
  });

  test("pool sem conexão livre e conexão caída são banco indisponível", () => {
    expect(isDatabaseUnavailableError(new Error("timeout exceeded when trying to connect"))).toBe(true);
    expect(isDatabaseUnavailableError(new Error("Connection terminated unexpectedly"))).toBe(true);
    expect(isDatabaseUnavailableError(new TypeError("bug"))).toBe(false);
  });

  test("tipo de conflito de concorrência e constraint da violação de unicidade", () => {
    expect(concurrencyConflictType(withCode("55P03"))).toBe("lock_timeout");
    expect(concurrencyConflictType(withCode("40P01"))).toBe("deadlock");
    expect(concurrencyConflictType(withCode("23505"))).toBe("unique_violation");
    expect(concurrencyConflictType(new ConcurrentUpdateError("version mudou"))).toBe("optimistic_lock");                  // o conflito que o flush acusa
    expect(isTransientInfrastructureError(new ConcurrentUpdateError("version mudou"))).toBe(false);                    // é bug, não vale repetir como transitório
    expect(uniqueViolationConstraint(withCode("23505", { constraint: "wager_transactions_idempotency_key_unique" }))).toBe("wager_transactions_idempotency_key_unique");
    expect(uniqueViolationConstraint(withCode("23514", { constraint: "x" }))).toBeUndefined();
  });
});

describe("cursor do ledger", () => {
  test("ida e volta, e o formato é o JSON {after} em base64url", () => {
    const cursor = encodeLedgerCursor(42);
    expect(Buffer.from(cursor, "base64url").toString("utf8")).toBe('{"after":42}');
    expect(decodeLedgerCursor(cursor)).toBe(42);
  });

  test.each(["", "!!!", Buffer.from("{}").toString("base64url"), Buffer.from('{"after":-1}').toString("base64url"), Buffer.from('{"after":1.5}').toString("base64url"), Buffer.from('{"after":1,"x":2}').toString("base64url"), Buffer.from("not json").toString("base64url")])("rejeita %p", (cursor) => {
    expect(() => decodeLedgerCursor(cursor)).toThrow(InvalidCursorError);
  });
});

describe("eventos, outbox e inbox", () => {
  test("envelope serializado com eventType e version do tipo, e data com MoneyProps", () => {
    const { wallet } = openWallet("100.00");
    const transaction = newTransaction({ amount: "25.00" });
    const entry = wallet.debit(transaction.money, { transactionId: transaction.id, ledgerEntryId: "entry-1", at: NOW });
    transaction.markProcessed(undefined, wallet.balance, NOW);
    const context = { eventId: "event-1", correlationId: "corr-1", causationId: "cause-1", occurredAt: NOW };

    const balanceChanged = WalletBalanceChanged.from(wallet, entry, context).toJSON();
    expect(balanceChanged).toEqual({
      eventId: "event-1", eventType: "WalletBalanceChanged", aggregateId: wallet.id, correlationId: "corr-1", causationId: "cause-1", occurredAt: NOW.toISOString(), version: 1,
      data: { walletId: wallet.id, transactionId: transaction.id, direction: LedgerDirection.Debit, money: { amount: "25.00", currency: "BRL" }, balanceBefore: { amount: "100.00", currency: "BRL" }, balanceAfter: { amount: "75.00", currency: "BRL" }, walletVersion: 2 },
    });
    const processed = WagerTransactionProcessed.from(transaction, { ...context, eventId: "event-2" }).toJSON();
    expect(processed.eventType).toBe("WagerTransactionProcessed");
    expect(processed.aggregateId).toBe(transaction.walletId);                                   // aggregateId = walletId em todos os eventos
    expect(JSON.parse(JSON.stringify(processed)).data.money).toEqual({ amount: "25.00", currency: "BRL" });
  });

  test("OutboxMessage: id é o eventId, nasce pendente e vencida, scheduleRetry conta e reagenda", () => {
    const { wallet, openingEntry } = openWallet("10.00");
    const event = WalletBalanceChanged.from(wallet, openingEntry!, { eventId: "event-9", correlationId: "c", occurredAt: NOW });
    const message = OutboxMessage.enqueue(event);
    expect(message.id).toBe("event-9");
    expect(message.isPending()).toBe(true);
    expect(message.isDue(NOW)).toBe(true);

    message.scheduleRetry(NOW, { baseMilliseconds: 1000, capMilliseconds: 300_000, random: () => 0.5 }, "SQS fora");
    expect(message.attempts).toBe(1);
    expect(message.nextAttemptAt).toEqual(new Date(NOW.getTime() + 1000));                      // 0.5 × 1 s × 2^1
    expect(message.isDue(NOW)).toBe(false);
    expect(message.lastError).toBe("SQS fora");

    message.markPublished(NOW);
    expect(message.isPending()).toBe(false);
  });

  test("InboxMessage: mesma mensagem é o mesmo hash", () => {
    const inbox = InboxMessage.receive({ messageId: "m-1", consumerName: "c", payloadHash: "a".repeat(64), receivedAt: NOW });
    expect(inbox.isProcessed()).toBe(false);
    inbox.markProcessed(NOW);
    expect(inbox.isProcessed()).toBe(true);
    expect(inbox.isSameMessage("a".repeat(64))).toBe(true);
    expect(inbox.isSameMessage("b".repeat(64))).toBe(false);
  });
});

describe("ordem dos timeouts na configuração", () => {
  const TIMEOUT_VARIABLES = ["DATABASE_LOCK_TIMEOUT", "DATABASE_STATEMENT_TIMEOUT", "DATABASE_BACKGROUND_LOCK_TIMEOUT", "DATABASE_BACKGROUND_STATEMENT_TIMEOUT", "SQS_VISIBILITY_TIMEOUT_SECONDS", "SQS_MAX_RECEIVE_COUNT", "CONSUMER_RETRY_BUDGET_MS"];

  function loadWith(environment: Record<string, string>) {
    const previous = Object.fromEntries(TIMEOUT_VARIABLES.map((name) => [name, process.env[name]]));
    for (const name of TIMEOUT_VARIABLES) delete process.env[name];                             // parte dos padrões do código
    Object.assign(process.env, environment);
    try {
      return loadConfig();
    } finally {
      for (const name of TIMEOUT_VARIABLES) {
        if (previous[name] === undefined) delete process.env[name];
        else process.env[name] = previous[name];
      }
    }
  }

  test("os padrões: HTTP espera o lock 2 s, a fila e o worker 10 s, abaixo dos 30 s de visibilidade", () => {
    const config = loadWith({});
    expect(config.database).toMatchObject({ lockTimeout: "2s", statementTimeout: "5s", backgroundLockTimeout: "10s", backgroundStatementTimeout: "15s" });
    expect(config.sqs.visibilityTimeoutSeconds).toBe(30);
  });

  test("os padrões da fila: 5 minutos de novas tentativas no consumidor, e maxReceiveCount 10 só como rede de segurança", () => {
    const config = loadWith({});
    expect(config.consumer.retryBudgetMilliseconds).toBe(300_000);
    expect(config.sqs.maxReceiveCount).toBe(10);
  });

  test("a transação da fila que não cabe na visibilidade é recusada na subida", () => {
    expect(() => loadWith({ SQS_VISIBILITY_TIMEOUT_SECONDS: "12" })).toThrow("DATABASE_BACKGROUND_STATEMENT_TIMEOUT precisa ser menor que SQS_VISIBILITY_TIMEOUT_SECONDS");
  });

  test("statement_timeout que cortaria a espera do lock é recusado", () => {
    expect(() => loadWith({ DATABASE_BACKGROUND_LOCK_TIMEOUT: "20s" })).toThrow("DATABASE_BACKGROUND_LOCK_TIMEOUT precisa ser menor que DATABASE_BACKGROUND_STATEMENT_TIMEOUT");
    expect(() => loadWith({ DATABASE_LOCK_TIMEOUT: "5000ms" })).toThrow("DATABASE_LOCK_TIMEOUT precisa ser menor que DATABASE_STATEMENT_TIMEOUT");
    expect(() => loadWith({ DATABASE_LOCK_TIMEOUT: "dois segundos" })).toThrow("precisa ser um intervalo");
  });
});
