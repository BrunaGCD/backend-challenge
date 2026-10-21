// ====================================================================================================
//   BACKOFF COM ALEATÓRIO INJETADO, CLASSIFICAÇÃO DE ERROS, CURSOR DO LEDGER, OUTBOX E INBOX
// ====================================================================================================
import { describe, expect, test } from "bun:test";
import { concurrencyConflictType, isTransientInfrastructureError, uniqueViolationConstraint } from "../../src/application/infrastructure-errors";
import { ConcurrentUpdateError, InvalidCursorError } from "../../src/application/errors";
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
