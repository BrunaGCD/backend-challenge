import { describe, expect, test } from "bun:test";                                             // runner de testes
import { InvalidTransactionStateError, InvalidWagerTransactionError } from "../../src/domain/errors";
import { FailureCode } from "../../src/domain/failure-code";                                    // motivos
import { Money } from "../../src/domain/money";                                                 // valores
import { LedgerDirection } from "../../src/domain/wallet-ledger-entry";                         // direção
import { WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from "../../src/domain/wager-transaction";
import { brl, FIXED_RETRY_POLICY, NOW, newTransaction, WALLET_ID, PLAYER_ID } from "../support/domain-fixtures";

const later = new Date(NOW.getTime() + 1000);

describe("WagerTransaction.create", () => {
  test("nasce em PENDING, sem nada decidido", () => {
    const transaction = newTransaction();
    expect(transaction.status).toBe(WagerTransactionStatus.Pending);
    expect(transaction.isTerminal()).toBe(false);
    expect(transaction.failureCode).toBeUndefined();
    expect(transaction.attempts).toBe(0);
  });

  test("REFUND e ROLLBACK exigem referência; BET proíbe; WIN e LOSS aceitam com ou sem", () => {
    expect(() => newTransaction({ kind: WagerTransactionKind.Refund })).toThrow(InvalidWagerTransactionError);
    expect(() => newTransaction({ kind: WagerTransactionKind.Rollback })).toThrow(InvalidWagerTransactionError);
    expect(() => newTransaction({ kind: WagerTransactionKind.Bet, reference: "bet-1" })).toThrow(InvalidWagerTransactionError);
    expect(newTransaction({ kind: WagerTransactionKind.Win }).requiresReference()).toBe(false);
    expect(newTransaction({ kind: WagerTransactionKind.Win, reference: "bet-1" }).requiresReference()).toBe(false);
    expect(newTransaction({ kind: WagerTransactionKind.Loss, reference: "bet-1" }).requiresReference()).toBe(false);
    expect(newTransaction({ kind: WagerTransactionKind.Refund, reference: "bet-1" }).requiresReference()).toBe(true);
  });

  test("valor zero só é aceito em LOSS (interpretação I6)", () => {
    expect(() => newTransaction({ kind: WagerTransactionKind.Bet, amount: "0.00" })).toThrow(InvalidWagerTransactionError);
    expect(() => newTransaction({ kind: WagerTransactionKind.Win, amount: "0.00" })).toThrow(InvalidWagerTransactionError);
    expect(newTransaction({ kind: WagerTransactionKind.Loss, amount: "0.00" }).affectsBalance()).toBe(false);
  });

  test("OPENING não pode vir de provedor", () => {
    expect(() => newTransaction({ kind: WagerTransactionKind.Opening })).toThrow(InvalidWagerTransactionError);
  });

  test("createOpening: sem provider, external id, rodada e jogo, com key opening:{walletId}", () => {
    const opening = WagerTransaction.createOpening({ id: "tx-open", walletId: WALLET_ID, playerId: PLAYER_ID, money: brl("1000.00"), payloadHash: "b".repeat(64), correlationId: "corr", createdAt: NOW });
    expect(opening.kind).toBe(WagerTransactionKind.Opening);
    expect(opening.providerId).toBeUndefined();
    expect(opening.externalTransactionId).toBeUndefined();
    expect(opening.roundId).toBeUndefined();
    expect(opening.gameId).toBeUndefined();
    expect(opening.idempotencyKey).toBe(`opening:${WALLET_ID}`);
    expect(opening.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
  });
});

describe("WagerTransaction: transições", () => {
  test("PENDING -> PROCESSED grava saldo observado e processedAt", () => {
    const transaction = newTransaction();
    transaction.markProcessed(undefined, brl("90.00"), later);
    expect(transaction.status).toBe(WagerTransactionStatus.Processed);
    expect(transaction.balanceAfter?.toString()).toBe("90.00");
    expect(transaction.processedAt).toEqual(later);
  });

  test("PENDING -> REJECTED grava o código", () => {
    const transaction = newTransaction();
    transaction.reject(FailureCode.InsufficientFunds, brl("20.00"), later);
    expect(transaction.status).toBe(WagerTransactionStatus.Rejected);
    expect(transaction.failureCode).toBe(FailureCode.InsufficientFunds);
    expect(transaction.processedAt).toBeUndefined();
  });

  test("PENDING -> PENDING_REFERENCE agenda a primeira tentativa com o backoff", () => {
    const transaction = newTransaction({ kind: WagerTransactionKind.Refund, reference: "bet-1" });
    transaction.markPendingReference(brl("50.00"), NOW, FIXED_RETRY_POLICY);
    expect(transaction.status).toBe(WagerTransactionStatus.PendingReference);
    expect(transaction.attempts).toBe(0);
    expect(transaction.nextAttemptAt).toEqual(new Date(NOW.getTime() + 500));                  // random 0.5 × min(60 s, 1 s × 2^0)
  });

  test("PENDING_REFERENCE -> PROCESSED, REJECTED e FAILED", () => {
    for (const decide of [
      (transaction: WagerTransaction) => transaction.markProcessed("bet-id", brl("1.00"), later),
      (transaction: WagerTransaction) => transaction.reject(FailureCode.ReferenceNotFound, brl("1.00"), later),
      (transaction: WagerTransaction) => transaction.fail(FailureCode.ProcessingFailed, later),
    ]) {
      const transaction = newTransaction({ kind: WagerTransactionKind.Refund, reference: "bet-1" });
      transaction.markPendingReference(brl("1.00"), NOW, FIXED_RETRY_POLICY);
      decide(transaction);
      expect(transaction.isTerminal()).toBe(true);
    }
  });

  test("scheduleReferenceRetry não é transição: só conta tentativa e reagenda", () => {
    const transaction = newTransaction({ kind: WagerTransactionKind.Refund, reference: "bet-1" });
    transaction.markPendingReference(brl("1.00"), NOW, FIXED_RETRY_POLICY);
    transaction.scheduleReferenceRetry(later, FIXED_RETRY_POLICY);
    expect(transaction.status).toBe(WagerTransactionStatus.PendingReference);
    expect(transaction.attempts).toBe(1);
    expect(transaction.nextAttemptAt).toEqual(new Date(later.getTime() + 1000));               // 0.5 × 1 s × 2^1
  });

  test("qualquer transição a partir de estado terminal lança InvalidTransactionStateError", () => {
    const processed = newTransaction();
    processed.markProcessed(undefined, brl("1.00"), later);
    expect(() => processed.markProcessed(undefined, brl("1.00"), later)).toThrow(InvalidTransactionStateError);
    expect(() => processed.reject(FailureCode.InsufficientFunds, brl("1.00"), later)).toThrow(InvalidTransactionStateError);
    expect(() => processed.markPendingReference(brl("1.00"), later, FIXED_RETRY_POLICY)).toThrow(InvalidTransactionStateError);
    expect(() => processed.fail(FailureCode.ProcessingFailed, later)).toThrow(InvalidTransactionStateError);
    expect(() => processed.scheduleReferenceRetry(later, FIXED_RETRY_POLICY)).toThrow(InvalidTransactionStateError);

    const rejected = newTransaction();
    rejected.reject(FailureCode.CurrencyMismatch, brl("1.00"), later);
    expect(() => rejected.markProcessed(undefined, brl("1.00"), later)).toThrow(InvalidTransactionStateError);
  });

  test("PENDING -> FAILED não existe", () => {
    expect(() => newTransaction().fail(FailureCode.ProcessingFailed, later)).toThrow(InvalidTransactionStateError);
  });

  test("rehydrate reconstrói estado terminal sem revalidar", () => {
    const processed = newTransaction();
    processed.markProcessed(undefined, brl("1.00"), later);
    const rebuilt = WagerTransaction.rehydrate(processed.toState());
    expect(rebuilt.status).toBe(WagerTransactionStatus.Processed);
    expect(rebuilt.money.equals(processed.money)).toBe(true);
  });
});

describe("WagerTransaction: consultas", () => {
  test("matchesPayload compara o hash", () => {
    const transaction = newTransaction();
    expect(transaction.matchesPayload("a".repeat(64))).toBe(true);
    expect(transaction.matchesPayload("b".repeat(64))).toBe(false);
  });

  test("ledgerDirectionFor: BET debita, WIN e REFUND creditam, ROLLBACK inverte a referência", () => {
    const bet = newTransaction({ kind: WagerTransactionKind.Bet });
    const win = newTransaction({ kind: WagerTransactionKind.Win });
    const refund = newTransaction({ kind: WagerTransactionKind.Refund, reference: "x" });
    const rollback = newTransaction({ kind: WagerTransactionKind.Rollback, reference: "x" });
    expect(bet.ledgerDirectionFor()).toBe(LedgerDirection.Debit);
    expect(win.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    expect(refund.ledgerDirectionFor()).toBe(LedgerDirection.Credit);
    expect(rollback.ledgerDirectionFor(bet)).toBe(LedgerDirection.Credit);
    expect(rollback.ledgerDirectionFor(win)).toBe(LedgerDirection.Debit);
    expect(rollback.ledgerDirectionFor(refund)).toBe(LedgerDirection.Debit);
  });

  test("TTL da espera contado a partir do createdAt", () => {
    const transaction = newTransaction({ kind: WagerTransactionKind.Refund, reference: "x" });
    expect(transaction.hasReferenceWaitExpired(new Date(NOW.getTime() + 9_999), 10_000)).toBe(false);
    expect(transaction.hasReferenceWaitExpired(new Date(NOW.getTime() + 10_000), 10_000)).toBe(true);
  });

  test("money da transação é Money, nunca number", () => {
    expect(newTransaction().money).toBeInstanceOf(Money);
  });
});
