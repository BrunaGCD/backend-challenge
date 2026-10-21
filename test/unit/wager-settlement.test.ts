import { describe, expect, test } from "bun:test";                                             // runner de testes
import { FailureCode } from "../../src/domain/failure-code";                                    // motivos
import { settleWagerTransaction } from "../../src/domain/wager-settlement";                     // regras da seção 7
import type { Wallet } from "../../src/domain/wallet";                                          // wallet travada
import { LedgerDirection, type WalletLedgerEntry } from "../../src/domain/wallet-ledger-entry";                // lançamentos
import { WagerTransactionKind as Kind, WagerTransactionStatus as Status, type WagerTransaction } from "../../src/domain/wager-transaction";
import { FIXED_RETRY_POLICY, NOW, OTHER_PLAYER_ID, brl, newTransaction, nextId, openWallet } from "../support/domain-fixtures";
import { ledgerChainProblems } from "../support/ledger-chain";                                  // invariante final

const TTL = 10_000;

// guarda os lançamentos de cada teste para conferir a cadeia no fim
function scenario(initialBalance: string) {
  const { wallet, openingEntry } = openWallet(initialBalance);
  const entries: WalletLedgerEntry[] = openingEntry ? [openingEntry] : [];
  function settle(transaction: WagerTransaction, options: { reference?: WagerTransaction; alreadyReversed?: boolean; now?: Date } = {}) {
    const outcome = settleWagerTransaction({
      transaction, wallet, reference: options.reference, referenceAlreadyReversed: options.alreadyReversed ?? false,
      ledgerEntryId: nextId("entry"), now: options.now ?? NOW, pendingRetryPolicy: FIXED_RETRY_POLICY, referenceTtlMilliseconds: TTL,
    });
    if (outcome.outcome === "processed" && outcome.ledgerEntry) entries.push(outcome.ledgerEntry);
    return outcome;
  }
  function assertLedgerConsistent(currentWallet: Wallet = wallet) {
    expect(ledgerChainProblems(currentWallet.balance, entries)).toEqual([]);
  }
  return { wallet, entries, settle, assertLedgerConsistent };
}

function processedBet(world: ReturnType<typeof scenario>, amount = "25.00", externalTransactionId?: string) {
  const bet = newTransaction({ kind: Kind.Bet, amount, externalTransactionId });
  world.settle(bet);
  expect(bet.status).toBe(Status.Processed);
  return bet;
}

describe("BET", () => {
  test("debita, gera um DEBIT e grava o saldo observado", () => {
    const world = scenario("100.00");
    const bet = newTransaction({ kind: Kind.Bet, amount: "25.00" });
    const outcome = world.settle(bet);
    expect(outcome.outcome).toBe("processed");
    expect(world.wallet.balance.toString()).toBe("75.00");
    expect(bet.balanceAfter?.toString()).toBe("75.00");
    expect(world.entries.at(-1)?.direction).toBe(LedgerDirection.Debit);
    world.assertLedgerConsistent();
  });

  test("saldo insuficiente: REJECTED com INSUFFICIENT_FUNDS, sem lançamento e sem mudar o saldo", () => {
    const world = scenario("100.00");
    processedBet(world, "80.00");
    const second = newTransaction({ kind: Kind.Bet, amount: "80.00" });
    const outcome = world.settle(second);
    expect(outcome).toEqual({ outcome: "rejected", failureCode: FailureCode.InsufficientFunds });
    expect(world.wallet.balance.toString()).toBe("20.00");
    expect(second.balanceAfter?.toString()).toBe("20.00");
    expect(world.entries).toHaveLength(2);                                                      // abertura e o primeiro débito
    world.assertLedgerConsistent();
  });

  test("moeda diferente da wallet: CURRENCY_MISMATCH", () => {
    const world = scenario("100.00");
    const outcome = world.settle(newTransaction({ kind: Kind.Bet, currency: "USD" }));
    expect(outcome).toEqual({ outcome: "rejected", failureCode: FailureCode.CurrencyMismatch });
    world.assertLedgerConsistent();
  });

  test("playerId que não é o dono da wallet: PLAYER_WALLET_MISMATCH", () => {
    const world = scenario("100.00");
    const outcome = world.settle(newTransaction({ kind: Kind.Bet, playerId: OTHER_PLAYER_ID }));
    expect(outcome).toEqual({ outcome: "rejected", failureCode: FailureCode.PlayerWalletMismatch });
  });
});

describe("WIN", () => {
  test("sem referência: credita", () => {
    const world = scenario("10.00");
    world.settle(newTransaction({ kind: Kind.Win, amount: "40.00" }));
    expect(world.wallet.balance.toString()).toBe("50.00");
    world.assertLedgerConsistent();
  });

  test("com referência a uma BET da mesma rodada: credita qualquer valor, e vários WIN podem apontar para a mesma BET", () => {
    const world = scenario("100.00");
    const bet = processedBet(world, "10.00");
    const firstWin = newTransaction({ kind: Kind.Win, amount: "30.00", reference: bet.externalTransactionId });
    const secondWin = newTransaction({ kind: Kind.Win, amount: "5.00", reference: bet.externalTransactionId });
    world.settle(firstWin, { reference: bet });
    world.settle(secondWin, { reference: bet });
    expect(firstWin.referenceTransactionId).toBe(bet.id);
    expect(world.wallet.balance.toString()).toBe("125.00");
    world.assertLedgerConsistent();
  });

  test("referência que não é BET: REFERENCE_KIND_NOT_ALLOWED", () => {
    const world = scenario("100.00");
    const otherWin = newTransaction({ kind: Kind.Win, amount: "1.00" });
    world.settle(otherWin);
    const outcome = world.settle(newTransaction({ kind: Kind.Win, reference: otherWin.externalTransactionId }), { reference: otherWin });
    expect(outcome).toEqual({ outcome: "rejected", failureCode: FailureCode.ReferenceKindNotAllowed });
  });

  test("referência de outra rodada: REFERENCE_MISMATCH", () => {
    const world = scenario("100.00");
    const bet = processedBet(world);
    const outcome = world.settle(newTransaction({ kind: Kind.Win, roundId: "round-2", reference: bet.externalTransactionId }), { reference: bet });
    expect(outcome).toEqual({ outcome: "rejected", failureCode: FailureCode.ReferenceMismatch });
  });

  test("referência ainda inexistente: entra em PENDING_REFERENCE", () => {
    const world = scenario("100.00");
    const win = newTransaction({ kind: Kind.Win, reference: "bet-que-nao-chegou" });
    expect(world.settle(win).outcome).toBe("entered_pending_reference");
    expect(win.status).toBe(Status.PendingReference);
    expect(world.wallet.balance.toString()).toBe("100.00");
  });
});

describe("LOSS", () => {
  test("processa sem mover saldo e sem lançamento, inclusive com valor zero", () => {
    const world = scenario("100.00");
    const loss = newTransaction({ kind: Kind.Loss, amount: "0.00" });
    const outcome = world.settle(loss);
    expect(outcome).toEqual({ outcome: "processed", ledgerEntry: undefined });
    expect(loss.status).toBe(Status.Processed);
    expect(loss.balanceAfter?.toString()).toBe("100.00");
    expect(world.wallet.version).toBe(1);                                                       // saldo não mudou, versão também não
    world.assertLedgerConsistent();
  });
});

describe("REFUND", () => {
  test("reverte uma BET processada creditando o mesmo valor", () => {
    const world = scenario("100.00");
    const bet = processedBet(world, "25.00");
    const refund = newTransaction({ kind: Kind.Refund, amount: "25.00", reference: bet.externalTransactionId });
    world.settle(refund, { reference: bet });
    expect(refund.status).toBe(Status.Processed);
    expect(refund.referenceTransactionId).toBe(bet.id);
    expect(world.wallet.balance.toString()).toBe("100.00");
    world.assertLedgerConsistent();
  });

  test("só referencia BET", () => {
    const world = scenario("100.00");
    const win = newTransaction({ kind: Kind.Win, amount: "5.00" });
    world.settle(win);
    const outcome = world.settle(newTransaction({ kind: Kind.Refund, amount: "5.00", reference: win.externalTransactionId }), { reference: win });
    expect(outcome).toEqual({ outcome: "rejected", failureCode: FailureCode.ReferenceKindNotAllowed });
  });

  test("valor diferente da BET: REFERENCE_AMOUNT_MISMATCH (reversão parcial fora de escopo)", () => {
    const world = scenario("100.00");
    const bet = processedBet(world, "25.00");
    const outcome = world.settle(newTransaction({ kind: Kind.Refund, amount: "10.00", reference: bet.externalTransactionId }), { reference: bet });
    expect(outcome).toEqual({ outcome: "rejected", failureCode: FailureCode.ReferenceAmountMismatch });
    world.assertLedgerConsistent();
  });

  test("BET já revertida: REFERENCE_ALREADY_REVERSED, sem crédito duplicado", () => {
    const world = scenario("100.00");
    const bet = processedBet(world, "25.00");
    const outcome = world.settle(newTransaction({ kind: Kind.Refund, amount: "25.00", reference: bet.externalTransactionId }), { reference: bet, alreadyReversed: true });
    expect(outcome).toEqual({ outcome: "rejected", failureCode: FailureCode.ReferenceAlreadyReversed });
    expect(world.wallet.balance.toString()).toBe("75.00");
  });

  test("BET rejeitada: REFERENCE_NOT_PROCESSED", () => {
    const world = scenario("10.00");
    const bet = newTransaction({ kind: Kind.Bet, amount: "25.00" });
    world.settle(bet);
    expect(bet.status).toBe(Status.Rejected);
    const outcome = world.settle(newTransaction({ kind: Kind.Refund, amount: "25.00", reference: bet.externalTransactionId }), { reference: bet });
    expect(outcome).toEqual({ outcome: "rejected", failureCode: FailureCode.ReferenceNotProcessed });
  });

  test("referência de outro player: REFERENCE_MISMATCH", () => {
    const world = scenario("100.00");
    const otherPlayersBet = newTransaction({ kind: Kind.Bet, amount: "25.00", playerId: OTHER_PLAYER_ID });
    const outcome = world.settle(newTransaction({ kind: Kind.Refund, amount: "25.00", reference: otherPlayersBet.externalTransactionId }), { reference: otherPlayersBet });
    expect(outcome).toEqual({ outcome: "rejected", failureCode: FailureCode.ReferenceMismatch });
  });
});

describe("ROLLBACK", () => {
  test("de BET credita, de WIN debita, de REFUND debita", () => {
    const world = scenario("100.00");
    const bet = processedBet(world, "30.00");                                                   // 70
    const win = newTransaction({ kind: Kind.Win, amount: "50.00" });
    world.settle(win);                                                                          // 120
    const refundBet = processedBet(world, "10.00");                                             // 110
    const refund = newTransaction({ kind: Kind.Refund, amount: "10.00", reference: refundBet.externalTransactionId });
    world.settle(refund, { reference: refundBet });                                             // 120

    world.settle(newTransaction({ kind: Kind.Rollback, amount: "30.00", reference: bet.externalTransactionId }), { reference: bet });         // 150
    world.settle(newTransaction({ kind: Kind.Rollback, amount: "50.00", reference: win.externalTransactionId }), { reference: win });         // 100
    world.settle(newTransaction({ kind: Kind.Rollback, amount: "10.00", reference: refund.externalTransactionId }), { reference: refund });   // 90
    expect(world.wallet.balance.toString()).toBe("90.00");
    world.assertLedgerConsistent();
  });

  test("de WIN já gasto: REVERSAL_INSUFFICIENT_FUNDS, diferente do INSUFFICIENT_FUNDS da aposta", () => {
    const world = scenario("0.00");
    const win = newTransaction({ kind: Kind.Win, amount: "50.00" });
    world.settle(win);
    processedBet(world, "45.00");                                                               // gastou o prêmio
    const rollback = newTransaction({ kind: Kind.Rollback, amount: "50.00", reference: win.externalTransactionId });
    const outcome = world.settle(rollback, { reference: win });
    expect(outcome).toEqual({ outcome: "rejected", failureCode: FailureCode.ReversalInsufficientFunds });
    expect(world.wallet.balance.toString()).toBe("5.00");
    world.assertLedgerConsistent();
  });

  test("de LOSS, de ROLLBACK e de OPENING não é permitido", () => {
    const world = scenario("100.00");
    const loss = newTransaction({ kind: Kind.Loss, amount: "1.00" });
    world.settle(loss);
    const outcome = world.settle(newTransaction({ kind: Kind.Rollback, amount: "1.00", reference: loss.externalTransactionId }), { reference: loss });
    expect(outcome).toEqual({ outcome: "rejected", failureCode: FailureCode.ReferenceKindNotAllowed });
  });

  test("referência pendente: o ROLLBACK também espera (I2)", () => {
    const world = scenario("100.00");
    const pendingWin = newTransaction({ kind: Kind.Win, amount: "5.00", reference: "bet-ausente" });
    world.settle(pendingWin);
    const rollback = newTransaction({ kind: Kind.Rollback, amount: "5.00", reference: pendingWin.externalTransactionId });
    expect(world.settle(rollback, { reference: pendingWin }).outcome).toBe("entered_pending_reference");
  });
});

describe("Interpretação I1: uma reversão por referência, somando REFUND e ROLLBACK", () => {
  test("ROLLBACK de uma BET que já teve REFUND é REFERENCE_ALREADY_REVERSED", () => {
    const world = scenario("100.00");
    const bet = processedBet(world, "25.00");
    world.settle(newTransaction({ kind: Kind.Refund, amount: "25.00", reference: bet.externalTransactionId }), { reference: bet });
    const outcome = world.settle(newTransaction({ kind: Kind.Rollback, amount: "25.00", reference: bet.externalTransactionId }), { reference: bet, alreadyReversed: true });
    expect(outcome).toEqual({ outcome: "rejected", failureCode: FailureCode.ReferenceAlreadyReversed });
    expect(world.wallet.balance.toString()).toBe("100.00");                                     // só um crédito de volta
    world.assertLedgerConsistent();
  });
});

describe("Worker: nova tentativa de uma pendente", () => {
  test("continua sem referência dentro do TTL: só reagenda", () => {
    const world = scenario("100.00");
    const refund = newTransaction({ kind: Kind.Refund, amount: "25.00", reference: "bet-ausente" });
    world.settle(refund);
    const outcome = world.settle(refund, { now: new Date(NOW.getTime() + 2000) });
    expect(outcome.outcome).toBe("still_pending_reference");
    expect(refund.attempts).toBe(1);
  });

  test("passou do TTL sem referência: REFERENCE_NOT_FOUND", () => {
    const world = scenario("100.00");
    const refund = newTransaction({ kind: Kind.Refund, amount: "25.00", reference: "bet-ausente" });
    world.settle(refund);
    const outcome = world.settle(refund, { now: new Date(NOW.getTime() + TTL) });
    expect(outcome).toEqual({ outcome: "rejected", failureCode: FailureCode.ReferenceNotFound });
    expect(world.wallet.balance.toString()).toBe("100.00");
  });

  test("a referência chegou: processa normalmente", () => {
    const world = scenario("100.00");
    const refund = newTransaction({ kind: Kind.Refund, amount: "25.00", reference: "bet-atrasada" });
    world.settle(refund);
    const bet = processedBet(world, "25.00", "bet-atrasada");
    world.settle(refund, { reference: bet, now: new Date(NOW.getTime() + 1500) });
    expect(refund.status).toBe(Status.Processed);
    expect(world.wallet.balance.toString()).toBe("100.00");
    world.assertLedgerConsistent();
  });
});
