import { describe, expect, test } from "bun:test";                                             // runner de testes
import { CurrencyMismatchError, InsufficientFundsError, InvalidLedgerEntryError } from "../../src/domain/errors";
import { Money } from "../../src/domain/money";                                                 // valores
import { LedgerDirection, WalletLedgerEntry } from "../../src/domain/wallet-ledger-entry";       // lançamentos
import { brl, NOW, openWallet } from "../support/domain-fixtures";                              // wallet pronta
import { ledgerChainProblems } from "../support/ledger-chain";                                  // invariante final

const movement = (suffix: string) => ({ transactionId: `tx-${suffix}`, ledgerEntryId: `entry-${suffix}`, at: NOW });

describe("Wallet.open", () => {
  test("com saldo inicial: nasce na versão 1 e devolve o lançamento de abertura com wallet_version 1", () => {
    const { wallet, openingEntry } = openWallet("1000.00");
    expect(wallet.balance.toString()).toBe("1000.00");
    expect(wallet.version).toBe(1);
    expect(openingEntry?.direction).toBe(LedgerDirection.Credit);
    expect(openingEntry?.balanceBefore.toString()).toBe("0.00");
    expect(openingEntry?.balanceAfter.toString()).toBe("1000.00");
    expect(openingEntry?.walletVersion).toBe(1);
    expect(ledgerChainProblems(wallet.balance, [openingEntry!])).toEqual([]);
  });

  test("com saldo zero: versão 1, sem lançamento", () => {
    const { wallet, openingEntry } = openWallet("0.00");
    expect(wallet.version).toBe(1);
    expect(wallet.balance.isZero()).toBe(true);
    expect(openingEntry).toBeUndefined();
  });

  test("a moeda da wallet é a do saldo inicial", () => {
    const { wallet } = openWallet("5.00", undefined, "USD");
    expect(wallet.currency).toBe("USD");
  });
});

describe("Wallet: débito e crédito mantêm saldo e ledger juntos", () => {
  test("cada movimento devolve o lançamento, sobe a versão em 1 e a cadeia fecha", () => {
    const { wallet, openingEntry } = openWallet("100.00");
    const debitEntry = wallet.debit(brl("80.00"), movement("1"));
    const creditEntry = wallet.credit(brl("15.50"), movement("2"));

    expect(wallet.balance.toString()).toBe("35.50");
    expect(wallet.version).toBe(3);
    expect(debitEntry.balanceBefore.toString()).toBe("100.00");
    expect(debitEntry.balanceAfter.toString()).toBe("20.00");
    expect(debitEntry.walletVersion).toBe(2);
    expect(creditEntry.walletVersion).toBe(3);
    expect(ledgerChainProblems(wallet.balance, [openingEntry!, debitEntry, creditEntry])).toEqual([]);
  });

  test("débito que deixaria saldo negativo lança InsufficientFundsError e não muda nada", () => {
    const { wallet } = openWallet("100.00");
    expect(() => wallet.debit(brl("100.01"), movement("1"))).toThrow(InsufficientFundsError);
    expect(wallet.balance.toString()).toBe("100.00");
    expect(wallet.version).toBe(1);
  });

  test("débito de exatamente o saldo leva a 0.00", () => {
    const { wallet } = openWallet("80.00");
    wallet.debit(brl("80.00"), movement("1"));
    expect(wallet.balance.toJSON()).toEqual({ amount: "0.00", currency: "BRL" });
  });

  test("movimento em outra moeda lança CurrencyMismatchError e não muda nada", () => {
    const { wallet } = openWallet("100.00");
    const usd = Money.from({ amount: "1.00", currency: "USD" });
    expect(() => wallet.debit(usd, movement("1"))).toThrow(CurrencyMismatchError);
    expect(() => wallet.credit(usd, movement("2"))).toThrow(CurrencyMismatchError);
    expect(wallet.version).toBe(1);
  });

  test("movimento de zero é recusado (version só sobe quando o saldo muda)", () => {
    const { wallet } = openWallet("100.00");
    expect(() => wallet.credit(brl("0.00"), movement("1"))).toThrow(InvalidLedgerEntryError);
    expect(wallet.version).toBe(1);
  });
});

describe("WalletLedgerEntry", () => {
  const base = {
    id: "entry-1", walletId: "wallet-1", transactionId: "tx-1", direction: LedgerDirection.Debit,
    money: brl("10.00"), balanceBefore: brl("50.00"), balanceAfter: brl("40.00"), walletVersion: 2, createdAt: NOW,
  };

  test("create aceita quando a conta fecha, e o lançamento é congelado", () => {
    const entry = WalletLedgerEntry.create(base);
    expect(entry.isBalanced()).toBe(true);
    expect(Object.isFrozen(entry)).toBe(true);
  });

  test("create recusa conta que não fecha", () => {
    expect(() => WalletLedgerEntry.create({ ...base, balanceAfter: brl("41.00") })).toThrow(InvalidLedgerEntryError);
    expect(() => WalletLedgerEntry.create({ ...base, direction: LedgerDirection.Credit })).toThrow(InvalidLedgerEntryError);
  });

  test("create recusa valor zero, saldo negativo e versão inválida", () => {
    expect(() => WalletLedgerEntry.create({ ...base, money: brl("0.00"), balanceAfter: brl("50.00") })).toThrow(InvalidLedgerEntryError);
    expect(() => WalletLedgerEntry.create({ ...base, money: brl("60.00"), balanceAfter: brl("50.00").subtract(brl("60.00")) })).toThrow(InvalidLedgerEntryError);
    expect(() => WalletLedgerEntry.create({ ...base, walletVersion: 0 })).toThrow(InvalidLedgerEntryError);
  });

  test("não tem como alterar um lançamento depois de criado", () => {
    const entry = WalletLedgerEntry.create(base) as unknown as Record<string, unknown>;
    expect(() => { entry.balanceAfter = brl("1.00"); }).toThrow(TypeError);                    // congelado: atribuição lança em modo estrito
  });
});
