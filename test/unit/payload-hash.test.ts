// ====================================================================================================
//            PAYLOAD HASH COM VETORES FIXOS E A IDEMPOTENCY KEY COM PAYLOAD DIVERGENTE
// ====================================================================================================
// O JSON canônico de cada vetor foi escrito à mão seguindo a RFC 8785 (chaves em ordem, sem espaços), e
// o hash esperado foi calculado fora do projeto, com o sha256sum do Git Bash, sobre esse mesmo texto.
import { describe, expect, test } from "bun:test";
import canonicalize from "canonicalize";
import { canonicalPayload, computeOpeningPayloadHash, computePayloadHash } from "../../src/application/payload-hash";
import { parseWagerCommand, type WagerCommand } from "../../src/application/wager-command";
import { Money } from "../../src/domain/money";
import { IdempotencyKeyConflictError } from "../../src/application/errors";
import type { Repositories } from "../../src/application/ports";
import { ProcessWagerTransaction } from "../../src/application/process-wager-transaction";
import { WagerTransaction } from "../../src/domain/wager-transaction";

const SPEC_EXAMPLE = {
  providerId: "provider-a",
  externalTransactionId: "transaction-123",
  playerId: "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
  walletId: "0192f291-27dd-7d3f-8071-5f8685deef37",
  roundId: "round-987",
  gameId: "fortune-chimp",
  kind: "BET",
  money: { amount: "25.00", currency: "BRL" },
};

function command(body: Record<string, unknown>): WagerCommand {
  const parsed = parseWagerCommand(body, ["BRL", "USD"]);
  if (!parsed.ok) throw new Error(JSON.stringify(parsed.errors));
  return parsed.value;
}

describe("vetores fixos", () => {
  test("BET do exemplo da seção 9 do desafio", () => {
    const expectedCanonical = '{"externalTransactionId":"transaction-123","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"},"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","providerId":"provider-a","roundId":"round-987","walletId":"0192f291-27dd-7d3f-8071-5f8685deef37"}';
    expect(canonicalPayload(command(SPEC_EXAMPLE))).toBe(expectedCanonical);
    expect(computePayloadHash(command(SPEC_EXAMPLE))).toBe("629836932b79106b99523d06a1e7fa80689b0ea1e1c47aa3f0a5a2c87d0c4344");
  });

  test("REFUND com referência: a chave entra na posição da ordem", () => {
    const refund = { ...SPEC_EXAMPLE, kind: "REFUND", externalTransactionId: "refund-1", referenceExternalTransactionId: "transaction-123" };
    expect(computePayloadHash(command(refund))).toBe("b20e66e900b7c36ba1cb7a112a7c31991b1fd658b41b56851205fc3dfab6dd60");
  });

  test("OPENING: kind, walletId e money", () => {
    expect(computeOpeningPayloadHash("0192f291-27dd-7d3f-8071-5f8685deef37", Money.from({ amount: "1000.00", currency: "BRL" }))).toBe("9e3f8e3c0cfb8b24fc6638b1da66363f9a03d7b3741ade0f3cd6155abb204501");
  });

  test("a biblioteca segue a ordenação da RFC 8785, seção 3.2.3 (por unidades UTF-16)", () => {
    const rfcExample = { "€": "Euro Sign", "\r": "Carriage Return", "דּ": "Hebrew Letter Dalet With Dagesh", "1": "One", "😀": "Emoji: Grinning Face", "\u0080": "Control", "ö": "Latin Small Letter O With Diaeresis" };
    const canonical = canonicalize(rfcExample) as string;
    const expectedOrder = ["\r", "1", "\u0080", "ö", "€", "😀", "דּ"];
    const positions = expectedOrder.map((key) => canonical.indexOf(`${JSON.stringify(key)}:`));   // posição de cada chave no texto (reler com JSON.parse reordenaria o "1")
    expect(positions.every((position) => position >= 0)).toBe(true);
    expect([...positions].sort((first, second) => first - second)).toEqual(positions);
  });
});

describe("o que entra e o que não entra no hash", () => {
  test("ordem das chaves no corpo e UUID em maiúsculas não mudam o hash", () => {
    const reordered = { money: { currency: "BRL", amount: "25.00" }, kind: "BET", gameId: "fortune-chimp", roundId: "round-987", walletId: SPEC_EXAMPLE.walletId.toUpperCase(), playerId: SPEC_EXAMPLE.playerId.toUpperCase(), externalTransactionId: "transaction-123", providerId: "provider-a" };
    expect(computePayloadHash(command(reordered))).toBe(computePayloadHash(command(SPEC_EXAMPLE)));
  });

  test("referência ausente e null são a mesma coisa", () => {
    const win = { ...SPEC_EXAMPLE, kind: "WIN" };
    expect(computePayloadHash(command({ ...win, referenceExternalTransactionId: null }))).toBe(computePayloadHash(command(win)));
  });

  test("idempotency key com payload divergente: qualquer campo de negócio diferente muda o hash", () => {
    const original = computePayloadHash(command(SPEC_EXAMPLE));
    const variations = [
      { ...SPEC_EXAMPLE, money: { amount: "25.01", currency: "BRL" } },
      { ...SPEC_EXAMPLE, money: { amount: "25.00", currency: "USD" } },
      { ...SPEC_EXAMPLE, providerId: "provider-b" },
      { ...SPEC_EXAMPLE, roundId: "round-988" },
      { ...SPEC_EXAMPLE, gameId: "other-game" },
      { ...SPEC_EXAMPLE, kind: "WIN" },
      { ...SPEC_EXAMPLE, kind: "WIN", referenceExternalTransactionId: "bet-1" },
    ];
    for (const variation of variations) expect(computePayloadHash(command(variation))).not.toBe(original);
  });
});

describe("idempotency key com payload divergente no caso de uso: conflito, não replay", () => {
  // portas falsas só aqui, em teste de unidade: a transação já gravada com a key, e um runner que
  // acusa se o caso de uso tentar abrir transação (replay e conflito se resolvem antes do lock)
  const IDEMPOTENCY_KEY = "provider-a:transaction-123";
  const original = command(SPEC_EXAMPLE);
  const stored = WagerTransaction.create({ ...original, id: "tx-1", idempotencyKey: IDEMPOTENCY_KEY, payloadHash: computePayloadHash(original), correlationId: "corr-1", createdAt: new Date("2026-07-29T15:00:00.000Z") });
  stored.markProcessed(undefined, Money.from({ amount: "975.00", currency: "BRL" }), new Date("2026-07-29T15:00:00.000Z"));

  const repositories = { transactions: { findByIdempotencyKey: async (key: string) => (key === IDEMPOTENCY_KEY ? stored : undefined) } } as unknown as Repositories;
  const runnerThatMustNotRun = { run: async () => { throw new Error("o caso de uso não devia abrir transação"); } };
  const useCase = new ProcessWagerTransaction(runnerThatMustNotRun, repositories, { now: () => new Date() }, { newId: () => "id" }, { pendingRetryPolicy: { baseMilliseconds: 1, capMilliseconds: 1, random: () => 0 }, referenceTtlMilliseconds: 1 });
  const context = { channel: "http" as const, correlationId: "corr-2", causationId: IDEMPOTENCY_KEY };

  test("mesma key e mesmo payload: replay, com a transação original e o saldo da época", async () => {
    const result = await useCase.execute(command(SPEC_EXAMPLE), IDEMPOTENCY_KEY, context);
    expect(result).toEqual({ kind: "decided", transaction: stored, idempotentReplay: true });
    if (result.kind === "decided") expect(result.transaction.balanceAfter?.toString()).toBe("975.00");
  });

  test("mesma key e payload diferente: IdempotencyKeyConflictError, nunca o resultado da original", async () => {
    const divergent = command({ ...SPEC_EXAMPLE, money: { amount: "25.01", currency: "BRL" } });
    await expect(useCase.execute(divergent, IDEMPOTENCY_KEY, context)).rejects.toBeInstanceOf(IdempotencyKeyConflictError);
  });
});
