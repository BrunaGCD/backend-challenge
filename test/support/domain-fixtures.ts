// ====================================================================================================
//                         PEÇAS DE DOMÍNIO PRONTAS PARA OS TESTES DE UNIDADE
// ====================================================================================================
import { Money } from "../../src/domain/money";                                                 // valores
import { Wallet } from "../../src/domain/wallet/wallet";                                        // wallet aberta
import { WagerTransaction, WagerTransactionKind } from "../../src/domain/wagering/wager-transaction"; // transações de provedor
import type { RetryPolicy } from "../../src/domain/backoff";                                    // backoff determinístico

export const PLAYER_ID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1";
export const OTHER_PLAYER_ID = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a2";
export const WALLET_ID = "0192f291-27dd-7d3f-8071-5f8685deef37";
export const NOW = new Date("2026-07-29T15:00:00.000Z");
export const FIXED_RETRY_POLICY: RetryPolicy = { baseMilliseconds: 1000, capMilliseconds: 10_000, random: () => 0.5 };   // metade do teto, sempre

export const brl = (amount: string) => Money.from({ amount, currency: "BRL" });

let idCounter = 0;
export function nextId(prefix = "id"): string {
  idCounter = idCounter + 1;
  return `${prefix}-${idCounter}`;                                                               // ids previsíveis nas mensagens de erro
}

export function openWallet(initialBalance: string, playerId = PLAYER_ID, currency = "BRL") {
  return Wallet.open({
    id: WALLET_ID,
    playerId,
    initialBalance: Money.from({ amount: initialBalance, currency }),
    openingTransactionId: "opening-transaction",
    openingLedgerEntryId: "opening-entry",
    openedAt: NOW,
  });
}

export interface TransactionOverrides {
  kind?: WagerTransactionKind;
  amount?: string;
  currency?: string;
  externalTransactionId?: string;
  reference?: string;
  playerId?: string;
  walletId?: string;
  roundId?: string;
  providerId?: string;
  createdAt?: Date;
}

export function newTransaction(overrides: TransactionOverrides = {}): WagerTransaction {
  const externalTransactionId = overrides.externalTransactionId ?? nextId("ext");
  return WagerTransaction.create({
    id: nextId("tx"),
    providerId: overrides.providerId ?? "provider-a",
    externalTransactionId,
    idempotencyKey: `provider-a:${externalTransactionId}`,
    payloadHash: "a".repeat(64),
    walletId: overrides.walletId ?? WALLET_ID,
    playerId: overrides.playerId ?? PLAYER_ID,
    roundId: overrides.roundId ?? "round-1",
    gameId: "fortune-chimp",
    kind: overrides.kind ?? WagerTransactionKind.Bet,
    money: Money.from({ amount: overrides.amount ?? "10.00", currency: overrides.currency ?? "BRL" }),
    referenceExternalTransactionId: overrides.reference,
    correlationId: "corr-1",
    createdAt: overrides.createdAt ?? NOW,
  });
}
