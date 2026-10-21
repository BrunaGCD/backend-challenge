// ====================================================================================================
//                       FORMATO DAS RESPOSTAS HTTP E O STATUS DE CADA SITUAÇÃO
// ====================================================================================================
import type { ReconciliationResult } from "../../application/queries";
import type { Wallet } from "../../domain/wallet";
import type { WalletLedgerEntry } from "../../domain/wallet-ledger-entry";
import { WagerTransactionStatus, type WagerTransaction } from "../../domain/wager-transaction";

export function presentWallet(wallet: Wallet, includeUpdatedAt: boolean) {
  const body: Record<string, unknown> = { id: wallet.id, playerId: wallet.playerId, balance: wallet.balance.toJSON(), version: wallet.version };
  if (includeUpdatedAt) body.updatedAt = wallet.updatedAt.toISOString();
  return body;
}

// Mesmo formato em 200, 202 e 422 (seção 9), mais failureCode quando houver.
export function presentTransactionResult(transaction: WagerTransaction, idempotentReplay: boolean) {
  const body: Record<string, unknown> = {
    transactionId: transaction.id,
    status: transaction.status,
    balance: transaction.balanceAfter?.toJSON() ?? null,                                        // saldo observado na decisão
    idempotentReplay,
  };
  if (transaction.failureCode) body.failureCode = transaction.failureCode;
  return body;
}

export function presentTransaction(transaction: WagerTransaction) {
  return {
    id: transaction.id,
    providerId: transaction.providerId ?? null,
    externalTransactionId: transaction.externalTransactionId ?? null,
    playerId: transaction.playerId,
    walletId: transaction.walletId,
    roundId: transaction.roundId ?? null,
    gameId: transaction.gameId ?? null,
    kind: transaction.kind,
    status: transaction.status,
    money: transaction.money.toJSON(),
    referenceExternalTransactionId: transaction.referenceExternalTransactionId ?? null,
    referenceTransactionId: transaction.referenceTransactionId ?? null,
    failureCode: transaction.failureCode ?? null,
    balance: transaction.balanceAfter?.toJSON() ?? null,
    createdAt: transaction.createdAt.toISOString(),
    processedAt: transaction.processedAt?.toISOString() ?? null,
  };
}

export function presentLedgerEntry(entry: WalletLedgerEntry) {
  return {
    id: entry.id,
    transactionId: entry.transactionId,
    direction: entry.direction,
    money: entry.money.toJSON(),
    balanceBefore: entry.balanceBefore.toJSON(),
    balanceAfter: entry.balanceAfter.toJSON(),
    walletVersion: entry.walletVersion,
    createdAt: entry.createdAt.toISOString(),
  };
}

export function presentReconciliation(result: ReconciliationResult) {
  return {
    walletId: result.walletId,
    storedBalance: result.storedBalance.toJSON(),
    calculatedBalance: result.calculatedBalance.toJSON(),
    difference: result.difference.toJSON(),
    consistent: result.consistent,
    checkedEntries: result.checkedEntries,
  };
}

// 200 processada, 202 pendente, 422 rejeitada. FAILED só aparece em replay de uma pendente que
// falhou no worker: não é regra de negócio nem algo que o provedor corrija, então fica 500 com o
// corpo da transação (existe uma WagerTransaction, então o corpo é ela).
export function statusCodeForTransaction(transaction: WagerTransaction): number {
  switch (transaction.status) {
    case WagerTransactionStatus.Processed:
      return 200;
    case WagerTransactionStatus.PendingReference:
      return 202;
    case WagerTransactionStatus.Rejected:
      return 422;
    default:
      return 500;
  }
}
