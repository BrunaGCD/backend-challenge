// ====================================================================================================
//                     DECISÃO DE UMA TRANSAÇÃO: PROCESSAR, REJEITAR OU ESPERAR
// ====================================================================================================
// Regras da seção 7 do desafio num lugar só. Recebe a wallet já travada e a referência já buscada; muda o estado
// da transação e da wallet em memória e devolve o lançamento, se houve movimento. Quem grava é o caso
// de uso, dentro da mesma transação SQL.
import type { RetryPolicy } from "../backoff";                                                  // reagendamento das pendentes
import { InsufficientFundsError } from "../errors";                                             // débito maior que o saldo
import { FailureCode } from "./failure-code";                                                   // motivos de rejeição
import type { Wallet } from "../wallet/wallet";                                                 // saldo travado
import { LedgerDirection, type WalletLedgerEntry } from "../wallet/wallet-ledger-entry";         // lançamento gerado
import { WagerTransactionKind, WagerTransactionStatus, type WagerTransaction } from "./wager-transaction";

export interface SettlementInput {
  transaction: WagerTransaction;                                                                // em PENDING ou PENDING_REFERENCE
  wallet: Wallet;                                                                               // travada com FOR UPDATE
  reference: WagerTransaction | undefined;                                                      // buscada por (providerId, referenceExternalTransactionId)
  referenceAlreadyReversed: boolean;                                                            // a referência já tem REFUND/ROLLBACK processado
  ledgerEntryId: string;
  now: Date;
  pendingRetryPolicy: RetryPolicy;
  referenceTtlMilliseconds: number;
}

export type SettlementOutcome =
  | { outcome: "processed"; ledgerEntry: WalletLedgerEntry | undefined }
  | { outcome: "rejected"; failureCode: FailureCode }
  | { outcome: "entered_pending_reference" }                                                    // primeira vez sem referência
  | { outcome: "still_pending_reference" };                                                     // worker tentou de novo e continua sem

const REFERENCE_KINDS_ALLOWED: Record<WagerTransactionKind, WagerTransactionKind[]> = {
  [WagerTransactionKind.Refund]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Rollback]: [WagerTransactionKind.Bet, WagerTransactionKind.Win, WagerTransactionKind.Refund],
  [WagerTransactionKind.Win]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Loss]: [WagerTransactionKind.Bet],
  [WagerTransactionKind.Bet]: [],                                                               // BET nem aceita referência
  [WagerTransactionKind.Opening]: [],
};

export function settleWagerTransaction(input: SettlementInput): SettlementOutcome {
  const { transaction, wallet, reference, now } = input;

  // ----------------------------------------------------------------------------------------------------
  //                              DONO DA WALLET E MOEDA (TODA OPERAÇÃO)
  // ----------------------------------------------------------------------------------------------------
  if (transaction.playerId !== wallet.playerId) return reject(input, FailureCode.PlayerWalletMismatch);
  if (transaction.money.currency !== wallet.currency) return reject(input, FailureCode.CurrencyMismatch);

  // ----------------------------------------------------------------------------------------------------
  //                     REFERÊNCIA: IDENTIDADE, TIPO E VALOR ANTES DO ESTADO DELA
  // ----------------------------------------------------------------------------------------------------
  // Identidade, tipo e valor da referência nunca mudam, então uma referência errada é rejeitada na hora,
  // mesmo que ela ainda esteja pendente. Só o estado dela pode mudar com o tempo.
  if (transaction.referenceExternalTransactionId !== undefined) {
    const referencesItself = transaction.referenceExternalTransactionId === transaction.externalTransactionId;   // seria uma do próprio tipo, que a regra 3 nunca permite
    if (referencesItself) return reject(input, FailureCode.ReferenceKindNotAllowed);           // rejeita na hora, em vez de esperar uma referência que nunca chega
    if (!reference) return waitForReference(input);

    const sameOwner = reference.providerId === transaction.providerId && reference.playerId === transaction.playerId && reference.walletId === transaction.walletId;
    const sameRoundAndCurrency = reference.roundId === transaction.roundId && reference.money.currency === transaction.money.currency;   // gameId não é comparado (I4)
    if (!sameOwner || !sameRoundAndCurrency) return reject(input, FailureCode.ReferenceMismatch);

    if (!REFERENCE_KINDS_ALLOWED[transaction.kind].includes(reference.kind)) return reject(input, FailureCode.ReferenceKindNotAllowed);
    if (transaction.requiresReference() && !reference.money.equals(transaction.money)) return reject(input, FailureCode.ReferenceAmountMismatch);

    if (reference.status === WagerTransactionStatus.PendingReference) return waitForReference(input);   // I2: a referência também espera
    if (reference.status !== WagerTransactionStatus.Processed) return reject(input, FailureCode.ReferenceNotProcessed);
    if (transaction.requiresReference() && input.referenceAlreadyReversed) return reject(input, FailureCode.ReferenceAlreadyReversed);
  }

  // ----------------------------------------------------------------------------------------------------
  //                                     MOVIMENTO DO SALDO
  // ----------------------------------------------------------------------------------------------------
  if (!transaction.affectsBalance()) {
    transaction.markProcessed(reference?.id, wallet.balance, now);                              // LOSS: registra sem mover saldo
    return { outcome: "processed", ledgerEntry: undefined };
  }

  const direction = transaction.ledgerDirectionFor(reference);
  const movementContext = { transactionId: transaction.id, ledgerEntryId: input.ledgerEntryId, at: now };
  try {
    const ledgerEntry = direction === LedgerDirection.Credit ? wallet.credit(transaction.money, movementContext) : wallet.debit(transaction.money, movementContext);
    transaction.markProcessed(reference?.id, wallet.balance, now);
    return { outcome: "processed", ledgerEntry };
  } catch (error) {
    if (!(error instanceof InsufficientFundsError)) throw error;
    const isReversal = transaction.kind === WagerTransactionKind.Rollback;                      // desfazer crédito já gasto é outro problema (regra 9)
    return reject(input, isReversal ? FailureCode.ReversalInsufficientFunds : FailureCode.InsufficientFunds);
  }
}

function reject(input: SettlementInput, failureCode: FailureCode): SettlementOutcome {
  input.transaction.reject(failureCode, input.wallet.balance, input.now);
  return { outcome: "rejected", failureCode };
}

function waitForReference(input: SettlementInput): SettlementOutcome {
  const { transaction, wallet, now } = input;
  if (transaction.status === WagerTransactionStatus.Pending) {
    transaction.markPendingReference(wallet.balance, now, input.pendingRetryPolicy);
    return { outcome: "entered_pending_reference" };
  }
  if (transaction.hasReferenceWaitExpired(now, input.referenceTtlMilliseconds)) {
    return reject(input, FailureCode.ReferenceNotFound);                                         // TTL acabou
  }
  transaction.scheduleReferenceRetry(now, input.pendingRetryPolicy);
  return { outcome: "still_pending_reference" };
}
