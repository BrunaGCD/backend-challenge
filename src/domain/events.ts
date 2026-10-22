// ====================================================================================================
//                            EVENTOS DE INTEGRAÇÃO (SEÇÃO 11 DO DESAFIO)
// ====================================================================================================
// Envelope abstrato e uma subclasse por evento: eventType e version ficam no tipo, não numa string
// solta no call site. data carrega MoneyProps (string decimal), nunca a instância de Money.
import type { FailureCode } from "./failure-code";                                              // motivo de rejeição ou falha
import type { MoneyProps } from "./money";                                                      // dinheiro serializado
import type { Wallet } from "./wallet";                                                         // dona do evento de saldo
import type { LedgerDirection, WalletLedgerEntry } from "./wallet-ledger-entry";                 // movimento do saldo
import type { WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from "./wager-transaction";

export interface IntegrationEventProps<T> {
  eventId: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: Date;
  data: T;
}

export interface EventContext {
  eventId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: Date;
}

export interface SerializedIntegrationEvent<T> {
  eventId: string;
  eventType: string;
  aggregateId: string;
  correlationId: string;
  causationId?: string;
  occurredAt: string;                                                                           // ISO-8601
  version: number;
  data: T;
}

export abstract class IntegrationEvent<T> {
  abstract readonly eventType: string;
  abstract readonly version: number;

  readonly eventId: string;
  readonly aggregateId: string;
  readonly correlationId: string;
  readonly causationId?: string;
  readonly occurredAt: Date;
  readonly data: Readonly<T>;

  protected constructor(props: IntegrationEventProps<T>) {
    this.eventId = props.eventId;
    this.aggregateId = props.aggregateId;
    this.correlationId = props.correlationId;
    this.causationId = props.causationId;
    this.occurredAt = props.occurredAt;
    this.data = props.data;
  }

  toJSON(): SerializedIntegrationEvent<T> {
    return {
      eventId: this.eventId,
      eventType: this.eventType,
      aggregateId: this.aggregateId,
      correlationId: this.correlationId,
      ...(this.causationId === undefined ? {} : { causationId: this.causationId }),
      occurredAt: this.occurredAt.toISOString(),
      version: this.version,
      data: this.data as T,
    };
  }
}

// ----------------------------------------------------------------------------------------------------
//                              EVENTOS DO CICLO DE VIDA DA TRANSAÇÃO
// ----------------------------------------------------------------------------------------------------
export interface WagerTransactionEventData {
  transactionId: string;
  walletId: string;
  providerId: string | null;                                                                    // null só no OPENING
  externalTransactionId: string | null;
  roundId: string | null;
  gameId: string | null;
  kind: WagerTransactionKind;
  status: WagerTransactionStatus;
  money: MoneyProps;
  referenceExternalTransactionId: string | null;
  referenceTransactionId: string | null;
  failureCode: FailureCode | null;
  balance: MoneyProps | null;                                                                   // saldo observado na decisão
}

function transactionEventData(transaction: WagerTransaction): WagerTransactionEventData {
  return {
    transactionId: transaction.id,
    walletId: transaction.walletId,
    providerId: transaction.providerId ?? null,
    externalTransactionId: transaction.externalTransactionId ?? null,
    roundId: transaction.roundId ?? null,
    gameId: transaction.gameId ?? null,
    kind: transaction.kind,
    status: transaction.status,
    money: transaction.money.toJSON(),
    referenceExternalTransactionId: transaction.referenceExternalTransactionId ?? null,
    referenceTransactionId: transaction.referenceTransactionId ?? null,
    failureCode: transaction.failureCode ?? null,
    balance: transaction.balanceAfter?.toJSON() ?? null,
  };
}

function transactionEventProps(transaction: WagerTransaction, context: EventContext): IntegrationEventProps<WagerTransactionEventData> {
  return { ...context, aggregateId: transaction.walletId, data: transactionEventData(transaction) };   // aggregateId = walletId: um grupo FIFO por wallet
}

export class WagerTransactionProcessed extends IntegrationEvent<WagerTransactionEventData> {
  readonly eventType = "WagerTransactionProcessed";
  readonly version = 1;

  static from(transaction: WagerTransaction, context: EventContext): WagerTransactionProcessed {
    return new WagerTransactionProcessed(transactionEventProps(transaction, context));
  }
}

export class WagerTransactionRejected extends IntegrationEvent<WagerTransactionEventData> {
  readonly eventType = "WagerTransactionRejected";
  readonly version = 1;

  static from(transaction: WagerTransaction, context: EventContext): WagerTransactionRejected {
    return new WagerTransactionRejected(transactionEventProps(transaction, context));
  }
}

export class WagerTransactionPendingReference extends IntegrationEvent<WagerTransactionEventData> {
  readonly eventType = "WagerTransactionPendingReference";
  readonly version = 1;

  static from(transaction: WagerTransaction, context: EventContext): WagerTransactionPendingReference {
    return new WagerTransactionPendingReference(transactionEventProps(transaction, context));
  }
}

export class WagerTransactionFailed extends IntegrationEvent<WagerTransactionEventData> {
  readonly eventType = "WagerTransactionFailed";
  readonly version = 1;

  static from(transaction: WagerTransaction, context: EventContext): WagerTransactionFailed {
    return new WagerTransactionFailed(transactionEventProps(transaction, context));
  }
}

// ----------------------------------------------------------------------------------------------------
//                                    EVENTO DE SALDO
// ----------------------------------------------------------------------------------------------------
export interface WalletBalanceChangedData {
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: MoneyProps;
  balanceBefore: MoneyProps;
  balanceAfter: MoneyProps;
  walletVersion: number;                                                                        // cresce de 1 em 1: o consumidor ordena por ele
}

export class WalletBalanceChanged extends IntegrationEvent<WalletBalanceChangedData> {
  readonly eventType = "WalletBalanceChanged";
  readonly version = 1;

  static from(wallet: Wallet, entry: WalletLedgerEntry, context: EventContext): WalletBalanceChanged {
    return new WalletBalanceChanged({
      ...context,
      aggregateId: wallet.id,
      data: {
        walletId: wallet.id,
        transactionId: entry.transactionId,
        direction: entry.direction,
        money: entry.money.toJSON(),
        balanceBefore: entry.balanceBefore.toJSON(),
        balanceAfter: entry.balanceAfter.toJSON(),
        walletVersion: entry.walletVersion,
      },
    });
  }
}
