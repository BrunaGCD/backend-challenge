// ====================================================================================================
//                    WAGER TRANSACTION: UMA OPERAÇÃO DO PROVEDOR E O SEU ESTADO
// ====================================================================================================
// Nasce em PENDING (só em memória) e sai dele antes do commit. PROCESSED, REJECTED e FAILED são
// terminais: tentar sair deles é erro de programação, não caminho de negócio.
import { fullJitterDelayMilliseconds, type RetryPolicy } from "./backoff";                     // reagendamento do worker
import { InvalidTransactionStateError, InvalidWagerTransactionError } from "./errors";          // estado e criação inválidos
import { FailureCode } from "./failure-code";                                                   // motivo de rejeição ou falha
import { Money } from "./money";                                                                // valor exato
import { LedgerDirection } from "./wallet-ledger-entry";                                        // débito ou crédito

export enum WagerTransactionKind {
  Opening = "OPENING",                                                                          // interno: crédito de abertura da wallet
  Bet = "BET",
  Win = "WIN",
  Loss = "LOSS",
  Refund = "REFUND",
  Rollback = "ROLLBACK",
}

export enum WagerTransactionStatus {
  Pending = "PENDING",                                                                          // aceita, ainda não aplicada (nunca gravada)
  PendingReference = "PENDING_REFERENCE",                                                       // aguardando a transação referenciada
  Processed = "PROCESSED",                                                                      // aplicada (terminal)
  Rejected = "REJECTED",                                                                        // violação de regra de negócio (terminal)
  Failed = "FAILED",                                                                            // erro permanente de infraestrutura (terminal)
}

const TERMINAL_STATUSES = [WagerTransactionStatus.Processed, WagerTransactionStatus.Rejected, WagerTransactionStatus.Failed];

export interface WagerTransactionState {
  id: string;
  providerId: string | undefined;                                                               // ausente só no OPENING
  externalTransactionId: string | undefined;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string | undefined;
  gameId: string | undefined;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId: string | undefined;                                           // id no provedor, não o id interno
  referenceTransactionId: string | undefined;                                                   // id interno, preenchido quando processa
  status: WagerTransactionStatus;
  failureCode: FailureCode | undefined;
  balanceAfter: Money | undefined;                                                              // saldo observado na decisão
  correlationId: string;
  attempts: number;
  nextAttemptAt: Date | undefined;
  createdAt: Date;
  updatedAt: Date;
  processedAt: Date | undefined;
}

export interface CreateWagerTransactionProps {
  id: string;
  providerId: string;
  externalTransactionId: string;
  idempotencyKey: string;
  payloadHash: string;
  walletId: string;
  playerId: string;
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId: string | undefined;
  correlationId: string;
  createdAt: Date;
}

export interface CreateOpeningTransactionProps {
  id: string;
  walletId: string;
  playerId: string;
  money: Money;
  payloadHash: string;
  correlationId: string;
  createdAt: Date;
}

export class WagerTransaction {
  private constructor(private state: WagerTransactionState) {}

  // ----------------------------------------------------------------------------------------------------
  //                                         FACTORIES
  // ----------------------------------------------------------------------------------------------------
  static create(props: CreateWagerTransactionProps): WagerTransaction {
    if (props.kind === WagerTransactionKind.Opening) throw new InvalidWagerTransactionError("OPENING é interno e não pode vir de provedor");
    const hasReference = props.referenceExternalTransactionId !== undefined;
    const referenceRequired = props.kind === WagerTransactionKind.Refund || props.kind === WagerTransactionKind.Rollback;
    if (referenceRequired && !hasReference) throw new InvalidWagerTransactionError(`${props.kind} exige referenceExternalTransactionId`);
    if (props.kind === WagerTransactionKind.Bet && hasReference) throw new InvalidWagerTransactionError("BET não aceita referenceExternalTransactionId");
    if (props.money.isNegative()) throw new InvalidWagerTransactionError("valor não pode ser negativo");
    if (props.kind !== WagerTransactionKind.Loss && props.money.isZero()) throw new InvalidWagerTransactionError(`${props.kind} exige valor maior que zero`);

    return new WagerTransaction({
      ...props,
      referenceTransactionId: undefined,
      status: WagerTransactionStatus.Pending,
      failureCode: undefined,
      balanceAfter: undefined,
      attempts: 0,
      nextAttemptAt: undefined,
      updatedAt: props.createdAt,
      processedAt: undefined,
    });
  }

  static createOpening(props: CreateOpeningTransactionProps): WagerTransaction {
    if (!props.money.isPositive()) throw new InvalidWagerTransactionError("OPENING só existe com saldo inicial maior que zero");
    return new WagerTransaction({
      id: props.id,
      providerId: undefined,                                                                    // não vem de provedor (interpretação I8)
      externalTransactionId: undefined,
      idempotencyKey: `opening:${props.walletId}`,                                              // também impede dois OPENING na mesma wallet
      payloadHash: props.payloadHash,
      walletId: props.walletId,
      playerId: props.playerId,
      roundId: undefined,
      gameId: undefined,
      kind: WagerTransactionKind.Opening,
      money: props.money,
      referenceExternalTransactionId: undefined,
      referenceTransactionId: undefined,
      status: WagerTransactionStatus.Pending,
      failureCode: undefined,
      balanceAfter: undefined,
      correlationId: props.correlationId,
      attempts: 0,
      nextAttemptAt: undefined,
      createdAt: props.createdAt,
      updatedAt: props.createdAt,
      processedAt: undefined,
    });
  }

  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction({ ...state });                                                  // só reconstrói, não revalida transições
  }

  // ----------------------------------------------------------------------------------------------------
  //                                          LEITURA
  // ----------------------------------------------------------------------------------------------------
  get id() { return this.state.id; }
  get providerId() { return this.state.providerId; }
  get externalTransactionId() { return this.state.externalTransactionId; }
  get idempotencyKey() { return this.state.idempotencyKey; }
  get payloadHash() { return this.state.payloadHash; }
  get walletId() { return this.state.walletId; }
  get playerId() { return this.state.playerId; }
  get roundId() { return this.state.roundId; }
  get gameId() { return this.state.gameId; }
  get kind() { return this.state.kind; }
  get money() { return this.state.money; }
  get referenceExternalTransactionId() { return this.state.referenceExternalTransactionId; }
  get referenceTransactionId() { return this.state.referenceTransactionId; }
  get status() { return this.state.status; }
  get failureCode() { return this.state.failureCode; }
  get balanceAfter() { return this.state.balanceAfter; }
  get correlationId() { return this.state.correlationId; }
  get attempts() { return this.state.attempts; }
  get nextAttemptAt() { return this.state.nextAttemptAt; }
  get createdAt() { return this.state.createdAt; }
  get updatedAt() { return this.state.updatedAt; }
  get processedAt() { return this.state.processedAt; }

  toState(): WagerTransactionState {
    return { ...this.state };                                                                   // cópia: quem lê não muda o estado por fora
  }

  // ----------------------------------------------------------------------------------------------------
  //                                         TRANSIÇÕES
  // ----------------------------------------------------------------------------------------------------
  markProcessed(referenceTransactionId: string | undefined, observedBalance: Money, at: Date): void {
    this.assertNotTerminal("markProcessed");
    this.state.status = WagerTransactionStatus.Processed;
    this.state.referenceTransactionId = referenceTransactionId;
    this.state.balanceAfter = observedBalance;
    this.state.processedAt = at;
    this.state.nextAttemptAt = undefined;
    this.state.updatedAt = at;
  }

  markPendingReference(observedBalance: Money, now: Date, retryPolicy: RetryPolicy): void {
    if (this.state.status !== WagerTransactionStatus.Pending) {
      throw new InvalidTransactionStateError(`markPendingReference só sai de PENDING, a transação ${this.id} está ${this.status}`);
    }
    this.state.status = WagerTransactionStatus.PendingReference;
    this.state.balanceAfter = observedBalance;
    this.state.attempts = 0;
    this.state.nextAttemptAt = new Date(now.getTime() + fullJitterDelayMilliseconds(0, retryPolicy));
    this.state.updatedAt = now;
  }

  reject(code: FailureCode, observedBalance: Money, at: Date): void {
    this.assertNotTerminal("reject");
    this.state.status = WagerTransactionStatus.Rejected;
    this.state.failureCode = code;
    this.state.balanceAfter = observedBalance;
    this.state.nextAttemptAt = undefined;
    this.state.updatedAt = at;
  }

  fail(code: FailureCode, at: Date): void {
    if (this.state.status !== WagerTransactionStatus.PendingReference) {                        // PENDING -> FAILED não existe (seção 6.3)
      throw new InvalidTransactionStateError(`fail só sai de PENDING_REFERENCE, a transação ${this.id} está ${this.status}`);
    }
    this.state.status = WagerTransactionStatus.Failed;
    this.state.failureCode = code;
    this.state.nextAttemptAt = undefined;
    this.state.updatedAt = at;
  }

  // ---- nova tentativa do worker sem referência: não é transição de estado
  scheduleReferenceRetry(now: Date, retryPolicy: RetryPolicy): void {
    if (this.state.status !== WagerTransactionStatus.PendingReference) {
      throw new InvalidTransactionStateError(`scheduleReferenceRetry só vale em PENDING_REFERENCE, a transação ${this.id} está ${this.status}`);
    }
    this.state.attempts = this.state.attempts + 1;
    this.state.nextAttemptAt = new Date(now.getTime() + fullJitterDelayMilliseconds(this.state.attempts, retryPolicy));
    this.state.updatedAt = now;
  }

  // ----------------------------------------------------------------------------------------------------
  //                                     CONSULTAS DE DOMÍNIO
  // ----------------------------------------------------------------------------------------------------
  isTerminal(): boolean {
    return TERMINAL_STATUSES.includes(this.state.status);
  }

  affectsBalance(): boolean {
    return this.state.kind !== WagerTransactionKind.Loss;                                       // LOSS só registra o resultado
  }

  requiresReference(): boolean {
    return this.state.kind === WagerTransactionKind.Refund || this.state.kind === WagerTransactionKind.Rollback;
  }

  matchesPayload(payloadHash: string): boolean {
    return this.state.payloadHash === payloadHash;
  }

  hasReferenceWaitExpired(now: Date, ttlMilliseconds: number): boolean {
    return now.getTime() - this.state.createdAt.getTime() >= ttlMilliseconds;                   // TTL contado do created_at da pendente
  }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.state.kind) {
      case WagerTransactionKind.Bet:
        return LedgerDirection.Debit;
      case WagerTransactionKind.Win:
      case WagerTransactionKind.Refund:
      case WagerTransactionKind.Opening:
        return LedgerDirection.Credit;
      case WagerTransactionKind.Rollback: {
        if (!reference) throw new InvalidWagerTransactionError("ROLLBACK precisa da referência para saber a direção");
        const referenceDirection = reference.ledgerDirectionFor();
        return referenceDirection === LedgerDirection.Debit ? LedgerDirection.Credit : LedgerDirection.Debit;   // inverso da referência
      }
      case WagerTransactionKind.Loss:
        throw new InvalidWagerTransactionError("LOSS não move saldo, então não tem direção");
    }
  }

  private assertNotTerminal(transition: string): void {
    if (this.isTerminal()) {
      throw new InvalidTransactionStateError(`${transition} recusado: a transação ${this.id} já está em ${this.status}, que é terminal`);
    }
  }
}
