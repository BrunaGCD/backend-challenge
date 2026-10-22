// ====================================================================================================
//                    WAGER TRANSACTION: UMA OPERAÇÃO DO PROVEDOR E O SEU ESTADO
// ====================================================================================================
// Nasce em PENDING (só em memória) e sai dele antes do commit. PROCESSED, REJECTED e FAILED são
// terminais: tentar sair deles é erro de programação, não caminho de negócio. A forma segue o
// esqueleto da seção 6.3 do desafio: identidade e negócio readonly, e só o estado da decisão privado e mutável.
import { fullJitterDelayMilliseconds, type RetryPolicy } from "../backoff";                    // reagendamento do worker
import { InvalidTransactionStateError, InvalidWagerTransactionError } from "../errors";         // estado e criação inválidos
import { FailureCode } from "./failure-code";                                                   // motivo de rejeição ou falha
import { Money } from "../money";                                                               // valor exato
import { LedgerDirection } from "../wallet/wallet-ledger-entry";                                // débito ou crédito

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
  private constructor(
    public readonly id: string,
    public readonly providerId: string | undefined,
    public readonly externalTransactionId: string | undefined,
    public readonly idempotencyKey: string,
    public readonly payloadHash: string,
    public readonly walletId: string,
    public readonly playerId: string,
    public readonly roundId: string | undefined,
    public readonly gameId: string | undefined,
    public readonly kind: WagerTransactionKind,
    public readonly money: Money,
    public readonly referenceExternalTransactionId: string | undefined,                         // id no provedor, não o id interno
    public readonly correlationId: string,
    public readonly createdAt: Date,
    private _status: WagerTransactionStatus,
    private _referenceTransactionId: string | undefined,
    private _failureCode: FailureCode | undefined,
    private _processedAt: Date | undefined,
    private _balanceAfter: Money | undefined,                                                   // saldo observado na decisão
    private _attempts: number,                                                                  // tentativas do worker sem a referência
    private _nextAttemptAt: Date | undefined,
    private _updatedAt: Date,
  ) {}

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

    return new WagerTransaction(
      props.id, props.providerId, props.externalTransactionId, props.idempotencyKey, props.payloadHash, props.walletId, props.playerId,
      props.roundId, props.gameId, props.kind, props.money, props.referenceExternalTransactionId, props.correlationId, props.createdAt,
      WagerTransactionStatus.Pending, undefined, undefined, undefined, undefined, 0, undefined, props.createdAt,   // nasce em PENDING, sem decisão
    );
  }

  static createOpening(props: CreateOpeningTransactionProps): WagerTransaction {
    if (!props.money.isPositive()) throw new InvalidWagerTransactionError("OPENING só existe com saldo inicial maior que zero");
    const idempotencyKey = `opening:${props.walletId}`;                                         // também impede dois OPENING na mesma wallet
    return new WagerTransaction(
      props.id, undefined, undefined, idempotencyKey, props.payloadHash, props.walletId, props.playerId,   // não vem de provedor (interpretação I8)
      undefined, undefined, WagerTransactionKind.Opening, props.money, undefined, props.correlationId, props.createdAt,
      WagerTransactionStatus.Pending, undefined, undefined, undefined, undefined, 0, undefined, props.createdAt,
    );
  }

  static rehydrate(state: WagerTransactionState): WagerTransaction {
    return new WagerTransaction(                                                                // só reconstrói, não revalida transições
      state.id, state.providerId, state.externalTransactionId, state.idempotencyKey, state.payloadHash, state.walletId, state.playerId,
      state.roundId, state.gameId, state.kind, state.money, state.referenceExternalTransactionId, state.correlationId, state.createdAt,
      state.status, state.referenceTransactionId, state.failureCode, state.processedAt, state.balanceAfter, state.attempts, state.nextAttemptAt, state.updatedAt,
    );
  }

  // ----------------------------------------------------------------------------------------------------
  //                                  LEITURA DO ESTADO DA DECISÃO
  // ----------------------------------------------------------------------------------------------------
  get status(): WagerTransactionStatus { return this._status; }
  get referenceTransactionId(): string | undefined { return this._referenceTransactionId; }
  get failureCode(): FailureCode | undefined { return this._failureCode; }
  get processedAt(): Date | undefined { return this._processedAt; }
  get balanceAfter(): Money | undefined { return this._balanceAfter; }
  get attempts(): number { return this._attempts; }
  get nextAttemptAt(): Date | undefined { return this._nextAttemptAt; }
  get updatedAt(): Date { return this._updatedAt; }

  toState(): WagerTransactionState {
    return {                                                                                    // cópia: quem lê não muda o estado por fora
      id: this.id, providerId: this.providerId, externalTransactionId: this.externalTransactionId, idempotencyKey: this.idempotencyKey, payloadHash: this.payloadHash,
      walletId: this.walletId, playerId: this.playerId, roundId: this.roundId, gameId: this.gameId, kind: this.kind, money: this.money,
      referenceExternalTransactionId: this.referenceExternalTransactionId, referenceTransactionId: this._referenceTransactionId, status: this._status,
      failureCode: this._failureCode, balanceAfter: this._balanceAfter, correlationId: this.correlationId, attempts: this._attempts,
      nextAttemptAt: this._nextAttemptAt, createdAt: this.createdAt, updatedAt: this._updatedAt, processedAt: this._processedAt,
    };
  }

  // ----------------------------------------------------------------------------------------------------
  //                                         TRANSIÇÕES
  // ----------------------------------------------------------------------------------------------------
  markProcessed(referenceTransactionId: string | undefined, observedBalance: Money, at: Date): void {
    this.assertNotTerminal("markProcessed");
    this._status = WagerTransactionStatus.Processed;
    this._referenceTransactionId = referenceTransactionId;
    this._balanceAfter = observedBalance;
    this._processedAt = at;
    this._nextAttemptAt = undefined;
    this._updatedAt = at;
  }

  markPendingReference(observedBalance: Money, now: Date, retryPolicy: RetryPolicy): void {
    if (this._status !== WagerTransactionStatus.Pending) {
      throw new InvalidTransactionStateError(`markPendingReference só sai de PENDING, a transação ${this.id} está ${this.status}`);
    }
    this._status = WagerTransactionStatus.PendingReference;
    this._balanceAfter = observedBalance;
    this._attempts = 0;
    this._nextAttemptAt = new Date(now.getTime() + fullJitterDelayMilliseconds(0, retryPolicy));
    this._updatedAt = now;
  }

  reject(code: FailureCode, observedBalance: Money, at: Date): void {
    this.assertNotTerminal("reject");
    this._status = WagerTransactionStatus.Rejected;
    this._failureCode = code;
    this._balanceAfter = observedBalance;
    this._nextAttemptAt = undefined;
    this._updatedAt = at;
  }

  fail(code: FailureCode, at: Date): void {
    if (this._status !== WagerTransactionStatus.PendingReference) {                             // PENDING -> FAILED não existe (seção 6.3 do desafio)
      throw new InvalidTransactionStateError(`fail só sai de PENDING_REFERENCE, a transação ${this.id} está ${this.status}`);
    }
    this._status = WagerTransactionStatus.Failed;
    this._failureCode = code;
    this._nextAttemptAt = undefined;
    this._updatedAt = at;
  }

  // ---- nova tentativa do worker sem referência: não é transição de estado
  scheduleReferenceRetry(now: Date, retryPolicy: RetryPolicy): void {
    if (this._status !== WagerTransactionStatus.PendingReference) {
      throw new InvalidTransactionStateError(`scheduleReferenceRetry só vale em PENDING_REFERENCE, a transação ${this.id} está ${this.status}`);
    }
    this._attempts = this._attempts + 1;
    this._nextAttemptAt = new Date(now.getTime() + fullJitterDelayMilliseconds(this._attempts, retryPolicy));
    this._updatedAt = now;
  }

  // ----------------------------------------------------------------------------------------------------
  //                                     CONSULTAS DE DOMÍNIO
  // ----------------------------------------------------------------------------------------------------
  isTerminal(): boolean {
    return TERMINAL_STATUSES.includes(this._status);
  }

  affectsBalance(): boolean {
    return this.kind !== WagerTransactionKind.Loss;                                             // LOSS só registra o resultado
  }

  requiresReference(): boolean {
    return this.kind === WagerTransactionKind.Refund || this.kind === WagerTransactionKind.Rollback;
  }

  matchesPayload(payloadHash: string): boolean {
    return this.payloadHash === payloadHash;
  }

  hasReferenceWaitExpired(now: Date, ttlMilliseconds: number): boolean {
    return now.getTime() - this.createdAt.getTime() >= ttlMilliseconds;                         // TTL contado do created_at da pendente
  }

  ledgerDirectionFor(reference?: WagerTransaction): LedgerDirection {
    switch (this.kind) {
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
