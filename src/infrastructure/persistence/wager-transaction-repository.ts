// ====================================================================================================
//                                 REPOSITÓRIO DE TRANSAÇÕES
// ====================================================================================================
import type { InferEntity } from "@mikro-orm/postgresql";                                    // tipo da linha a partir do registro
import { LockMode } from "@mikro-orm/postgresql";                                              // FOR UPDATE e SKIP LOCKED
import type { WagerTransactionRepository } from "../../application/ports";
import type { FailureCode } from "../../domain/failure-code";
import { Money } from "../../domain/money";
import { WagerTransaction, WagerTransactionKind, WagerTransactionStatus } from "../../domain/wager-transaction";
import type { MikroOrmDatabase } from "./database";
import { WagerTransactionRecord } from "./records";

type TransactionRow = InferEntity<typeof WagerTransactionRecord>;

function toDomain(row: TransactionRow): WagerTransaction {
  const hasObservedBalance = row.balanceAfter !== null && row.balanceAfter !== undefined && row.balanceCurrency !== null && row.balanceCurrency !== undefined;
  return WagerTransaction.rehydrate({
    id: row.id,
    providerId: row.providerId ?? undefined,
    externalTransactionId: row.externalTransactionId ?? undefined,
    idempotencyKey: row.idempotencyKey,
    payloadHash: row.payloadHash,
    walletId: row.walletId,
    playerId: row.playerId,
    roundId: row.roundId ?? undefined,
    gameId: row.gameId ?? undefined,
    kind: row.kind as WagerTransactionKind,
    money: Money.from({ amount: row.amount, currency: row.currency }),
    referenceExternalTransactionId: row.referenceExternalTransactionId ?? undefined,
    referenceTransactionId: row.referenceTransactionId ?? undefined,
    status: row.status as WagerTransactionStatus,
    failureCode: (row.failureCode ?? undefined) as FailureCode | undefined,
    balanceAfter: hasObservedBalance ? Money.from({ amount: row.balanceAfter!, currency: row.balanceCurrency! }) : undefined,
    correlationId: row.correlationId,
    attempts: row.attempts,
    nextAttemptAt: row.nextAttemptAt ?? undefined,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
    processedAt: row.processedAt ?? undefined,
  });
}

function decisionColumns(transaction: WagerTransaction) {
  return {
    status: transaction.status,
    referenceTransactionId: transaction.referenceTransactionId ?? null,
    failureCode: transaction.failureCode ?? null,
    balanceAfter: transaction.balanceAfter?.toString() ?? null,
    balanceCurrency: transaction.balanceAfter?.currency ?? null,
    attempts: transaction.attempts,
    nextAttemptAt: transaction.nextAttemptAt ?? null,
    updatedAt: transaction.updatedAt,
    processedAt: transaction.processedAt ?? null,
  };
}

export class MikroOrmWagerTransactionRepository implements WagerTransactionRepository {
  constructor(private readonly database: MikroOrmDatabase) {}

  async findById(transactionId: string): Promise<WagerTransaction | undefined> {
    const row = await this.database.entityManager().findOne(WagerTransactionRecord, { id: transactionId });
    return row ? toDomain(row) : undefined;
  }

  async findByIdForUpdate(transactionId: string): Promise<WagerTransaction | undefined> {
    const row = await this.database.entityManager().findOne(WagerTransactionRecord, { id: transactionId }, { lockMode: LockMode.PESSIMISTIC_WRITE });
    return row ? toDomain(row) : undefined;
  }

  async findByIdempotencyKey(idempotencyKey: string): Promise<WagerTransaction | undefined> {
    const row = await this.database.entityManager().findOne(WagerTransactionRecord, { idempotencyKey });
    return row ? toDomain(row) : undefined;
  }

  async findByProviderAndExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | undefined> {
    const row = await this.database.entityManager().findOne(WagerTransactionRecord, { providerId, externalTransactionId });   // usa o UNIQUE (provider_id, external_transaction_id)
    return row ? toDomain(row) : undefined;
  }

  async hasProcessedReversal(referenceTransactionId: string): Promise<boolean> {
    const count = await this.database.entityManager().count(WagerTransactionRecord, {
      referenceTransactionId,
      kind: { $in: [WagerTransactionKind.Refund, WagerTransactionKind.Rollback] },
      status: WagerTransactionStatus.Processed,
    });
    return count > 0;
  }

  async lockNextDuePending(now: Date): Promise<WagerTransaction | undefined> {
    const rows = await this.database.entityManager().find(
      WagerTransactionRecord,
      { status: WagerTransactionStatus.PendingReference, nextAttemptAt: { $lte: now } },
      { orderBy: { nextAttemptAt: "asc" }, limit: 1, lockMode: LockMode.PESSIMISTIC_PARTIAL_WRITE },   // FOR UPDATE SKIP LOCKED: duas instâncias nunca pegam a mesma
    );
    const row = rows[0];
    return row ? toDomain(row) : undefined;
  }

  async countPending(): Promise<number> {
    return this.database.entityManager().count(WagerTransactionRecord, { status: WagerTransactionStatus.PendingReference });
  }

  async insert(transaction: WagerTransaction): Promise<void> {
    await this.database.entityManager().insert(WagerTransactionRecord, {
      id: transaction.id,
      providerId: transaction.providerId ?? null,
      externalTransactionId: transaction.externalTransactionId ?? null,
      idempotencyKey: transaction.idempotencyKey,
      payloadHash: transaction.payloadHash,
      walletId: transaction.walletId,
      playerId: transaction.playerId,
      roundId: transaction.roundId ?? null,
      gameId: transaction.gameId ?? null,
      kind: transaction.kind,
      amount: transaction.money.toString(),
      currency: transaction.money.currency,
      referenceExternalTransactionId: transaction.referenceExternalTransactionId ?? null,
      correlationId: transaction.correlationId,
      createdAt: transaction.createdAt,
      ...decisionColumns(transaction),
    });
  }

  async updateDecision(transaction: WagerTransaction): Promise<void> {
    const affectedRows = await this.database.entityManager().nativeUpdate(
      WagerTransactionRecord,
      { id: transaction.id, status: WagerTransactionStatus.PendingReference },                  // só uma pendente muda; terminal o trigger recusa
      decisionColumns(transaction),
    );
    if (affectedRows !== 1) throw new Error(`UPDATE da pendente ${transaction.id} afetou ${affectedRows} linhas`);
  }
}
