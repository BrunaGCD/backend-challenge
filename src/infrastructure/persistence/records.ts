// ====================================================================================================
//                     REGISTROS DE PERSISTÊNCIA (MIKRORM, DEFINEENTITY, SEM DECORATORS)
// ====================================================================================================
// O formato das linhas do banco. As classes de domínio não são mapeadas no ORM: os repositórios
// convertem registro em objeto de domínio pela factory rehydrate. O schema real vem das migrations.
import { defineEntity, p } from "@mikro-orm/postgresql";                                       // construtor de registros

export const WalletRecord = defineEntity({
  name: "WalletRecord",
  tableName: "wallets",
  properties: {
    id: p.uuid().primary(),
    playerId: p.uuid(),
    currency: p.string().length(3),
    balance: p.decimal().precision(18).scale(2),                                                // volta como string, vai direto para Money.from
    version: p.integer(),
    createdAt: p.datetime(),
    updatedAt: p.datetime(),
  },
});

export const WagerTransactionRecord = defineEntity({
  name: "WagerTransactionRecord",
  tableName: "wager_transactions",
  properties: {
    id: p.uuid().primary(),
    providerId: p.string().length(128).nullable(),
    externalTransactionId: p.string().length(128).nullable(),
    idempotencyKey: p.string().length(255),
    payloadHash: p.string().length(64),
    walletId: p.uuid(),
    playerId: p.uuid(),
    roundId: p.string().length(128).nullable(),
    gameId: p.string().length(128).nullable(),
    kind: p.string().length(16),
    amount: p.decimal().precision(18).scale(2),
    currency: p.string().length(3),
    referenceExternalTransactionId: p.string().length(128).nullable(),
    referenceTransactionId: p.uuid().nullable(),
    status: p.string().length(24),
    failureCode: p.string().length(64).nullable(),
    balanceAfter: p.decimal().precision(18).scale(2).nullable(),
    balanceCurrency: p.string().length(3).nullable(),
    correlationId: p.string().length(128),
    attempts: p.integer(),
    nextAttemptAt: p.datetime().nullable(),
    createdAt: p.datetime(),
    updatedAt: p.datetime(),
    processedAt: p.datetime().nullable(),
  },
});

export const WalletLedgerEntryRecord = defineEntity({
  name: "WalletLedgerEntryRecord",
  tableName: "wallet_ledger_entries",
  properties: {
    id: p.uuid().primary(),
    walletId: p.uuid(),
    transactionId: p.uuid(),
    direction: p.string().length(6),
    amount: p.decimal().precision(18).scale(2),
    currency: p.string().length(3),
    balanceBefore: p.decimal().precision(18).scale(2),
    balanceAfter: p.decimal().precision(18).scale(2),
    walletVersion: p.integer(),
    createdAt: p.datetime(),
  },
});

export const InboxMessageRecord = defineEntity({
  name: "InboxMessageRecord",
  tableName: "inbox_messages",
  properties: {
    consumerName: p.string().length(128).primary(),
    messageId: p.string().length(128).primary(),
    payloadHash: p.string().length(64),
    receivedAt: p.datetime(),
    processedAt: p.datetime().nullable(),
  },
});

export const OutboxMessageRecord = defineEntity({
  name: "OutboxMessageRecord",
  tableName: "outbox_messages",
  properties: {
    id: p.uuid().primary(),
    aggregateId: p.uuid(),
    eventType: p.string().length(64),
    payload: p.json<Record<string, unknown>>(),
    occurredAt: p.datetime(),
    attempts: p.integer(),
    nextAttemptAt: p.datetime(),
    publishedAt: p.datetime().nullable(),
    lastError: p.text().nullable(),
  },
});

export const ALL_RECORDS = [WalletRecord, WagerTransactionRecord, WalletLedgerEntryRecord, InboxMessageRecord, OutboxMessageRecord];
