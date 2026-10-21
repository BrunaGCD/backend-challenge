// ====================================================================================================
//                                PORTAS QUE A APLICAÇÃO USA
// ====================================================================================================
// A aplicação só conhece estas interfaces; as implementações (MikroORM, SQS, relógio do sistema)
// ficam na infraestrutura e entram pela composição. É por aqui que os testes injetam falhas.
import type { InboxMessage } from "../domain/inbox-message";
import type { Money } from "../domain/money";
import type { OutboxMessage } from "../domain/outbox-message";
import type { Wallet } from "../domain/wallet";
import type { WalletLedgerEntry } from "../domain/wallet-ledger-entry";
import type { WagerTransaction } from "../domain/wager-transaction";

export interface Clock {
  now(): Date;
}

export interface IdGenerator {
  newId(): string;                                                                              // UUIDv7
}

export interface RandomSource {
  next(): number;                                                                               // número em [0, 1)
}

// Uma transação SQL por execução de caso de uso. Dentro de work, os repositórios usam o
// EntityManager da transação, que chega pelo contexto.
export interface TransactionRunner {
  run<T>(work: () => Promise<T>): Promise<T>;
}

export interface WalletRepository {
  findById(walletId: string): Promise<Wallet | undefined>;
  findByIdForUpdate(walletId: string): Promise<Wallet | undefined>;                             // SELECT ... FOR UPDATE
  findByPlayerAndCurrency(playerId: string, currency: string): Promise<Wallet | undefined>;
  insert(wallet: Wallet): Promise<void>;
  updateBalance(wallet: Wallet, expectedVersion: number): Promise<void>;                        // WHERE version = expectedVersion
}

export interface WagerTransactionRepository {
  findById(transactionId: string): Promise<WagerTransaction | undefined>;
  findByIdForUpdate(transactionId: string): Promise<WagerTransaction | undefined>;
  findByIdempotencyKey(idempotencyKey: string): Promise<WagerTransaction | undefined>;
  findByProviderAndExternalId(providerId: string, externalTransactionId: string): Promise<WagerTransaction | undefined>;
  hasProcessedReversal(referenceTransactionId: string): Promise<boolean>;
  lockNextDuePending(now: Date): Promise<WagerTransaction | undefined>;                         // FOR UPDATE SKIP LOCKED
  countPending(): Promise<number>;
  insert(transaction: WagerTransaction): Promise<void>;
  updateDecision(transaction: WagerTransaction): Promise<void>;                                 // só sai de PENDING_REFERENCE
}

export interface ReconciliationRow {
  storedBalance: Money;
  totalCredits: Money;
  totalDebits: Money;
  checkedEntries: number;
}

export interface LedgerRepository {
  insert(entry: WalletLedgerEntry): Promise<void>;
  listByWallet(walletId: string, afterWalletVersion: number, limit: number): Promise<WalletLedgerEntry[]>;
  reconcile(walletId: string): Promise<ReconciliationRow | undefined>;                          // uma consulta, um snapshot
}

export interface InboxRepository {
  insertIfAbsent(message: InboxMessage): Promise<boolean>;                                      // ON CONFLICT DO NOTHING
  find(consumerName: string, messageId: string): Promise<InboxMessage | undefined>;
}

export interface OutboxStats {
  pending: number;
  stuck: number;
  oldestOccurredAt: Date | undefined;
}

export interface OutboxRepository {
  insert(messages: OutboxMessage[]): Promise<void>;
  lockDueBatch(now: Date, limit: number): Promise<OutboxMessage[]>;                             // FOR UPDATE SKIP LOCKED
  saveAttempt(message: OutboxMessage): Promise<void>;
  stats(stuckAfterAttempts: number): Promise<OutboxStats>;
}

export interface PublishOutcome {
  publishedIds: string[];
  failures: { id: string; error: string }[];
}

export interface EventPublisher {
  publishBatch(messages: OutboxMessage[]): Promise<PublishOutcome>;                             // SendMessageBatch
}

export interface Repositories {
  wallets: WalletRepository;
  transactions: WagerTransactionRepository;
  ledger: LedgerRepository;
  inbox: InboxRepository;
  outbox: OutboxRepository;
}
