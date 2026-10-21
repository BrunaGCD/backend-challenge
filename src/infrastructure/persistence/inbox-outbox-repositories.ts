// ====================================================================================================
//                                REPOSITÓRIOS DA INBOX E DA OUTBOX
// ====================================================================================================
import type { InferEntity } from "@mikro-orm/postgresql";                                    // tipo da linha a partir do registro
import { LockMode } from "@mikro-orm/postgresql";                                              // SKIP LOCKED dos publishers
import type { InboxRepository, OutboxRepository, OutboxStats } from "../../application/ports";
import { InboxMessage } from "../../domain/inbox-message";
import { OutboxMessage } from "../../domain/outbox-message";
import type { MikroOrmDatabase } from "./database";
import { InboxMessageRecord, OutboxMessageRecord } from "./records";

type OutboxRow = InferEntity<typeof OutboxMessageRecord>;

function outboxToDomain(row: OutboxRow): OutboxMessage {
  return OutboxMessage.rehydrate({
    id: row.id,
    aggregateId: row.aggregateId,
    eventType: row.eventType,
    payload: row.payload,
    occurredAt: row.occurredAt,
    attempts: row.attempts,
    nextAttemptAt: row.nextAttemptAt,
    publishedAt: row.publishedAt ?? undefined,
    lastError: row.lastError ?? undefined,
  });
}

export class MikroOrmInboxRepository implements InboxRepository {
  constructor(private readonly database: MikroOrmDatabase) {}

  async insertIfAbsent(message: InboxMessage): Promise<boolean> {
    const rows = await this.database.entityManager().execute<{ message_id: string }[]>(
      `insert into inbox_messages (consumer_name, message_id, payload_hash, received_at, processed_at)
       values (?, ?, ?, ?, ?)
       on conflict (consumer_name, message_id) do nothing
       returning message_id`,
      [message.consumerName, message.messageId, message.payloadHash, message.receivedAt, message.processedAt ?? null],
    );
    return rows.length === 1;                                                                   // 0 linhas: já existia
  }

  async find(consumerName: string, messageId: string): Promise<InboxMessage | undefined> {
    const row = await this.database.entityManager().findOne(InboxMessageRecord, { consumerName, messageId });
    if (!row) return undefined;
    return InboxMessage.rehydrate({ consumerName: row.consumerName, messageId: row.messageId, payloadHash: row.payloadHash, receivedAt: row.receivedAt, processedAt: row.processedAt ?? undefined });
  }
}

export class MikroOrmOutboxRepository implements OutboxRepository {
  constructor(private readonly database: MikroOrmDatabase) {}

  async insert(messages: OutboxMessage[]): Promise<void> {
    if (messages.length === 0) return;
    await this.database.entityManager().insertMany(OutboxMessageRecord, messages.map((message) => ({
      id: message.id,
      aggregateId: message.aggregateId,
      eventType: message.eventType,
      payload: message.payload as Record<string, unknown>,
      occurredAt: message.occurredAt,
      attempts: message.attempts,
      nextAttemptAt: message.nextAttemptAt ?? message.occurredAt,
      publishedAt: message.publishedAt ?? null,
      lastError: message.lastError ?? null,
    })));
  }

  async lockDueBatch(now: Date, limit: number): Promise<OutboxMessage[]> {
    const rows = await this.database.entityManager().find(
      OutboxMessageRecord,
      { publishedAt: null, nextAttemptAt: { $lte: now } },
      { orderBy: { nextAttemptAt: "asc" }, limit, lockMode: LockMode.PESSIMISTIC_PARTIAL_WRITE },   // dois publishers nunca pegam a mesma linha
    );
    return rows.map(outboxToDomain);
  }

  async saveAttempt(message: OutboxMessage): Promise<void> {
    await this.database.entityManager().nativeUpdate(
      OutboxMessageRecord,
      { id: message.id },
      { attempts: message.attempts, nextAttemptAt: message.nextAttemptAt ?? message.occurredAt, publishedAt: message.publishedAt ?? null, lastError: message.lastError ?? null },
    );
  }

  async stats(stuckAfterAttempts: number): Promise<OutboxStats> {
    const entityManager = this.database.entityManager();
    const pending = await entityManager.count(OutboxMessageRecord, { publishedAt: null });
    const stuck = await entityManager.count(OutboxMessageRecord, { publishedAt: null, attempts: { $gte: stuckAfterAttempts } });
    const oldest = await entityManager.findOne(OutboxMessageRecord, { publishedAt: null }, { orderBy: { occurredAt: "asc" } });
    return { pending, stuck, oldestOccurredAt: oldest?.occurredAt };
  }
}
