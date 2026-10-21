// ====================================================================================================
//                     PUBLISHER DA OUTBOX (SEÇÃO 11): UM CICLO, EM TODAS AS INSTÂNCIAS
// ====================================================================================================
// BEGIN; até 10 linhas vencidas com FOR UPDATE SKIP LOCKED; SendMessageBatch; marca publicadas ou
// reagenda as recusadas; COMMIT. A única duplicação possível é publicar e morrer antes do COMMIT:
// a cópia tem o mesmo eventId, o SQS descarta dentro de 5 minutos e o consumidor deduplica por eventId.
import type { RetryPolicy } from "../domain/backoff";
import { logger } from "../observability/logger";
import { outboxPublishDelaySeconds, retriesTotal } from "../observability/metrics";
import type { Clock, EventPublisher, OutboxRepository, TransactionRunner } from "./ports";

export interface OutboxPublisherSettings {
  batchSize: number;
  retryPolicy: RetryPolicy;
  stuckAfterAttempts: number;
}

export class OutboxPublisher {
  constructor(
    private readonly runner: TransactionRunner,
    private readonly outbox: OutboxRepository,
    private readonly publisher: EventPublisher,
    private readonly clock: Clock,
    private readonly settings: OutboxPublisherSettings,
  ) {}

  // devolve quantas linhas pegou: lote cheio quer dizer que pode ter mais esperando
  async publishOnce(): Promise<number> {
    return this.runner.run(async () => {
      const batch = await this.outbox.lockDueBatch(this.clock.now(), this.settings.batchSize);
      if (batch.length === 0) return 0;

      const outcome = await this.publisher.publishBatch(batch);                                 // com a transação aberta: o timeout do SQS fica abaixo do idle_in_transaction
      const publishedAt = this.clock.now();
      const failureById = new Map(outcome.failures.map((failure) => [failure.id, failure.error]));

      for (const message of batch) {
        if (outcome.publishedIds.includes(message.id)) {
          message.markPublished(publishedAt);
          outboxPublishDelaySeconds.observe((publishedAt.getTime() - message.occurredAt.getTime()) / 1000);
          logger.info({ event: "outbox_event_published", outboxMessageId: message.id, eventType: message.eventType, walletId: message.aggregateId }, "evento publicado");   // uma linha por publicação aceita: é como se conta cópia
        } else {
          const error = failureById.get(message.id) ?? "sem resposta para esta mensagem no lote";
          message.scheduleRetry(publishedAt, this.settings.retryPolicy, error);
          retriesTotal.inc({ component: "outbox" });
          const logFields = { event: "outbox_publish_failed", outboxMessageId: message.id, eventType: message.eventType, attempt: message.attempts, error };
          if (message.attempts >= this.settings.stuckAfterAttempts) logger.error(logFields, "evento da outbox continua sem publicar depois de muitas tentativas");
          else logger.warn(logFields, "falha ao publicar evento da outbox; nova tentativa agendada");
        }
        await this.outbox.saveAttempt(message);
      }
      return batch.length;
    });
  }
}
