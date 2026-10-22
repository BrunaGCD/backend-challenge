// ====================================================================================================
//                          OUTBOX: EVENTO GRAVADO JUNTO COM O EFEITO
// ====================================================================================================
// O id é o eventId. Sem limite de tentativas: descartar um evento quebraria "não perder eventos
// confirmados"; depois de muitas tentativas o publisher só avisa (log e métrica) e continua tentando.
import { fullJitterDelayMilliseconds, type RetryPolicy } from "../backoff";                    // espera entre tentativas
import type { IntegrationEvent } from "../events";                                              // evento a publicar

export interface OutboxMessageState {
  id: string;
  aggregateId: string;
  eventType: string;
  payload: Readonly<Record<string, unknown>>;
  occurredAt: Date;
  attempts: number;
  nextAttemptAt: Date | undefined;
  publishedAt: Date | undefined;
  lastError: string | undefined;
}

export class OutboxMessage {
  private constructor(
    public readonly id: string,
    public readonly aggregateId: string,
    public readonly eventType: string,
    public readonly payload: Readonly<Record<string, unknown>>,
    public readonly occurredAt: Date,
    private _attempts: number,
    private _nextAttemptAt?: Date,
    private _publishedAt?: Date,
    private _lastError?: string,
  ) {}

  static enqueue(event: IntegrationEvent<unknown>): OutboxMessage {
    const payload = event.toJSON() as unknown as Record<string, unknown>;                       // envelope serializado
    return new OutboxMessage(event.eventId, event.aggregateId, event.eventType, payload, event.occurredAt, 0, event.occurredAt);
  }

  static rehydrate(state: OutboxMessageState): OutboxMessage {
    return new OutboxMessage(state.id, state.aggregateId, state.eventType, state.payload, state.occurredAt, state.attempts, state.nextAttemptAt, state.publishedAt, state.lastError);
  }

  get attempts(): number {
    return this._attempts;
  }

  get nextAttemptAt(): Date | undefined {
    return this._nextAttemptAt;
  }

  get publishedAt(): Date | undefined {
    return this._publishedAt;
  }

  get lastError(): string | undefined {
    return this._lastError;
  }

  isPending(): boolean {
    return this._publishedAt === undefined;
  }

  isDue(now: Date): boolean {
    return this.isPending() && (this._nextAttemptAt === undefined || this._nextAttemptAt.getTime() <= now.getTime());
  }

  markPublished(at: Date): void {
    this._publishedAt = at;
  }

  // incrementa attempts e calcula o próximo nextAttemptAt (backoff)
  scheduleRetry(now: Date, retryPolicy: RetryPolicy, error: string): void {
    this._attempts = this._attempts + 1;
    this._nextAttemptAt = new Date(now.getTime() + fullJitterDelayMilliseconds(this._attempts, retryPolicy));
    this._lastError = error;
  }
}
