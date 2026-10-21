// ====================================================================================================
//                       CONSUMIDOR SQS DA FILA DE ENTRADA (SEÇÃO 10)
// ====================================================================================================
// Usa o mesmo caso de uso do HTTP. Ack (DeleteMessage) só depois do commit. Classificação:
//   sucesso, replay, mensagem já na inbox, rejeição de negócio -> DeleteMessage
//   transitório ou inesperado (nada foi gravado)               -> ChangeMessageVisibility com backoff
//   permanente (mensagem inválida, wallet inexistente, conflito) -> DLQ explícita e DeleteMessage
import { ApplicationError, ExternalTransactionIdConflictError, IdempotencyKeyConflictError, InboxPayloadConflictError, ValidationError, WalletNotFoundError, type FieldError } from "../../application/errors";
import type { ProcessWagerTransaction } from "../../application/process-wager-transaction";
import { isPlainObject, isValidIdempotencyKey, parseWagerCommand, rejectUnknownFields } from "../../application/wager-command";
import { fullJitterDelayMilliseconds, type RetryPolicy } from "../../domain/backoff";          // backoff comum
import { runWithLogContext } from "../../observability/log-context";
import { errorSummary, logger } from "../../observability/logger";
import { dlqForwardedTotal, inboxPayloadConflictsTotal, retriesTotal } from "../../observability/metrics";
import type { InputQueue, ReceivedMessage } from "./sqs";

export interface ConsumerSettings {
  consumerName: string;
  acceptedCurrencies: string[];
  waitTimeSeconds: number;
  retryPolicy: RetryPolicy;
  shutdownTimeoutMilliseconds: number;
  visibilityTimeoutMilliseconds: number;                                                        // o prazo de cada recebimento, o mesmo da fila
  longestMessageMilliseconds: number;                                                           // o statement_timeout da fila: o máximo que uma mensagem espera no banco
}

type HandleOutcome = "acknowledged" | "sent_to_dead_letter" | "retry_scheduled";

const MESSAGE_TYPE = "WagerTransactionRequested";
const ENVELOPE_FIELDS = ["messageId", "type", "occurredAt", "data"];
const MESSAGE_ID_PATTERN = /^[\x21-\x7E]{1,128}$/;
const CORRELATION_ID_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;
const MAX_MESSAGES_PER_RECEIVE = 10;

class PermanentMessageError extends Error {
  constructor(public readonly reason: string, message: string) {
    super(message);
  }
}

interface ParsedMessage {
  messageId: string;
  idempotencyKey: string;
  command: NonNullable<Extract<ReturnType<typeof parseWagerCommand>, { ok: true }>["value"]>;
}

export class SqsConsumer {
  private running = false;
  private stopping = false;
  private loopFinished: Promise<void> = Promise.resolve();
  private receiveAbort = new AbortController();

  constructor(
    private readonly queue: InputQueue,
    private readonly processWager: ProcessWagerTransaction,
    private readonly settings: ConsumerSettings,
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loopFinished = this.loop();
  }

  // SIGTERM: para de receber, devolve o que não começou e conclui o que está em andamento (com limite)
  async stop(): Promise<void> {
    if (!this.running) return;
    this.stopping = true;
    this.receiveAbort.abort();                                                                  // cancela o long polling em andamento
    const timedOut = Symbol("timeout");
    const finished = await Promise.race([this.loopFinished.then(() => "done"), Bun.sleep(this.settings.shutdownTimeoutMilliseconds).then(() => timedOut)]);
    if (finished === timedOut) logger.warn({ event: "consumer_shutdown_timeout" }, "consumidor não terminou dentro do limite; o que sobrou volta pela visibilidade");
    this.running = false;
  }

  private async loop(): Promise<void> {
    let consecutiveReceiveFailures = 0;
    while (!this.stopping) {
      let messages: ReceivedMessage[] = [];
      let receivedAt = 0;
      try {
        messages = await this.queue.receive(MAX_MESSAGES_PER_RECEIVE, this.settings.waitTimeSeconds, this.receiveAbort.signal);
        receivedAt = performance.now();                                                         // o prazo de visibilidade começa a contar aqui
        consecutiveReceiveFailures = 0;
      } catch (error) {
        if (this.stopping) break;                                                               // abortado pelo desligamento
        consecutiveReceiveFailures = consecutiveReceiveFailures + 1;
        const waitMilliseconds = fullJitterDelayMilliseconds(Math.min(consecutiveReceiveFailures, 5), this.settings.retryPolicy);
        logger.warn({ event: "sqs_receive_failed", attempt: consecutiveReceiveFailures, ...errorSummary(error) }, "falha ao receber da fila; tentando de novo");
        await Bun.sleep(waitMilliseconds);
        continue;
      }
      await this.processBatch(messages, receivedAt);
    }
  }

  // grupos diferentes em paralelo; dentro do grupo, em ordem
  private async processBatch(messages: ReceivedMessage[], receivedAt: number): Promise<void> {
    const messagesByGroup = new Map<string, ReceivedMessage[]>();
    for (const message of messages) {
      const group = messagesByGroup.get(message.groupId) ?? [];
      group.push(message);
      messagesByGroup.set(message.groupId, group);
    }
    await Promise.all([...messagesByGroup.values()].map((group) => this.processGroup(group, receivedAt)));
  }

  private async processGroup(group: ReceivedMessage[], receivedAt: number): Promise<void> {
    for (let index = 0; index < group.length; index++) {
      const message = group[index]!;
      const remaining = group.slice(index + 1);
      if (this.stopping) {
        await this.releaseVisibility([message, ...remaining]);                                  // recebida e não iniciada: volta na hora
        return;
      }
      if (!this.fitsInVisibility(receivedAt)) {
        logger.info({ event: "sqs_visibility_running_out", released: 1 + remaining.length }, "o prazo de visibilidade não cabe mais uma espera inteira; as mensagens que faltam do grupo voltam para a fila");
        await this.releaseVisibility([message, ...remaining]);                                  // voltam na hora, em vez de vencer no meio do processamento
        return;
      }
      const outcome = await this.handleSafely(message);
      if (outcome === "retry_scheduled") {
        await this.releaseVisibility(remaining);                                                // as seguintes do grupo esperam a da frente
        return;
      }
    }
  }

  // A visibilidade é contada a partir do recebimento, e as mensagens de uma wallet vão uma por vez. Se as
  // primeiras demoraram esperando o lock, a próxima pode não caber no que sobrou: começar mesmo assim
  // deixaria a mensagem voltar para a fila no meio, outra instância pegá-la, e o recebimento contar à toa.
  private fitsInVisibility(receivedAt: number): boolean {
    const elapsedMilliseconds = performance.now() - receivedAt;                                 // quanto do prazo já foi gasto
    return elapsedMilliseconds + this.settings.longestMessageMilliseconds <= this.settings.visibilityTimeoutMilliseconds;   // cabe mais uma espera inteira?
  }

  private async releaseVisibility(messages: ReceivedMessage[]): Promise<void> {
    for (const message of messages) {
      try {
        await this.queue.changeVisibility(message, 0);
      } catch (error) {
        logger.warn({ event: "sqs_release_failed", ...errorSummary(error) }, "não deu para devolver a visibilidade; a mensagem volta quando o timeout vencer");
      }
    }
  }

  private async handleSafely(message: ReceivedMessage): Promise<HandleOutcome> {
    return runWithLogContext({ messageId: message.sqsMessageId }, async () => {
      try {
        return await this.handle(message);
      } catch (error) {
        logger.error({ event: "sqs_message_handling_failed", ...errorSummary(error) }, "falha ao tratar a mensagem; ela volta pela visibilidade");
        return "retry_scheduled";
      }
    });
  }

  private async handle(message: ReceivedMessage): Promise<HandleOutcome> {
    let parsed: ParsedMessage;
    try {
      parsed = this.parseMessage(message.body);
    } catch (error) {
      if (!(error instanceof PermanentMessageError)) throw error;
      return this.sendToDeadLetter(message, error.reason, error.message);
    }

    return runWithLogContext({ messageId: parsed.messageId }, async () => {
      const correlationId = message.correlationIdAttribute && CORRELATION_ID_PATTERN.test(message.correlationIdAttribute) ? message.correlationIdAttribute : parsed.messageId;
      try {
        await this.processWager.execute(parsed.command, parsed.idempotencyKey, { channel: "sqs", correlationId, causationId: parsed.messageId, inbox: { consumerName: this.settings.consumerName, messageId: parsed.messageId } });
      } catch (error) {
        if (this.isPermanent(error)) {
          if (error instanceof InboxPayloadConflictError) inboxPayloadConflictsTotal.inc();
          return this.sendToDeadLetter(message, (error as ApplicationError).code, (error as Error).message);
        }
        return this.scheduleRetry(message, error);
      }
      await this.queue.delete(message);                                                         // ack só depois do commit
      return "acknowledged";
    });
  }

  private isPermanent(error: unknown): boolean {
    return error instanceof ValidationError || error instanceof WalletNotFoundError || error instanceof IdempotencyKeyConflictError || error instanceof ExternalTransactionIdConflictError || error instanceof InboxPayloadConflictError;
  }

  private async sendToDeadLetter(message: ReceivedMessage, reason: string, detail: string): Promise<HandleOutcome> {
    await this.queue.sendToDeadLetter(message, reason);                                         // não é atômico com o delete; a dedup da DLQ segura a cópia
    await this.queue.delete(message);
    dlqForwardedTotal.inc({ reason });
    logger.warn({ event: "sqs_message_sent_to_dlq", reason, detail }, "mensagem com erro permanente enviada para a DLQ");
    return "sent_to_dead_letter";
  }

  private async scheduleRetry(message: ReceivedMessage, error: unknown): Promise<HandleOutcome> {
    const delayMilliseconds = fullJitterDelayMilliseconds(message.receiveCount - 1, this.settings.retryPolicy);
    const delaySeconds = Math.ceil(delayMilliseconds / 1000);                                   // a visibilidade é em segundos
    await this.queue.changeVisibility(message, delaySeconds);
    retriesTotal.inc({ component: "sqs_consumer" });
    logger.warn({ event: "sqs_message_retry_scheduled", attempt: message.receiveCount, delaySeconds, ...errorSummary(error) }, "erro transitório; a mensagem volta depois do backoff");
    return "retry_scheduled";
  }

  // ----------------------------------------------------------------------------------------------------
  //                          MENSAGEM: JSON, ENVELOPE E O MESMO VALIDADOR DO HTTP
  // ----------------------------------------------------------------------------------------------------
  private parseMessage(body: string): ParsedMessage {
    let envelope: unknown;
    try {
      envelope = JSON.parse(body);
    } catch {
      throw new PermanentMessageError("INVALID_JSON", "corpo da mensagem não é JSON");
    }
    if (!isPlainObject(envelope)) throw new PermanentMessageError("VALIDATION_ERROR", "envelope precisa ser um objeto");

    const errors: FieldError[] = [];
    rejectUnknownFields(envelope, ENVELOPE_FIELDS, "", errors);
    if (typeof envelope.messageId !== "string" || !MESSAGE_ID_PATTERN.test(envelope.messageId)) errors.push({ field: "messageId", message: "precisa ter de 1 a 128 caracteres ASCII visíveis" });
    if (typeof envelope.occurredAt !== "string" || Number.isNaN(Date.parse(envelope.occurredAt))) errors.push({ field: "occurredAt", message: "precisa ser uma data ISO-8601" });
    if (envelope.type !== MESSAGE_TYPE) throw new PermanentMessageError("UNKNOWN_TYPE", `type desconhecido: ${String(envelope.type).slice(0, 64)}`);
    if (!isPlainObject(envelope.data)) errors.push({ field: "data", message: "precisa ser um objeto" });
    if (errors.length > 0) throw new PermanentMessageError("VALIDATION_ERROR", errors.map((error) => error.field).join(", "));

    const { idempotencyKey, ...commandFields } = envelope.data as Record<string, unknown>;
    if (!isValidIdempotencyKey(idempotencyKey)) throw new PermanentMessageError("VALIDATION_ERROR", "data.idempotencyKey ausente ou inválido");
    const parsedCommand = parseWagerCommand(commandFields, this.settings.acceptedCurrencies);    // kind OPENING cai aqui: não é um tipo aceito
    if (!parsedCommand.ok) throw new PermanentMessageError("VALIDATION_ERROR", parsedCommand.errors.map((error) => `data.${error.field}`).join(", "));
    return { messageId: envelope.messageId as string, idempotencyKey, command: parsedCommand.value };
  }
}
