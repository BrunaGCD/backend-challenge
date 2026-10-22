// ====================================================================================================
//                      CONSUMIDOR SQS DA FILA DE ENTRADA (SEÇÃO 10 DO DESAFIO)
// ====================================================================================================
// Usa o mesmo caso de uso do HTTP. Ack (DeleteMessage) só depois do commit. Classificação:
//   sucesso, replay, mensagem já na inbox, rejeição de negócio  -> DeleteMessage
//   transitório ou inesperado (nada foi gravado)                -> fica com o consumidor e tenta de novo ali mesmo, até o orçamento
//   permanente (mensagem inválida, wallet inexistente, conflito) -> DLQ explícita e DeleteMessage
// O orçamento de tentativas é do consumidor, não o ApproximateReceiveCount: um recebimento só é gasto
// quando a mensagem sai das mãos dele (desligamento, processo morto), e o maxReceiveCount da fila fica
// como rede de segurança para isso. Banco fora ou saturado também pausa o recebimento de mensagens novas.
// A busca não espera os grupos em mãos terminarem, até o limite de mensagens em mãos: uma wallet presa
// tentando de novo não segura as outras. O FIFO não entrega outra mensagem de um grupo em mãos.
import { ExternalTransactionIdConflictError, IdempotencyKeyConflictError, InboxPayloadConflictError, ValidationError, WalletNotFoundError, type ApplicationError, type FieldError } from "../../application/errors";
import { isDatabaseUnavailableError } from "../../application/infrastructure-errors";            // banco fora ou sem conexão livre
import type { ProcessWagerTransaction } from "../../application/process-wager-transaction";
import { isPlainObject, isValidCorrelationId, isValidIdempotencyKey, parseWagerCommand, rejectUnknownFields, type WagerCommand } from "../../application/wager-command";
import { fullJitterDelayMilliseconds, type RetryPolicy } from "../../domain/backoff";          // backoff comum
import { runWithLogContext } from "../../observability/log-context";
import { errorSummary, logger } from "../../observability/logger";
import { consumerPausesTotal, dlqForwardedTotal, inboxPayloadConflictsTotal, retriesTotal } from "../../observability/metrics";
import { GroupVisibility } from "./group-visibility";                                         // o prazo das mensagens de uma wallet em mãos
import type { InputQueue, ReceivedMessage } from "./sqs";

export interface ConsumerSettings {
  consumerName: string;
  acceptedCurrencies: string[];
  waitTimeSeconds: number;
  retryPolicy: RetryPolicy;
  retryBudgetMilliseconds: number;                                                              // quanto tempo uma mensagem tenta de novo no consumidor antes da DLQ
  shutdownTimeoutMilliseconds: number;
  maxInFlightMessages: number;                                                                  // quantas mensagens ficam em mãos ao mesmo tempo, somando todos os grupos
  visibilityTimeoutMilliseconds: number;                                                       // o prazo de cada recebimento, o mesmo da fila
  longestMessageMilliseconds: number;                                                           // o statement_timeout da fila: a estimativa do que uma mensagem espera no banco
  isDatabaseReady: () => Promise<boolean>;                                                      // a mesma checagem do banco que a readiness faz
}

type MessageResult = "finished" | "stopped";

// onde a mensagem está: falta processar, falta confirmar (o commit já foi feito) ou falta mandar para a DLQ
type Step = { kind: "process" } | { kind: "delete" } | { kind: "dead_letter"; reason: string; detail: string };

const MESSAGE_TYPE = "WagerTransactionRequested";
const ENVELOPE_FIELDS = ["messageId", "type", "occurredAt", "data"];
const MESSAGE_ID_PATTERN = /^[\x21-\x7E]{1,128}$/;
const MAX_MESSAGES_PER_RECEIVE = 10;
const WAIT_SECONDS_WITH_GROUPS_IN_HANDS = 1;                                                    // busca curta com grupos em mãos: no desligamento ela termina sozinha, sem cancelar
const RETRY_BUDGET_EXHAUSTED = "RETRY_BUDGET_EXHAUSTED";                                        // reason na DLQ: transitório que não passou dentro do orçamento
const RECEIVE_FAILURE_MAX_EXPONENT = 5;                                                         // falhas seguidas da busca: com o padrão, 2 s × 2^5 já passa do teto de 60 s

class PermanentMessageError extends Error {
  constructor(public readonly reason: string, message: string) {
    super(message);
  }
}

interface ParsedMessage {
  messageId: string;
  idempotencyKey: string;
  command: WagerCommand;                                                                        // o mesmo comando validado do HTTP
}

export class SqsConsumer {
  private running = false;
  private stopping = false;
  private databaseUnavailable = false;                                                          // ligado por um erro de conexão ou de saturação do banco
  private pausedAt = 0;
  private loopFinished: Promise<void> = Promise.resolve();
  private receiveAbort = new AbortController();
  private signalStop: () => void = () => {};
  private readonly stopRequested = new Promise<void>((resolve) => { this.signalStop = resolve; });   // acorda quem está esperando backoff ou o banco
  private messagesInHands = 0;                                                                  // recebidas e ainda não confirmadas, mandadas para a DLQ ou devolvidas
  private readonly groupsInHands = new Set<Promise<void>>();                                    // o desligamento espera todos eles
  private wakeReceiver: () => void = () => {};                                                  // acorda a busca parada no limite quando uma mensagem sai das mãos
  private receiveInProgress: Promise<unknown> = Promise.resolve();                              // a busca aberta; no desligamento os grupos só devolvem depois que ela termina

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
    this.signalStop();
    if (this.messagesInHands === 0) this.receiveAbort.abort();                                  // sem grupo em mãos, cancela o long polling; com grupo, a busca é curta e termina sozinha
    const timedOut = Symbol("timeout");
    const finished = await Promise.race([this.loopFinished.then(() => "done"), Bun.sleep(this.settings.shutdownTimeoutMilliseconds).then(() => timedOut)]);
    if (finished === timedOut) logger.warn({ event: "consumer_shutdown_timeout" }, "consumidor não terminou dentro do limite; o que sobrou volta pela visibilidade");
    this.running = false;
  }

  private async loop(): Promise<void> {
    let consecutiveReceiveFailures = 0;
    while (!this.stopping) {
      if (this.databaseUnavailable) {
        await this.waitForDatabase();                                                           // nenhuma mensagem nova enquanto o banco não responde
        continue;
      }
      const freeSlots = this.settings.maxInFlightMessages - this.messagesInHands;               // quantas mensagens ainda cabem em mãos
      if (freeSlots <= 0) {
        await this.waitForFreeSlot();                                                           // no limite: só busca de novo quando alguma sair das mãos
        continue;
      }
      let messages: ReceivedMessage[] = [];
      let receivedAt = 0;
      const waitTimeSeconds = this.messagesInHands > 0 ? Math.min(WAIT_SECONDS_WITH_GROUPS_IN_HANDS, this.settings.waitTimeSeconds) : this.settings.waitTimeSeconds;
      try {
        const receiving = this.queue.receive(Math.min(MAX_MESSAGES_PER_RECEIVE, freeSlots), waitTimeSeconds, this.receiveAbort.signal);
        this.receiveInProgress = receiving.catch(() => undefined);                              // o erro é tratado logo abaixo
        messages = await receiving;
        receivedAt = performance.now();                                                         // o prazo de visibilidade começa a contar aqui
        consecutiveReceiveFailures = 0;
      } catch (error) {
        if (this.stopping) break;                                                               // abortado pelo desligamento
        consecutiveReceiveFailures = consecutiveReceiveFailures + 1;
        const waitMilliseconds = fullJitterDelayMilliseconds(Math.min(consecutiveReceiveFailures, RECEIVE_FAILURE_MAX_EXPONENT), this.settings.retryPolicy);
        logger.warn({ event: "sqs_receive_failed", attempt: consecutiveReceiveFailures, ...errorSummary(error) }, "falha ao receber da fila; tentando de novo");
        await this.sleepUnlessStopping(waitMilliseconds);
        continue;
      }
      this.startGroups(messages, receivedAt);                                                   // não espera: a próxima busca sai com estes grupos em mãos (desligando, eles devolvem na hora)
    }
    await Promise.all([...this.groupsInHands]);                                                 // desligando: cada grupo termina a mensagem em andamento e devolve as outras
  }

  private async waitForFreeSlot(): Promise<void> {
    const slotFreed = new Promise<void>((resolve) => { this.wakeReceiver = resolve; });
    await Promise.race([slotFreed, this.stopRequested]);
  }

  private markOutOfHands(count: number): void {
    if (count === 0) return;
    this.messagesInHands = this.messagesInHands - count;
    this.wakeReceiver();                                                                        // abriu vaga para a busca
  }

  // ----------------------------------------------------------------------------------------------------
  //                          PAUSA ENQUANTO O BANCO ESTÁ FORA OU SATURADO
  // ----------------------------------------------------------------------------------------------------
  // Receber durante a queda faria cada mensagem nova esperar o banco nas mãos de alguém, e qualquer
  // desligamento no meio gastaria um recebimento dela. Parado, o consumidor deixa as mensagens na fila.
  private pauseReceiving(error: unknown): void {
    if (this.databaseUnavailable) return;                                                       // uma pausa por queda
    this.databaseUnavailable = true;
    this.pausedAt = performance.now();
    consumerPausesTotal.inc({ reason: "database_unavailable" });
    logger.warn({ event: "consumer_paused", ...errorSummary(error) }, "banco indisponível ou saturado; o consumidor para de buscar mensagens novas até ele responder");
  }

  private async waitForDatabase(): Promise<void> {
    while (!this.stopping) {
      if (await this.settings.isDatabaseReady()) {
        this.databaseUnavailable = false;
        logger.info({ event: "consumer_resumed", pausedMilliseconds: Math.round(performance.now() - this.pausedAt) }, "o banco respondeu; o consumidor volta a buscar mensagens");
        return;
      }
      await this.sleepUnlessStopping(this.settings.retryPolicy.baseMilliseconds);               // confere de novo a cada base do backoff
    }
  }

  // ----------------------------------------------------------------------------------------------------
  //                                    LOTE E GRUPO (WALLET)
  // ----------------------------------------------------------------------------------------------------
  // grupos diferentes em paralelo, entre si e com os que já estavam em mãos; dentro do grupo, em ordem
  private startGroups(messages: ReceivedMessage[], receivedAt: number): void {
    const messagesByGroup = new Map<string, ReceivedMessage[]>();
    for (const message of messages) {
      const group = messagesByGroup.get(message.groupId) ?? [];
      group.push(message);
      messagesByGroup.set(message.groupId, group);
    }
    this.messagesInHands = this.messagesInHands + messages.length;
    for (const group of messagesByGroup.values()) {
      const groupInHands: Promise<void> = this.processGroup(group, receivedAt)
        .catch((error: unknown) => { logger.error({ event: "sqs_group_failed", ...errorSummary(error) }, "erro inesperado no grupo; as mensagens que sobraram voltam pela visibilidade"); })
        .finally(() => { this.groupsInHands.delete(groupInHands); });
      this.groupsInHands.add(groupInHands);
    }
  }

  // O grupo inteiro fica com o consumidor até a última mensagem terminar, inclusive quando a da frente
  // falha: devolver as seguintes gastaria um recebimento delas sem elas terem sido tentadas.
  private async processGroup(group: ReceivedMessage[], receivedAt: number): Promise<void> {
    let messagesLeft = group.length;                                                            // as do grupo que ainda estão em mãos
    const visibility = new GroupVisibility(this.queue, group, receivedAt, this.settings.visibilityTimeoutMilliseconds, this.inHandsLimitMilliseconds());
    visibility.startWatching();                                                                 // estende sozinho se o processamento ou a espera passar do estimado
    try {
      for (const message of group) {
        if (this.stopping) {
          await this.releaseOnShutdown(visibility);                                             // recebida e não iniciada: volta na hora
          return;
        }
        if (!visibility.hasLeft(this.settings.longestMessageMilliseconds)) {                    // não cabe mais uma espera inteira no que sobrou
          const extended = await visibility.extend();
          if (!extended) {
            await visibility.release();                                                         // sem extensão o prazo venceria no meio; voltam na hora
            return;
          }
        }
        const result = await this.processMessage(message);
        if (result === "stopped") {
          await this.releaseOnShutdown(visibility);                                             // desligando com esta esperando nova tentativa: ela e as seguintes voltam
          return;
        }
        visibility.finish(message);
        messagesLeft = messagesLeft - 1;
        this.markOutOfHands(1);                                                                 // terminou: abre vaga sem esperar o resto do grupo
      }
    } finally {
      visibility.stopWatching();
      this.markOutOfHands(messagesLeft);                                                        // as devolvidas também saíram das mãos
    }
  }

  // O teto de tempo em mãos de uma mensagem: o orçamento de tentativas, mais a espera máxima da fila (a
  // tentativa final, que começa no fim do orçamento), mais uma folga de outra espera máxima (a primeira
  // tentativa, antes de o orçamento começar a contar) e de um prazo de visibilidade (as chamadas ao SQS
  // do fim). Uma mensagem que não terminou nesse tempo está presa numa chamada que não volta.
  private inHandsLimitMilliseconds(): number {
    const longestMessageMilliseconds = this.settings.longestMessageMilliseconds;                // o statement_timeout da fila
    const marginMilliseconds = longestMessageMilliseconds + this.settings.visibilityTimeoutMilliseconds;   // 15 s + 30 s em produção
    return this.settings.retryBudgetMilliseconds + longestMessageMilliseconds + marginMilliseconds;        // 6 minutos em produção
  }

  // Uma busca aberta pegaria a mensagem assim que ela voltasse, e isso gastaria mais um recebimento
  // dela; cancelada, seria pior, porque o SQS ainda pode entregar para uma conexão que ninguém lê mais.
  private async releaseOnShutdown(visibility: GroupVisibility): Promise<void> {
    await this.receiveInProgress;                                                               // no máximo a busca curta de 1 s
    await visibility.release();
  }

  // ----------------------------------------------------------------------------------------------------
  //                         UMA MENSAGEM, COM AS NOVAS TENTATIVAS NO CONSUMIDOR
  // ----------------------------------------------------------------------------------------------------
  private async processMessage(message: ReceivedMessage): Promise<MessageResult> {
    let parsed: ParsedMessage | undefined;
    let firstStep: Step = { kind: "process" };
    try {
      parsed = this.parseMessage(message.body);
    } catch (error) {
      const reason = error instanceof PermanentMessageError ? error.reason : "VALIDATION_ERROR";
      firstStep = { kind: "dead_letter", reason, detail: error instanceof Error ? error.message : String(error) };
    }
    const messageId = parsed?.messageId ?? message.sqsMessageId;                                // o id da mensagem; sem envelope válido, o do SQS
    return runWithLogContext({ messageId }, () => this.runWithRetries(message, parsed, firstStep));
  }

  // A tentativa se repete ali mesmo depois do backoff, com o prazo de visibilidade estendido, até o
  // orçamento de tempo acabar, contado da primeira falha. A tentativa final cai no fim do orçamento,
  // então uma queda do banco mais curta que ele nunca manda a mensagem para a DLQ.
  private async runWithRetries(message: ReceivedMessage, parsed: ParsedMessage | undefined, firstStep: Step): Promise<MessageResult> {
    let step = firstStep;
    let firstFailureAt: number | undefined;
    for (let attempt = 1; ; attempt++) {
      try {
        const nextStep = step.kind === "process" ? await this.processOnce(message, parsed!) : step;
        step = nextStep;                                                                        // se o próximo passo falhar, a nova tentativa começa dele
        if (nextStep.kind === "delete") {
          await this.queue.delete(message);                                                     // ack só depois do commit
          logger.info({ event: "sqs_message_acknowledged", receiveCount: message.receiveCount, attempts: attempt }, "mensagem confirmada");
          return "finished";
        }
        await this.forwardToDeadLetter(message, nextStep.reason, nextStep.detail);
        return "finished";
      } catch (error) {
        if (step.kind === "process" && isDatabaseUnavailableError(error)) this.pauseReceiving(error);
        firstFailureAt ??= performance.now();
        const budgetLeftMilliseconds = this.settings.retryBudgetMilliseconds - (performance.now() - firstFailureAt);
        if (budgetLeftMilliseconds <= 0) return this.giveUp(message, step, attempt, error);
        if (this.stopping) {
          logger.warn({ event: "sqs_message_failed_during_shutdown", attempt, step: step.kind, receiveCount: message.receiveCount, ...errorSummary(error) }, "falha transitória com o desligamento em andamento; a mensagem volta para a fila em vez de tentar de novo");
          return "stopped";
        }
        const delayMilliseconds = Math.min(fullJitterDelayMilliseconds(attempt - 1, this.settings.retryPolicy), budgetLeftMilliseconds);   // a última espera termina junto com o orçamento
        retriesTotal.inc({ component: "sqs_consumer" });
        logger.warn({ event: "sqs_message_retry_scheduled", attempt, step: step.kind, receiveCount: message.receiveCount, delayMilliseconds, ...errorSummary(error) }, "falha transitória; a mensagem fica com o consumidor e tenta de novo depois do backoff");
        await this.sleepUnlessStopping(delayMilliseconds);
        if (this.stopping) return "stopped";
      }
    }
  }

  // uma passada pelo caso de uso: devolve o próximo passo, ou lança o erro transitório (nada foi gravado)
  private async processOnce(message: ReceivedMessage, parsed: ParsedMessage): Promise<Exclude<Step, { kind: "process" }>> {
    const correlationId = isValidCorrelationId(message.correlationIdAttribute) ? message.correlationIdAttribute : parsed.messageId;
    try {
      await this.processWager.execute(parsed.command, parsed.idempotencyKey, { channel: "sqs", correlationId, causationId: parsed.messageId, inbox: { consumerName: this.settings.consumerName, messageId: parsed.messageId } });
    } catch (error) {
      if (!this.isPermanent(error)) throw error;
      if (error instanceof InboxPayloadConflictError) inboxPayloadConflictsTotal.inc();
      return { kind: "dead_letter", reason: (error as ApplicationError).code, detail: (error as Error).message };
    }
    return { kind: "delete" };
  }

  private isPermanent(error: unknown): boolean {
    return error instanceof ValidationError || error instanceof WalletNotFoundError || error instanceof IdempotencyKeyConflictError || error instanceof ExternalTransactionIdConflictError || error instanceof InboxPayloadConflictError;
  }

  // Orçamento esgotado. Sem commit, a mensagem vai para a DLQ com o motivo. Com o commit feito (só faltava
  // o DeleteMessage) ela nunca vai para a DLQ: volta pela visibilidade e a inbox a reconhece.
  private async giveUp(message: ReceivedMessage, step: Step, attempts: number, error: unknown): Promise<MessageResult> {
    if (step.kind === "process") {
      const lastError = error instanceof Error ? error.message.split("\n")[0] : String(error);   // primeira linha: o detalhe do pg pode trazer valores
      try {
        await this.forwardToDeadLetter(message, RETRY_BUDGET_EXHAUSTED, `${attempts} tentativas; a última: ${lastError}`);
      } catch (forwardError) {
        logger.error({ event: "sqs_dead_letter_failed", ...errorSummary(forwardError) }, "orçamento esgotado e não deu para mandar para a DLQ; a mensagem volta pela visibilidade");
      }
      return "finished";
    }
    logger.error({ event: "sqs_message_abandoned", step: step.kind, attempts, ...errorSummary(error) }, "a fila não respondeu dentro do orçamento; a mensagem volta pela visibilidade e é reclassificada (a inbox reconhece a que já tinha commit)");
    return "finished";
  }

  private async forwardToDeadLetter(message: ReceivedMessage, reason: string, detail: string): Promise<void> {
    await this.queue.sendToDeadLetter(message, reason);                                         // não é atômico com o delete; a dedup da DLQ segura a cópia
    await this.queue.delete(message);
    dlqForwardedTotal.inc({ reason });
    logger.warn({ event: "sqs_message_sent_to_dlq", reason, detail, receiveCount: message.receiveCount }, "mensagem enviada para a DLQ");
  }

  // espera que o desligamento interrompe: o SIGTERM não fica preso num backoff de até 60 s
  private async sleepUnlessStopping(milliseconds: number): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const slept = new Promise<void>((resolve) => { timer = setTimeout(resolve, milliseconds); });
    await Promise.race([slept, this.stopRequested]);
    clearTimeout(timer);
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
