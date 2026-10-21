// ====================================================================================================
//                 CASO DE USO: PROCESSAR UMA TRANSAÇÃO (HTTP, FILA E WORKER DE PENDENTES)
// ====================================================================================================
// O mesmo fluxo atende os três canais (seção 8). A garantia vem do banco: lock da wallet, UNIQUE da
// idempotency_key e de (provider_id, external_transaction_id), inbox e outbox no mesmo commit.
import type { RetryPolicy } from "../domain/backoff";                                           // backoff das pendentes
import { WagerTransactionFailed, WagerTransactionPendingReference, WagerTransactionProcessed, WagerTransactionRejected, WalletBalanceChanged, type EventContext, type IntegrationEvent } from "../domain/events";
import { FailureCode } from "../domain/failure-code";                                           // PROCESSING_FAILED
import { InboxMessage } from "../domain/inbox-message";                                         // dedup da fila
import { OutboxMessage } from "../domain/outbox-message";                                       // eventos no mesmo commit
import type { Wallet } from "../domain/wallet";
import { settleWagerTransaction, type SettlementOutcome } from "../domain/wager-settlement";   // regras de negócio
import { WagerTransaction, WagerTransactionStatus } from "../domain/wager-transaction";
import { logger, errorSummary } from "../observability/logger";                                 // logs sem valores
import { addToLogContext, runWithLogContext } from "../observability/log-context";              // ids no contexto
import { concurrencyConflictsTotal, duplicatesTotal, idempotencyConflictsTotal, processingDurationSeconds, retriesTotal, transactionsTotal } from "../observability/metrics";
import { ExternalTransactionIdConflictError, IdempotencyKeyConflictError, InboxPayloadConflictError, WalletNotFoundError } from "./errors";
import { concurrencyConflictType, isTransientInfrastructureError, uniqueViolationConstraint } from "./infrastructure-errors";
import { computePayloadHash } from "./payload-hash";                                            // hash do comando
import type { Clock, IdGenerator, Repositories, TransactionRunner } from "./ports";
import type { WagerCommand } from "./wager-command";

export type Channel = "http" | "sqs" | "pending_worker";

export interface ProcessContext {
  channel: Channel;
  correlationId: string;
  causationId: string;
  inbox?: { consumerName: string; messageId: string };                                          // só na entrada pela fila
}

export type ProcessResult =
  | { kind: "decided"; transaction: WagerTransaction; idempotentReplay: boolean }
  | { kind: "message_already_processed" };                                                      // inbox já tinha a mensagem com o mesmo hash

export interface ProcessWagerTransactionSettings {
  pendingRetryPolicy: RetryPolicy;
  referenceTtlMilliseconds: number;
}

const IDEMPOTENCY_CONSTRAINTS = ["wager_transactions_idempotency_key_unique", "wager_transactions_provider_external_unique"];

export class ProcessWagerTransaction {
  constructor(
    private readonly runner: TransactionRunner,
    private readonly repositories: Repositories,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
    private readonly settings: ProcessWagerTransactionSettings,
  ) {}

  // ====================================================================================================
  //                                 ENTRADA NOVA (HTTP OU FILA)
  // ====================================================================================================
  async execute(command: WagerCommand, idempotencyKey: string, context: ProcessContext): Promise<ProcessResult> {
    const startedAt = performance.now();
    addToLogContext({ walletId: command.walletId, providerId: command.providerId, correlationId: context.correlationId });
    const payloadHash = computePayloadHash(command);

    // atalho sem lock, só pela key: replay não entra na fila do lock da wallet
    const replay = await this.findReplay(idempotencyKey, payloadHash);
    if (replay) return this.finishReplay(replay, context, startedAt);

    try {
      const result = await this.runner.run(() => this.decideUnderLock(command, idempotencyKey, payloadHash, context));
      this.recordDecision(result, context, startedAt);
      return result;
    } catch (error) {
      const constraint = uniqueViolationConstraint(error);
      if (constraint && IDEMPOTENCY_CONSTRAINTS.includes(constraint)) {
        // duas cópias que travaram wallets diferentes: a outra ganhou o INSERT, então consulta de novo
        concurrencyConflictsTotal.inc({ type: "unique_violation" });
        const winner = await this.findReplay(idempotencyKey, payloadHash);
        if (winner) return this.finishReplay(winner, context, startedAt);
        await this.rejectReusedExternalId(command);                                             // a key não existe, então foi o par (provider, external)
      }
      const conflictType = concurrencyConflictType(error);
      if (conflictType && conflictType !== "unique_violation") concurrencyConflictsTotal.inc({ type: conflictType });
      processingDurationSeconds.observe({ channel: context.channel, outcome: isTransientInfrastructureError(error) ? "transient_error" : "error" }, (performance.now() - startedAt) / 1000);
      throw error;
    }
  }

  private async decideUnderLock(command: WagerCommand, idempotencyKey: string, payloadHash: string, context: ProcessContext): Promise<ProcessResult> {
    const now = this.clock.now();

    if (context.inbox) {
      const inboxMessage = InboxMessage.receive({ messageId: context.inbox.messageId, consumerName: context.inbox.consumerName, payloadHash, receivedAt: now });
      inboxMessage.markProcessed(now);                                                          // gravada no mesmo commit do efeito
      const inserted = await this.repositories.inbox.insertIfAbsent(inboxMessage);
      if (!inserted) {
        const seen = await this.repositories.inbox.find(context.inbox.consumerName, context.inbox.messageId);
        if (seen && seen.isSameMessage(payloadHash)) {
          duplicatesTotal.inc({ source: "inbox" });
          return { kind: "message_already_processed" };
        }
        throw new InboxPayloadConflictError(`messageId ${context.inbox.messageId} já foi visto com outro conteúdo`);
      }
    }

    const wallet = await this.repositories.wallets.findByIdForUpdate(command.walletId);
    if (!wallet) throw new WalletNotFoundError(`wallet ${command.walletId} não existe`);

    // de novo, agora serializado pela wallet: fecha a corrida das cópias simultâneas
    const replayUnderLock = await this.findReplay(idempotencyKey, payloadHash);
    if (replayUnderLock) return { kind: "decided", transaction: replayUnderLock, idempotentReplay: true };
    await this.rejectReusedExternalId(command);

    const transaction = WagerTransaction.create({ ...command, id: this.ids.newId(), idempotencyKey, payloadHash, correlationId: context.correlationId, createdAt: now });
    addToLogContext({ transactionId: transaction.id });
    const reference = await this.findReference(transaction);
    const referenceAlreadyReversed = reference && transaction.requiresReference() ? await this.repositories.transactions.hasProcessedReversal(reference.id) : false;

    const versionBeforeMovement = wallet.version;
    const outcome = settleWagerTransaction({
      transaction, wallet, reference, referenceAlreadyReversed, ledgerEntryId: this.ids.newId(), now,
      pendingRetryPolicy: this.settings.pendingRetryPolicy, referenceTtlMilliseconds: this.settings.referenceTtlMilliseconds,
    });

    await this.repositories.transactions.insert(transaction);                                   // antes do lançamento: o ledger tem FK para ela
    await this.persistMovement(outcome, wallet, versionBeforeMovement);
    await this.repositories.outbox.insert(this.eventsFor(outcome, transaction, wallet, context));
    return { kind: "decided", transaction, idempotentReplay: false };
  }

  // ====================================================================================================
  //                           NOVA TENTATIVA DE UMA PENDENTE (WORKER)
  // ====================================================================================================
  // Devolve false quando não havia pendente vencida. Ordem de locks: linha da pendente, depois wallet.
  async processNextDuePending(): Promise<boolean> {
    return runWithLogContext({}, () => this.processNextDuePendingInContext());                   // o laço de fundo não tem contexto de log próprio
  }

  private async processNextDuePendingInContext(): Promise<boolean> {
    let pendingId: string | undefined;
    const startedAt = performance.now();
    try {
      const outcome = await this.runner.run(async () => {
        const now = this.clock.now();
        const pending = await this.repositories.transactions.lockNextDuePending(now);
        if (!pending) return undefined;
        pendingId = pending.id;
        addToLogContext({ transactionId: pending.id, walletId: pending.walletId, providerId: pending.providerId, correlationId: pending.correlationId });

        const wallet = await this.repositories.wallets.findByIdForUpdate(pending.walletId);
        if (!wallet) throw new WalletNotFoundError(`wallet ${pending.walletId} da pendente não existe`);   // FK impede; se acontecer é bug
        const reference = await this.findReference(pending);
        const referenceAlreadyReversed = reference && pending.requiresReference() ? await this.repositories.transactions.hasProcessedReversal(reference.id) : false;

        const versionBeforeMovement = wallet.version;
        const settlement = settleWagerTransaction({
          transaction: pending, wallet, reference, referenceAlreadyReversed, ledgerEntryId: this.ids.newId(), now,
          pendingRetryPolicy: this.settings.pendingRetryPolicy, referenceTtlMilliseconds: this.settings.referenceTtlMilliseconds,
        });

        await this.repositories.transactions.updateDecision(pending);
        await this.persistMovement(settlement, wallet, versionBeforeMovement);
        const workerContext: ProcessContext = { channel: "pending_worker", correlationId: pending.correlationId, causationId: pending.id };
        await this.repositories.outbox.insert(this.eventsFor(settlement, pending, wallet, workerContext));
        return { settlement, pending };
      });

      if (!outcome) return false;
      if (outcome.settlement.outcome === "still_pending_reference") {
        retriesTotal.inc({ component: "pending_reference" });
        logger.info({ event: "pending_reference_retry_scheduled", attempt: outcome.pending.attempts }, "pendente continua sem referência, nova tentativa agendada");
      } else {
        this.recordDecision({ kind: "decided", transaction: outcome.pending, idempotentReplay: false }, { channel: "pending_worker", correlationId: outcome.pending.correlationId, causationId: outcome.pending.id }, startedAt);
      }
      return true;
    } catch (error) {
      if (pendingId === undefined) throw error;                                                 // falhou antes de pegar uma pendente
      if (isTransientInfrastructureError(error)) {
        logger.warn({ event: "pending_reference_transient_error", ...errorSummary(error) }, "erro transitório ao reprocessar pendente; volta na próxima varredura");
        return false;
      }
      await this.markPendingAsFailed(pendingId, error);
      return true;
    }
  }

  // erro permanente: uma transação SQL separada marca FAILED, porque a anterior foi desfeita
  private async markPendingAsFailed(pendingId: string, cause: unknown): Promise<void> {
    logger.error({ event: "pending_reference_failed", transactionId: pendingId, ...errorSummary(cause) }, "erro permanente ao reprocessar pendente; marcando FAILED");
    const failed = await this.runner.run(async () => {
      const pending = await this.repositories.transactions.findByIdForUpdate(pendingId);
      if (!pending || pending.status !== WagerTransactionStatus.PendingReference) return undefined;   // outra instância já decidiu
      pending.fail(FailureCode.ProcessingFailed, this.clock.now());
      await this.repositories.transactions.updateDecision(pending);
      const event = WagerTransactionFailed.from(pending, this.eventContext({ channel: "pending_worker", correlationId: pending.correlationId, causationId: pending.id }));
      await this.repositories.outbox.insert([OutboxMessage.enqueue(event)]);
      return pending;
    });
    if (failed) transactionsTotal.inc({ kind: failed.kind, status: failed.status, channel: "pending_worker" });
  }

  // ====================================================================================================
  //                                        PEDAÇOS COMUNS
  // ====================================================================================================
  // mesma key e mesmo hash é replay; mesma key e hash diferente é conflito, nunca replay
  private async findReplay(idempotencyKey: string, payloadHash: string): Promise<WagerTransaction | undefined> {
    const byKey = await this.repositories.transactions.findByIdempotencyKey(idempotencyKey);
    if (!byKey) return undefined;
    if (byKey.matchesPayload(payloadHash)) return byKey;
    idempotencyConflictsTotal.inc({ type: "idempotency_key" });
    throw new IdempotencyKeyConflictError(`a Idempotency-Key já foi usada por outra operação (transação ${byKey.id})`);
  }

  // chamada depois que a key não foi achada: o mesmo (providerId, externalTransactionId) com outra key é conflito
  private async rejectReusedExternalId(command: WagerCommand): Promise<void> {
    const byExternalId = await this.repositories.transactions.findByProviderAndExternalId(command.providerId, command.externalTransactionId);
    if (!byExternalId) return;
    idempotencyConflictsTotal.inc({ type: "external_transaction_id" });
    throw new ExternalTransactionIdConflictError(`externalTransactionId já foi usado com outra Idempotency-Key (transação ${byExternalId.id})`);
  }

  private finishReplay(transaction: WagerTransaction, context: ProcessContext, startedAt: number): ProcessResult {
    duplicatesTotal.inc({ source: "idempotency_key" });
    addToLogContext({ transactionId: transaction.id });
    processingDurationSeconds.observe({ channel: context.channel, outcome: "replay" }, (performance.now() - startedAt) / 1000);
    logger.info({ event: "wager_transaction_replayed", kind: transaction.kind, status: transaction.status, channel: context.channel }, "replay: devolvendo o resultado original");
    return { kind: "decided", transaction, idempotentReplay: true };
  }

  private recordDecision(result: ProcessResult, context: ProcessContext, startedAt: number): void {
    if (result.kind !== "decided") {
      processingDurationSeconds.observe({ channel: context.channel, outcome: "message_already_processed" }, (performance.now() - startedAt) / 1000);
      return;
    }
    const { transaction } = result;
    addToLogContext({ transactionId: transaction.id, walletId: transaction.walletId, providerId: transaction.providerId });   // vale também para o replay achado sob lock
    const outcomeLabel = result.idempotentReplay ? "replay" : transaction.status.toLowerCase();
    if (result.idempotentReplay) duplicatesTotal.inc({ source: "idempotency_key" });
    else transactionsTotal.inc({ kind: transaction.kind, status: transaction.status, channel: context.channel });
    processingDurationSeconds.observe({ channel: context.channel, outcome: outcomeLabel }, (performance.now() - startedAt) / 1000);
    logger.info({ event: "wager_transaction_decided", kind: transaction.kind, status: transaction.status, failureCode: transaction.failureCode, channel: context.channel, idempotentReplay: result.idempotentReplay }, "transação decidida");
  }

  private async findReference(transaction: WagerTransaction): Promise<WagerTransaction | undefined> {
    if (transaction.referenceExternalTransactionId === undefined || transaction.providerId === undefined) return undefined;
    return this.repositories.transactions.findByProviderAndExternalId(transaction.providerId, transaction.referenceExternalTransactionId);   // sempre dentro do mesmo provedor
  }

  private async persistMovement(outcome: SettlementOutcome, wallet: Wallet, versionBeforeMovement: number): Promise<void> {
    if (outcome.outcome !== "processed" || !outcome.ledgerEntry) return;                        // sem movimento, sem UPDATE e sem lançamento
    await this.repositories.wallets.updateBalance(wallet, versionBeforeMovement);
    await this.repositories.ledger.insert(outcome.ledgerEntry);
  }

  private eventsFor(outcome: SettlementOutcome, transaction: WagerTransaction, wallet: Wallet, context: ProcessContext): OutboxMessage[] {
    const events: IntegrationEvent<unknown>[] = [];
    switch (outcome.outcome) {
      case "processed":
        events.push(WagerTransactionProcessed.from(transaction, this.eventContext(context)));
        if (outcome.ledgerEntry) events.push(WalletBalanceChanged.from(wallet, outcome.ledgerEntry, this.eventContext(context)));
        break;
      case "rejected":
        events.push(WagerTransactionRejected.from(transaction, this.eventContext(context)));
        break;
      case "entered_pending_reference":
        events.push(WagerTransactionPendingReference.from(transaction, this.eventContext(context)));   // sai uma vez, na entrada em espera
        break;
      case "still_pending_reference":
        break;                                                                                  // nova tentativa aparece em log e métrica, não em evento
    }
    return events.map((event) => OutboxMessage.enqueue(event));
  }

  private eventContext(context: ProcessContext): EventContext {
    return { eventId: this.ids.newId(), correlationId: context.correlationId, causationId: context.causationId, occurredAt: this.clock.now() };
  }
}
