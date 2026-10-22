// ====================================================================================================
//                COMPOSIÇÃO: MONTA TODAS AS PEÇAS DA APLICAÇÃO COM CHAMADAS EXPLÍCITAS
// ====================================================================================================
// O NestJS só recebe o que sai daqui. O código de produção não tem "modo de teste": os testes trocam
// uma porta pelos overrides (fila que morre antes do ack, outbox que lança erro), e banco e fila
// continuam reais.
import type { SQSClient } from "@aws-sdk/client-sqs";
import type { MikroORM } from "@mikro-orm/postgresql";
import { CreateWallet } from "./application/create-wallet";
import { OutboxPublisher } from "./application/outbox-publisher";
import type { Clock, EventPublisher, IdGenerator, RandomSource, Repositories } from "./application/ports";
import { ProcessWagerTransaction } from "./application/process-wager-transaction";
import { WalletQueries } from "./application/queries";
import { intervalMilliseconds, type AppConfig } from "./config";
import type { RetryPolicy } from "./domain/backoff";
import { BackgroundLoop } from "./infrastructure/background-loop";
import { approximateNumberOfMessages, createSqsClient, QueueUrls, SqsEventPublisher, SqsInputQueue, type InputQueue } from "./infrastructure/messaging/sqs";
import { SqsConsumer } from "./infrastructure/messaging/sqs-consumer";
import { MikroOrmDatabase, type SchemaStatus } from "./infrastructure/persistence/database";
import { MikroOrmInboxRepository, MikroOrmOutboxRepository } from "./infrastructure/persistence/inbox-outbox-repositories";
import { MikroOrmLedgerRepository } from "./infrastructure/persistence/ledger-repository";
import { createOrm } from "./infrastructure/persistence/orm";
import { MikroOrmWagerTransactionRepository } from "./infrastructure/persistence/wager-transaction-repository";
import { MikroOrmWalletRepository } from "./infrastructure/persistence/wallet-repository";
import { mathRandom, systemClock, uuidV7Generator } from "./infrastructure/system";
import { errorSummary, logger } from "./observability/logger";
import { dlqMessages, outboxLagSeconds, outboxPending, outboxStuck, pendingReferenceGauge } from "./observability/metrics";

export interface CompositionOverrides {
  clock?: Clock;
  random?: RandomSource;
  repositories?: (real: Repositories) => Repositories;
  inputQueue?: (real: InputQueue) => InputQueue;
  eventPublisher?: (real: EventPublisher) => EventPublisher;
}

export interface Application {
  config: AppConfig;
  orm: MikroORM;
  database: MikroOrmDatabase;
  sqsClient: SQSClient;
  queueUrls: QueueUrls;
  repositories: Repositories;
  processWager: ProcessWagerTransaction;                                                        // HTTP, com o lock_timeout curto
  processWagerInBackground: ProcessWagerTransaction;                                            // fila e worker, com o lock_timeout longo
  createWallet: CreateWallet;
  queries: WalletQueries;
  outboxPublisher: OutboxPublisher;
  consumer: SqsConsumer;
  backgroundLoops: BackgroundLoop[];
  ids: IdGenerator;
  shutdownState: { shuttingDown: boolean };
  checkReadiness: () => Promise<Readiness>;
}

// readiness: desligando responde 503 sem olhar nada; senão, banco (com as tabelas) e fila de entrada
export type Readiness =
  | { shuttingDown: true }
  | { shuttingDown: false; ready: boolean; checks: { postgres: SchemaStatus; sqs: "ok" | "unavailable" } };

function retryPolicy(baseMilliseconds: number, capMilliseconds: number, random: RandomSource): RetryPolicy {
  return { baseMilliseconds, capMilliseconds, random: () => random.next() };
}

export async function composeApplication(config: AppConfig, overrides: CompositionOverrides = {}): Promise<Application> {
  const clock = overrides.clock ?? systemClock;
  const random = overrides.random ?? mathRandom;
  const ids = uuidV7Generator;

  // ----------------------------------------------------------------------------------------------------
  //                                        PERSISTÊNCIA
  // ----------------------------------------------------------------------------------------------------
  const orm = await createOrm({ databaseUrl: config.database.url, poolMax: config.database.poolMax, idleInTransactionTimeoutMilliseconds: config.database.idleInTransactionTimeoutMilliseconds, connectTimeoutMilliseconds: config.database.connectTimeoutMilliseconds });
  // dois runners sobre o mesmo pool: no HTTP alguém espera a resposta, então a espera pelo lock é curta
  // (503 e o provedor reenvia); na fila, no worker e no publisher ninguém espera, e desistir cedo só gasta
  // mais uma tentativa
  const database = new MikroOrmDatabase(orm, { lockTimeout: config.database.lockTimeout, statementTimeout: config.database.statementTimeout });
  const backgroundDatabase = new MikroOrmDatabase(orm, { lockTimeout: config.database.backgroundLockTimeout, statementTimeout: config.database.backgroundStatementTimeout });
  const realRepositories: Repositories = {
    wallets: new MikroOrmWalletRepository(database),
    transactions: new MikroOrmWagerTransactionRepository(database),
    ledger: new MikroOrmLedgerRepository(database),
    inbox: new MikroOrmInboxRepository(database),
    outbox: new MikroOrmOutboxRepository(database),
  };
  const repositories = overrides.repositories ? overrides.repositories(realRepositories) : realRepositories;

  // ----------------------------------------------------------------------------------------------------
  //                                         MENSAGERIA
  // ----------------------------------------------------------------------------------------------------
  const sqsClient = createSqsClient(config.sqs);
  const longPollingSqsClient = createSqsClient(config.sqs, (config.sqs.waitTimeSeconds + 5) * 1000);
  const queueUrls = new QueueUrls(sqsClient);
  const realInputQueue = new SqsInputQueue(sqsClient, longPollingSqsClient, queueUrls, config.sqs.inputQueueName, config.sqs.deadLetterQueueName);
  const inputQueue = overrides.inputQueue ? overrides.inputQueue(realInputQueue) : realInputQueue;
  const realEventPublisher = new SqsEventPublisher(sqsClient, queueUrls, config.sqs.eventsQueueName);
  const eventPublisher = overrides.eventPublisher ? overrides.eventPublisher(realEventPublisher) : realEventPublisher;

  // ----------------------------------------------------------------------------------------------------
  //                                       CASOS DE USO
  // ----------------------------------------------------------------------------------------------------
  const processWagerSettings = {
    pendingRetryPolicy: retryPolicy(config.pendingReference.retryBaseMilliseconds, config.pendingReference.retryCapMilliseconds, random),
    referenceTtlMilliseconds: config.pendingReference.ttlMilliseconds,
  };
  const processWager = new ProcessWagerTransaction(database, repositories, clock, ids, processWagerSettings);                    // HTTP
  const processWagerInBackground = new ProcessWagerTransaction(backgroundDatabase, repositories, clock, ids, processWagerSettings);   // fila e worker: o mesmo caso de uso, com espera maior
  const createWallet = new CreateWallet(database, repositories, clock, ids);
  const queries = new WalletQueries(repositories, config.ledgerPage);
  const outboxPublisher = new OutboxPublisher(backgroundDatabase, repositories.outbox, eventPublisher, clock, {
    batchSize: config.outbox.batchSize,
    retryPolicy: retryPolicy(config.outbox.retryBaseMilliseconds, config.outbox.retryCapMilliseconds, random),
    stuckAfterAttempts: config.outbox.stuckAfterAttempts,
  });
  const consumer = new SqsConsumer(inputQueue, processWagerInBackground, {
    consumerName: config.consumer.name,
    acceptedCurrencies: config.acceptedCurrencies,
    waitTimeSeconds: config.sqs.waitTimeSeconds,
    retryPolicy: retryPolicy(config.consumer.retryBaseMilliseconds, config.consumer.retryCapMilliseconds, random),
    retryBudgetMilliseconds: config.consumer.retryBudgetMilliseconds,
    shutdownTimeoutMilliseconds: config.consumer.shutdownTimeoutMilliseconds,
    maxInFlightMessages: config.consumer.maxInFlightMessages,
    visibilityTimeoutMilliseconds: config.sqs.visibilityTimeoutSeconds * 1_000,
    longestMessageMilliseconds: intervalMilliseconds("DATABASE_BACKGROUND_STATEMENT_TIMEOUT", config.database.backgroundStatementTimeout),   // a estimativa do que uma mensagem espera no banco
    isDatabaseReady: async () => (await database.schemaStatus()) === "ok",                     // a mesma checagem do banco da readiness, abaixo
  });

  // ----------------------------------------------------------------------------------------------------
  //                                      LAÇOS DE FUNDO
  // ----------------------------------------------------------------------------------------------------
  const backgroundLoops: BackgroundLoop[] = [];
  if (config.outbox.enabled) {
    backgroundLoops.push(new BackgroundLoop("outbox_publisher", config.outbox.intervalMilliseconds, async () => {
      const taken = await outboxPublisher.publishOnce();
      return taken >= config.outbox.batchSize ? "run_again_now" : "wait";                       // lote cheio: volta na hora
    }, config.backgroundLoopFailureCapMilliseconds));
  }
  if (config.pendingReference.enabled) {
    backgroundLoops.push(new BackgroundLoop("pending_reference_worker", config.pendingReference.intervalMilliseconds, async () => {
      const foundOne = await processWagerInBackground.processNextDuePending();
      return foundOne ? "run_again_now" : "wait";
    }, config.backgroundLoopFailureCapMilliseconds));
  }
  backgroundLoops.push(new BackgroundLoop("outbox_and_pending_gauges", config.outbox.gaugeIntervalMilliseconds, async () => {
    const stats = await repositories.outbox.stats(config.outbox.stuckAfterAttempts);
    outboxPending.set(stats.pending);
    outboxStuck.set(stats.stuck);
    outboxLagSeconds.set(stats.oldestOccurredAt ? Math.max(0, (clock.now().getTime() - stats.oldestOccurredAt.getTime()) / 1000) : 0);
    pendingReferenceGauge.set(await repositories.transactions.countPending());
    return "wait";
  }, config.backgroundLoopFailureCapMilliseconds));
  backgroundLoops.push(new BackgroundLoop("dlq_gauge", config.dlqGaugeIntervalMilliseconds, async () => {
    try {
      dlqMessages.set(await approximateNumberOfMessages(sqsClient, queueUrls, config.sqs.deadLetterQueueName));   // conta também o que o próprio SQS moveu
    } catch (error) {
      logger.warn({ event: "dlq_gauge_failed", ...errorSummary(error) }, "não deu para ler o tamanho da DLQ");
    }
    return "wait";
  }, config.backgroundLoopFailureCapMilliseconds));

  // ----------------------------------------------------------------------------------------------------
  //                                          READINESS
  // ----------------------------------------------------------------------------------------------------
  const shutdownState = { shuttingDown: false };
  async function checkReadiness(): Promise<Readiness> {
    if (shutdownState.shuttingDown) return { shuttingDown: true };
    const postgres = await database.schemaStatus();                                             // conexão e as 5 tabelas
    let sqs: "ok" | "unavailable" = "ok";
    try {
      await approximateNumberOfMessages(sqsClient, queueUrls, config.sqs.inputQueueName);      // GetQueueAttributes na fila de entrada
    } catch {
      sqs = "unavailable";
    }
    return { shuttingDown: false, ready: postgres === "ok" && sqs === "ok", checks: { postgres, sqs } };
  }

  return { config, orm, database, sqsClient, queueUrls, repositories, processWager, processWagerInBackground, createWallet, queries, outboxPublisher, consumer, backgroundLoops, ids, shutdownState, checkReadiness };
}

export function startBackgroundWork(application: Application): void {
  for (const loop of application.backgroundLoops) loop.start();
  if (application.config.consumer.enabled) application.consumer.start();
}

// passos 1 a 4 do SIGTERM: consumidor, worker e publisher terminam o item atual e param
export async function stopBackgroundWork(application: Application): Promise<void> {
  await application.consumer.stop();
  await Promise.all(application.backgroundLoops.map((loop) => loop.stop()));
}
