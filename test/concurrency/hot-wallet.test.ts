// ====================================================================================================
//    HOT WALLET: CENTENAS DE OPERAÇÕES MISTURADAS NUMA WALLET SÓ, NAS 3 INSTÂNCIAS E NA FILA
// ====================================================================================================
// Prova de correção sob disputa, não teste de carga. O saldo esperado é calculado a partir das
// operações que terminaram PROCESSED e conferido com o saldo e com o ledger. Sorteios com semente 42.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { GetQueueAttributesCommand } from "@aws-sdk/client-sqs";
import { createTestQueues, createWallet, drainQueue, httpRequest, queueUrl, seededRandom, sendWagerMessage, submitLikeProvider, testEnvironment, testSql, testSqsClient, wagerMessage, waitUntil, type HttpResult, type TestQueues, type WagerInput } from "../support/harness";
import { spawnInstance, stopInstance, type Instance } from "../support/instances";
import { expectWalletInvariants } from "../support/invariants";

let queues: TestQueues;
let instances: Instance[] = [];
let hotWalletId = "";

// maxReceiveCount e espera do lock da fila de produção; a visibilidade fica em 20 s (30 em produção) só
// para o teste não demorar, e continua acima dos 15 s de statement_timeout da fila
const PRODUCTION_LIKE_QUEUE = { visibilityTimeoutSeconds: 20, maxReceiveCount: 5, waitTimeSeconds: 1 };
const PRODUCTION_LIKE_ENVIRONMENT = { SQS_VISIBILITY_TIMEOUT_SECONDS: "20", SQS_MAX_RECEIVE_COUNT: "5", DATABASE_BACKGROUND_LOCK_TIMEOUT: "10s", DATABASE_BACKGROUND_STATEMENT_TIMEOUT: "15s" };

beforeAll(async () => {
  queues = await createTestQueues("hot", PRODUCTION_LIKE_QUEUE);
  const environment = testEnvironment(queues, { PENDING_REFERENCE_TTL_MS: "8000", ...PRODUCTION_LIKE_ENVIRONMENT });
  instances = await Promise.all(["hot-1", "hot-2", "hot-3"].map((name) => spawnInstance(name, environment)));
});

afterAll(async () => {
  await expectWalletInvariants([hotWalletId]);
  await Promise.all(instances.map((instance) => stopInstance(instance)));
});

function amountText(cents: number): string {
  return `${Math.floor(cents / 100)}.${String(cents % 100).padStart(2, "0")}`;
}

test("300 operações (BET, WIN, REFUND, ROLLBACK e repetições) numa wallet só: cada operação única vira exatamente uma transação, nenhuma vai para a DLQ, e saldo, ledger e reconciliação batem", async () => {
  const { walletId, playerId } = await createWallet(instances[0]!.baseUrl, "1000.00");
  hotWalletId = walletId;
  const random = seededRandom(42);
  const operations: WagerInput[] = [];
  const reversibleBets: WagerInput[] = [];
  const reversibleWins: WagerInput[] = [];

  for (let index = 0; index < 300; index++) {
    const draw = random();
    let operation: WagerInput;
    if (draw < 0.45 || reversibleBets.length === 0) {
      operation = { walletId, playerId, kind: "BET", amount: amountText(100 + Math.floor(random() * 2000)), externalTransactionId: `hot-bet-${index}`, roundId: `round-${index}` };
      reversibleBets.push(operation);
    } else if (draw < 0.7) {
      operation = { walletId, playerId, kind: "WIN", amount: amountText(50 + Math.floor(random() * 1500)), externalTransactionId: `hot-win-${index}`, roundId: `round-${index}` };
      reversibleWins.push(operation);
    } else if (draw < 0.8) {
      const bet = reversibleBets[Math.floor(random() * reversibleBets.length)]!;
      operation = { walletId, playerId, kind: "REFUND", amount: bet.amount, reference: bet.externalTransactionId, externalTransactionId: `hot-refund-${index}`, roundId: bet.roundId };
    } else if (draw < 0.9) {
      const pool = random() < 0.5 && reversibleWins.length > 0 ? reversibleWins : reversibleBets;
      const target = pool[Math.floor(random() * pool.length)]!;
      operation = { walletId, playerId, kind: "ROLLBACK", amount: target.amount, reference: target.externalTransactionId, externalTransactionId: `hot-rollback-${index}`, roundId: target.roundId };
    } else {
      operation = operations[Math.floor(random() * operations.length)] ?? { walletId, playerId, kind: "LOSS", amount: "0.00", externalTransactionId: `hot-loss-${index}` };   // repetição de uma já enviada
    }
    operations.push(operation);
  }

  // metade por HTTP nas 3 instâncias (reenviando no 503, como o provedor), metade pela fila; tudo ao mesmo tempo
  const retryCounter = { serviceUnavailable: 0, networkErrors: 0 };
  const httpResponses: Promise<HttpResult>[] = [];
  const queueSends: Promise<void>[] = [];
  operations.forEach((operation, index) => {
    const rotatedUrls = [0, 1, 2].map((offset) => instances[(index + offset) % 3]!.baseUrl);
    if (index % 2 === 0) httpResponses.push(submitLikeProvider(rotatedUrls, operation, random, retryCounter));
    else queueSends.push(sendWagerMessage(queues.inputQueueName, wagerMessage(`hot-msg-${index}`, operation)));
  });
  const responses = await Promise.all(httpResponses);
  await Promise.all(queueSends);
  console.log(`hot wallet: reenvios com a mesma key: ${retryCounter.serviceUnavailable} por 503 (lock_timeout), ${retryCounter.networkErrors} por conexão caída`);
  expect(responses.every((response) => [200, 202, 422].includes(response.status))).toBe(true);

  await waitUntil("fila consumida", async () => {
    const attributes = await testSqsClient().send(new GetQueueAttributesCommand({ QueueUrl: await queueUrl(queues.inputQueueName), AttributeNames: ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"] }));
    return Number(attributes.Attributes?.ApproximateNumberOfMessages) === 0 && Number(attributes.Attributes?.ApproximateNumberOfMessagesNotVisible) === 0;
  }, 90_000);
  await waitUntil("nenhuma pendente", async () => {
    const [row] = await testSql()`select count(*)::int as total from wager_transactions where wallet_id = ${walletId} and status = 'PENDING_REFERENCE'`;
    return row.total === 0;
  }, 30_000);

  // saldo esperado a partir do que terminou PROCESSED
  const rows = await testSql()`select t.external_transaction_id, t.kind, t.status, t.amount, r.kind as reference_kind
                                 from wager_transactions t left join wager_transactions r on r.id = t.reference_transaction_id
                                where t.wallet_id = ${walletId} and t.kind <> 'OPENING'`;
  // Toda operação única virou exatamente uma transação (repetição não vira outra), e nenhuma mensagem
  // válida foi para a DLQ: a fila espera o lock até 10 s, então a disputa não esgota o maxReceiveCount.
  const uniqueOperations = new Set(operations.map((operation) => operation.externalTransactionId!));
  const recorded = new Set(rows.map((row: any) => row.external_transaction_id as string));
  expect(recorded.size).toBe(rows.length);                                                      // nenhuma operação duplicada
  const deadLetters = await drainQueue(queues.deadLetterQueueName, 1000);
  const deadLetteredIds = new Set(deadLetters.map((message) => JSON.parse(message.body).data.externalTransactionId as string));
  const missing = [...uniqueOperations].filter((externalTransactionId) => !recorded.has(externalTransactionId));
  console.log(`hot wallet: ${rows.length} transações, ${missing.length} operações sem transação, ${deadLetters.length} mensagens na DLQ (${[...deadLetteredIds].join(", ") || "nenhuma"})`);
  expect(missing).toEqual([]);                                                                  // nada sumiu
  expect(deadLetters).toHaveLength(0);                                                          // nenhuma operação válida desistida

  let expectedCents = 1000_00n;
  for (const row of rows) {
    if (row.status !== "PROCESSED") continue;
    const cents = BigInt(row.amount.replace(".", ""));
    if (row.kind === "BET") expectedCents -= cents;
    if (row.kind === "WIN" || row.kind === "REFUND") expectedCents += cents;
    if (row.kind === "ROLLBACK") expectedCents += row.reference_kind === "BET" ? cents : -cents;   // inverso da referência
  }
  const wallet = await httpRequest(instances[0]!.baseUrl, "GET", `/wallets/${walletId}`);
  expect(wallet.body.balance.amount).toBe(`${expectedCents / 100n}.${String(expectedCents % 100n).padStart(2, "0")}`);
  const reconciliation = await httpRequest(instances[1]!.baseUrl, "POST", `/wallets/${walletId}/reconciliation`);
  expect(reconciliation.body.consistent).toBe(true);

  const statusCount: Record<string, number> = {};
  for (const row of rows) statusCount[`${row.kind}:${row.status}`] = (statusCount[`${row.kind}:${row.status}`] ?? 0) + 1;
  console.log("hot wallet, transações por tipo e status:", JSON.stringify(statusCount));

  // a métrica de espera por lock fica registrada (soma das 3 instâncias)
  let lockWaitSum = 0;
  let lockWaitCount = 0;
  for (const instance of instances) {
    const metrics = (await httpRequest(instance.baseUrl, "GET", "/metrics")).body as string;
    lockWaitSum += Number(/wagering_wallet_lock_wait_seconds_sum (\S+)/.exec(metrics)?.[1] ?? 0);
    lockWaitCount += Number(/wagering_wallet_lock_wait_seconds_count (\S+)/.exec(metrics)?.[1] ?? 0);
  }
  console.log(`hot wallet, espera pelo lock: ${lockWaitCount} travas, média ${((lockWaitSum / Math.max(lockWaitCount, 1)) * 1000).toFixed(1)} ms`);
  expect(lockWaitCount).toBeGreaterThan(0);
});
