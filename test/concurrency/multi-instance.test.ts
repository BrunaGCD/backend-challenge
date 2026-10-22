// ====================================================================================================
//     CONCORRÊNCIA COM 3 INSTÂNCIAS DE VERDADE: MESMO BANCO, MESMAS FILAS, REQUISIÇÕES AO MESMO TEMPO
// ====================================================================================================
// Cenários 1, 2, 3, 4, 6 e 7 da seção 13. Paralelismo real (processos separados), nada de mock.
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { GetQueueAttributesCommand } from "@aws-sdk/client-sqs";
import { createTestQueues, createWallet, drainQueue, holdWalletLock, httpRequest, queueUrl, seededRandom, sendWagerMessage, submitLikeProvider, testEnvironment, testSql, testSqsClient, wagerMessage, waitUntil, type ProviderRetryCounter, type TestQueues, type WagerInput } from "../support/harness";
import { instanceLogLines, spawnInstance, stopInstance, type Instance } from "../support/instances";
import { expectWalletInvariants } from "../support/invariants";

let queues: TestQueues;
let instances: Instance[] = [];
const touchedWallets: string[] = [];
const playerIds: string[] = [];

const pick = (index: number) => instances[index % instances.length]!;                           // reparte entre as 3
const random = seededRandom(42);
const retryCounter: ProviderRetryCounter = { serviceUnavailable: 0, networkErrors: 0 };

// manda para a instância do índice e, se precisar reenviar (503 ou conexão caída), para a seguinte
function send(index: number, input: WagerInput) {
  return submitLikeProvider([pick(index).baseUrl, pick(index + 1).baseUrl], input, random, retryCounter);
}

async function newWallet(initialBalance: string) {
  const wallet = await createWallet(pick(touchedWallets.length).baseUrl, initialBalance);
  touchedWallets.push(wallet.walletId);
  playerIds.push(wallet.playerId);
  return wallet;
}

async function inputQueueIsEmpty(): Promise<boolean> {
  const response = await testSqsClient().send(new GetQueueAttributesCommand({ QueueUrl: await queueUrl(queues.inputQueueName), AttributeNames: ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"] }));
  return Number(response.Attributes?.ApproximateNumberOfMessages) === 0 && Number(response.Attributes?.ApproximateNumberOfMessagesNotVisible) === 0;
}

// sessões deste banco paradas esperando um lock num SELECT ... FOR UPDATE (as instâncias esperando a wallet)
async function sessionsWaitingForUpdateLock(): Promise<number> {
  const [row] = await testSql()`select count(*)::int as total from pg_stat_activity where datname = current_database() and wait_event_type = 'Lock' and query ilike '%for update%'`;
  return row.total;
}

async function ledgerDebits(walletId: string): Promise<number> {
  const [row] = await testSql()`select count(*)::int as total from wallet_ledger_entries where wallet_id = ${walletId} and direction = 'DEBIT'`;
  return row.total;
}

beforeAll(async () => {
  queues = await createTestQueues("multi");
  const environment = testEnvironment(queues, { LOG_LEVEL: "info" });
  instances = await Promise.all(["instance-1", "instance-2", "instance-3"].map((name) => spawnInstance(name, environment)));
});

afterAll(async () => {
  await waitUntil("fila de entrada vazia", inputQueueIsEmpty, 30_000);
  await waitUntil("nenhuma pendente", async () => {
    const [row] = await testSql()`select count(*)::int as total from wager_transactions where status = 'PENDING_REFERENCE'`;
    return row.total === 0;
  }, 30_000);
  await expectWalletInvariants(touchedWallets);

  // logs: JSON, com os ids técnicos, e sem playerId nem valores monetários
  const allOutput = instances.map((instance) => instance.output.join("")).join("");
  for (const playerId of playerIds) expect(allOutput).not.toContain(playerId);
  expect(allOutput).not.toContain('"amount"');
  const decided = instances.flatMap(instanceLogLines).filter((line) => line.event === "wager_transaction_decided");
  expect(decided.length).toBeGreaterThan(0);
  for (const line of decided) {
    expect(line).toHaveProperty("correlationId");
    expect(line).toHaveProperty("transactionId");
    expect(line).toHaveProperty("walletId");
    expect(line).toHaveProperty("providerId");
  }
  console.log(`reenvios do cliente (papel do provedor): ${retryCounter.serviceUnavailable} por 503, ${retryCounter.networkErrors} por conexão caída`);
  await Promise.all(instances.map((instance) => stopInstance(instance, "SIGKILL")));
});

// A concorrência de verdade aqui vem das 40 cópias por HTTP, espalhadas pelas 3 instâncias. As 10 pela fila
// vão no mesmo MessageGroupId (a wallet), então o FIFO as entrega uma por vez: elas disputam com o HTTP,
// mas não entre si.
describe("cenário 1: a mesma aposta enviada 50 vezes em paralelo", () => {
  test("40 cópias por HTTP nas 3 instâncias, ao mesmo tempo, e 10 pela fila, uma por vez: um único débito", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    const bet = { walletId, playerId, kind: "BET", amount: "30.00", externalTransactionId: `bet-${Bun.randomUUIDv7()}` };

    const httpCopies = Array.from({ length: 40 }, (_, index) => send(index, bet));
    const queueCopies = Array.from({ length: 10 }, (_, index) => sendWagerMessage(queues.inputQueueName, wagerMessage(`msg-${index}-${Bun.randomUUIDv7()}`, bet)));
    const [responses] = await Promise.all([Promise.all(httpCopies), Promise.all(queueCopies)]);
    await waitUntil("fila consumida", inputQueueIsEmpty);

    expect(responses.every((response) => response.status === 200)).toBe(true);
    expect(new Set(responses.map((response) => response.body.transactionId)).size).toBe(1);   // todas apontam para a mesma transação
    expect(responses.filter((response) => !response.body.idempotentReplay).length).toBeLessThanOrEqual(1);
    expect(await ledgerDebits(walletId)).toBe(1);
    const wallet = await httpRequest(pick(0).baseUrl, "GET", `/wallets/${walletId}`);
    expect(wallet.body.balance).toEqual({ amount: "70.00", currency: "BRL" });
  });
});

// Para as duas apostas se sobreporem de fato no banco, o teste trava a wallet por fora, dispara as duas, espera
// o PostgreSQL mostrar as duas paradas no lock e só então solta. Sem o lock da wallet, as duas leriam 100.00.
describe("cenário 2: duas apostas de 80.00 disputando 100.00", () => {
  test("em 10 wallets, cada uma com as duas apostas em instâncias diferentes esperando o mesmo lock", async () => {
    for (let round = 0; round < 10; round++) {
      const { walletId, playerId } = await newWallet("100.00");
      const first = { walletId, playerId, kind: "BET", amount: "80.00", externalTransactionId: `bet-a-${Bun.randomUUIDv7()}` };
      const second = { walletId, playerId, kind: "BET", amount: "80.00", externalTransactionId: `bet-b-${Bun.randomUUIDv7()}` };

      const lock = await holdWalletLock(walletId);
      const serviceUnavailableBefore = retryCounter.serviceUnavailable;
      const bothBets = Promise.all([send(round, first), send(round + 1, second)]);
      try {
        await waitUntil("as duas apostas esperando o lock da wallet", async () => (await sessionsWaitingForUpdateLock()) === 2, 1_500, 20);   // solta bem antes dos 2 s de lock_timeout do HTTP
      } finally {
        await lock.release();                                                                   // solta mesmo se a espera falhar, para não deixar a wallet travada
      }
      const responses = await bothBets;
      expect(retryCounter.serviceUnavailable).toBe(serviceUnavailableBefore);                  // nenhuma desistiu do lock: as duas saíram da mesma espera

      const statuses = responses.map((response) => response.body.status).sort();
      expect(statuses).toEqual(["PROCESSED", "REJECTED"]);                                     // exatamente uma de cada
      expect(responses.find((response) => response.body.status === "REJECTED")!.body.failureCode).toBe("INSUFFICIENT_FUNDS");
      expect(await ledgerDebits(walletId)).toBe(1);

      const retries = await Promise.all([send(round + 2, first), send(round, second)]);   // o provedor reenvia as duas
      expect(retries.every((response) => response.body.idempotentReplay)).toBe(true);
      expect(await ledgerDebits(walletId)).toBe(1);                                             // nenhum retry duplica o débito
      const wallet = await httpRequest(pick(round).baseUrl, "GET", `/wallets/${walletId}`);
      expect(wallet.body.balance).toEqual({ amount: "20.00", currency: "BRL" });
    }
  });
});

describe("cenário 3: wallets distintas em paralelo", () => {
  test("20 wallets com 5 apostas cada, tudo ao mesmo tempo nas 3 instâncias", async () => {
    const wallets = [];
    for (let index = 0; index < 20; index++) wallets.push(await newWallet("50.00"));
    const startedAt = performance.now();
    const all = wallets.flatMap((wallet, walletIndex) => Array.from({ length: 5 }, (_, betIndex) => send(walletIndex + betIndex, { ...wallet, kind: "BET", amount: "10.00" })));
    const responses = await Promise.all(all);
    const elapsedSeconds = (performance.now() - startedAt) / 1000;
    expect(responses.every((response) => response.status === 200 && response.body.status === "PROCESSED")).toBe(true);
    for (const wallet of wallets) {
      const current = await httpRequest(pick(0).baseUrl, "GET", `/wallets/${wallet.walletId}`);
      expect(current.body.balance).toEqual({ amount: "0.00", currency: "BRL" });
    }
    console.log(`cenário 3: 100 apostas em 20 wallets em ${elapsedSeconds.toFixed(2)} s`);
  });
});

describe("cenário 4: três instâncias ao mesmo tempo, HTTP e fila misturados", () => {
  test("HTTP em todas as instâncias e mensagens consumidas pelos consumidores das três", async () => {
    const wallets = [];
    for (let index = 0; index < 9; index++) wallets.push(await newWallet("100.00"));
    const httpBets = wallets.flatMap((wallet, walletIndex) => [0, 1, 2].map((instanceIndex) => send(walletIndex + instanceIndex, { ...wallet, kind: "BET", amount: "5.00" })));
    const queueWins = wallets.flatMap((wallet) => [0, 1, 2, 3].map((index) => sendWagerMessage(queues.inputQueueName, wagerMessage(`msg-win-${index}-${Bun.randomUUIDv7()}`, { ...wallet, kind: "WIN", amount: "2.50" }))));
    const [responses] = await Promise.all([Promise.all(httpBets), Promise.all(queueWins)]);
    await waitUntil("fila consumida", inputQueueIsEmpty);
    expect(responses.every((response) => response.status === 200)).toBe(true);
    for (const wallet of wallets) {
      const current = await httpRequest(pick(1).baseUrl, "GET", `/wallets/${wallet.walletId}`);
      expect(current.body.balance).toEqual({ amount: "95.00", currency: "BRL" });             // 100 - 3 × 5 + 4 × 2.50
    }
    const consumedPerInstance = instances.map((instance) => instanceLogLines(instance).filter((line) => line.event === "wager_transaction_decided" && line.channel === "sqs").length);
    console.log(`cenário 4: mensagens decididas por instância: ${consumedPerInstance.join(", ")}`);
    expect(consumedPerInstance.reduce((sum, count) => sum + count, 0)).toBeGreaterThanOrEqual(36);
  });
});

describe("cenário 6: os publishers das três instâncias sobre a mesma outbox", () => {
  test("cada evento da outbox foi publicado uma vez só, contando as publicações de cada instância, e nenhum se perdeu", async () => {
    // o banco é da execução inteira: conta só os eventos das wallets deste arquivo, que vão para estas filas
    await waitUntil("outbox vazia", async () => {
      const [row] = await testSql()`select count(*)::int as total from outbox_messages where published_at is null and aggregate_id in ${testSql()(touchedWallets)}`;
      return row.total === 0;
    }, 30_000);
    const outboxIds: string[] = (await testSql()`select id from outbox_messages where aggregate_id in ${testSql()(touchedWallets)}`).map((row: any) => row.id as string);

    // a fila FIFO descartaria uma cópia com o mesmo eventId, então a contagem vem do que cada publisher
    // registrou como publicação aceita pelo SQS (uma linha de log por evento por tentativa)
    const publicationsPerEvent = new Map<string, number>();
    for (const line of instances.flatMap(instanceLogLines)) {
      if (line.event !== "outbox_event_published") continue;
      const outboxMessageId = line.outboxMessageId as string;
      publicationsPerEvent.set(outboxMessageId, (publicationsPerEvent.get(outboxMessageId) ?? 0) + 1);
    }
    const publishedTwice = outboxIds.filter((id) => (publicationsPerEvent.get(id) ?? 0) > 1);
    const neverPublished = outboxIds.filter((id) => !publicationsPerEvent.has(id));
    const publishersThatWorked = instances.filter((instance) => instanceLogLines(instance).some((line) => line.event === "outbox_event_published")).length;
    console.log(`cenário 6: ${outboxIds.length} eventos na outbox, publicados por ${publishersThatWorked} das 3 instâncias, ${publishedTwice.length} publicados mais de uma vez`);
    expect(publishedTwice).toEqual([]);                                                         // SKIP LOCKED: nenhuma linha pega por dois publishers
    expect(neverPublished).toEqual([]);

    const received = (await drainQueue(queues.eventsQueueName, 2000)).map((message) => JSON.parse(message.body).eventId as string);
    expect(new Set(received)).toEqual(new Set(outboxIds));                                      // nenhum evento confirmado perdido no caminho
  });
});

describe("cenário 7: ROLLBACK e REFUND antes da referência", () => {
  test("a reversão chega antes, por instâncias e canais diferentes, e termina resolvida", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    const betExternalId = `bet-${Bun.randomUUIDv7()}`;
    const winExternalId = `win-${Bun.randomUUIDv7()}`;
    const refund = await send(0, { walletId, playerId, kind: "REFUND", amount: "20.00", reference: betExternalId, externalTransactionId: `refund-${Bun.randomUUIDv7()}` });
    const rollbackOfWin = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "ROLLBACK", amount: "7.00", reference: winExternalId });
    await sendWagerMessage(queues.inputQueueName, rollbackOfWin);
    expect(refund.status).toBe(202);

    await send(1, { walletId, playerId, kind: "BET", amount: "20.00", externalTransactionId: betExternalId });
    await sendWagerMessage(queues.inputQueueName, wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "WIN", amount: "7.00", externalTransactionId: winExternalId }));

    await waitUntil("as duas reversões processadas", async () => {
      const rows = await testSql()`select status from wager_transactions where wallet_id = ${walletId} and kind in ('REFUND','ROLLBACK')`;
      return rows.length === 2 && rows.every((row: any) => row.status === "PROCESSED");
    }, 20_000);
    const wallet = await httpRequest(pick(2).baseUrl, "GET", `/wallets/${walletId}`);
    expect(wallet.body.balance).toEqual({ amount: "100.00", currency: "BRL" });                // tudo desfeito
  });
});
