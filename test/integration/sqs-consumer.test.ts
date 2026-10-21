// ====================================================================================================
//        CONSUMIDOR SQS: INBOX E REDELIVERY, CLASSIFICAÇÃO DE ERROS, REPLAY ENTRE CANAIS
// ====================================================================================================
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { GetQueueAttributesCommand } from "@aws-sdk/client-sqs";
import { createTestQueues, createWallet, drainQueue, httpRequest, queueUrl, sendWagerMessage, startInProcessApp, submitWager, testConfig, testSql, testSqsClient, wagerMessage, waitUntil, type RunningApp, type TestQueues } from "../support/harness";
import { expectWalletInvariants } from "../support/invariants";

let queues: TestQueues;
let app: RunningApp;
const touchedWallets: string[] = [];

async function newWallet(initialBalance = "100.00") {
  const wallet = await createWallet(app.baseUrl, initialBalance);
  touchedWallets.push(wallet.walletId);
  return wallet;
}

async function transactionByExternalId(externalTransactionId: string) {
  const [row] = await testSql()`select id, status, failure_code from wager_transactions where provider_id = 'provider-a' and external_transaction_id = ${externalTransactionId}`;
  return row;
}

async function messagesInQueue(queueName: string): Promise<number> {
  const response = await testSqsClient().send(new GetQueueAttributesCommand({ QueueUrl: await queueUrl(queueName), AttributeNames: ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"] }));
  return Number(response.Attributes?.ApproximateNumberOfMessages ?? 0) + Number(response.Attributes?.ApproximateNumberOfMessagesNotVisible ?? 0);
}

async function metricValue(name: string, labels = ""): Promise<number> {
  const metrics = await httpRequest(app.baseUrl, "GET", "/metrics");
  const line = (metrics.body as string).split("\n").find((candidate) => candidate.startsWith(`${name}${labels}`) && !candidate.startsWith("#"));
  return line ? Number(line.split(" ").at(-1)) : 0;
}

beforeAll(async () => {
  queues = await createTestQueues("sqs");
  app = await startInProcessApp(testConfig(queues));
});

afterAll(async () => {
  await expectWalletInvariants(touchedWallets);
  await app.stop();
});

describe("caminho feliz e ack depois do commit", () => {
  test("mensagem válida: processada uma vez, inbox gravada, mensagem apagada da fila", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    const message = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "30.00" });
    await sendWagerMessage(queues.inputQueueName, message);

    const transaction = await waitUntil("transação gravada", () => transactionByExternalId(message.data.externalTransactionId as string));
    expect(transaction.status).toBe("PROCESSED");
    const [inbox] = await testSql()`select payload_hash, processed_at from inbox_messages where message_id = ${message.messageId}`;
    expect(inbox.processed_at).not.toBeNull();
    await waitUntil("mensagem apagada da fila", async () => (await messagesInQueue(queues.inputQueueName)) === 0);
    const [wallet] = await testSql()`select balance from wallets where id = ${walletId}`;
    expect(wallet.balance).toBe("70.00");
  });

  test("rejeição de negócio pela fila: grava REJECTED e confirma a mensagem (não vai para a DLQ)", async () => {
    const { walletId, playerId } = await newWallet("10.00");
    const message = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "99.00" });
    await sendWagerMessage(queues.inputQueueName, message);
    const transaction = await waitUntil("rejeição gravada", () => transactionByExternalId(message.data.externalTransactionId as string));
    expect(transaction).toMatchObject({ status: "REJECTED", failure_code: "INSUFFICIENT_FUNDS" });
    await waitUntil("mensagem apagada da fila", async () => (await messagesInQueue(queues.inputQueueName)) === 0);
  });
});

describe("inbox e redelivery", () => {
  test("a mesma mensagem entregue duas vezes não duplica o efeito", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    const message = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "10.00" });
    await sendWagerMessage(queues.inputQueueName, message, { deduplicationId: `${message.messageId}-a` });   // dedup do broker diferente: as duas chegam
    await sendWagerMessage(queues.inputQueueName, message, { deduplicationId: `${message.messageId}-b` });
    await waitUntil("as duas mensagens consumidas", async () => (await messagesInQueue(queues.inputQueueName)) === 0);
    const [counts] = await testSql()`select count(*)::int as total from wager_transactions where external_transaction_id = ${message.data.externalTransactionId}`;
    expect(counts.total).toBe(1);
    const [wallet] = await testSql()`select balance from wallets where id = ${walletId}`;
    expect(wallet.balance).toBe("90.00");
  });

  test("mesmo messageId e mesmo conteúdo, chegando com outra key: a inbox reconhece e confirma sem efeito", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    const message = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "10.00" });
    await sendWagerMessage(queues.inputQueueName, message);
    await waitUntil("primeira processada", () => transactionByExternalId(message.data.externalTransactionId as string));
    const inboxDuplicatesBefore = await metricValue("wagering_duplicates_total", '{source="inbox"}');

    const sameMessageOtherKey = { ...message, data: { ...message.data, idempotencyKey: `outra-key-${Bun.randomUUIDv7()}` } };   // a key não entra no hash
    await sendWagerMessage(queues.inputQueueName, sameMessageOtherKey, { deduplicationId: `${message.messageId}-redelivery` });
    await waitUntil("inbox contar a duplicata", async () => (await metricValue("wagering_duplicates_total", '{source="inbox"}')) === inboxDuplicatesBefore + 1);
    await waitUntil("mensagem confirmada", async () => (await messagesInQueue(queues.inputQueueName)) === 0);
    const [wallet] = await testSql()`select balance from wallets where id = ${walletId}`;
    expect(wallet.balance).toBe("90.00");
  });

  test("mesmo messageId com conteúdo diferente: DLQ com o motivo, métrica, e nada gravado", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    const messageId = `msg-${Bun.randomUUIDv7()}`;
    const original = wagerMessage(messageId, { walletId, playerId, kind: "BET", amount: "10.00" });
    await sendWagerMessage(queues.inputQueueName, original);
    await waitUntil("original processada", () => transactionByExternalId(original.data.externalTransactionId as string));
    const conflictsBefore = await metricValue("wagering_inbox_payload_conflicts_total");

    const reusedId = wagerMessage(messageId, { walletId, playerId, kind: "BET", amount: "20.00" });   // produtor reutilizou o messageId
    await sendWagerMessage(queues.inputQueueName, reusedId, { deduplicationId: `${messageId}-reused` });
    const deadLetters = await waitUntil("mensagem na DLQ", async () => {
      const received = await drainQueue(queues.deadLetterQueueName, 300);
      return received.length > 0 ? received : undefined;
    });
    expect(deadLetters[0]!.attributes.reason).toBe("INBOX_PAYLOAD_CONFLICT");
    expect(JSON.parse(deadLetters[0]!.body).data.externalTransactionId).toBe(reusedId.data.externalTransactionId);
    expect(await metricValue("wagering_inbox_payload_conflicts_total")).toBe(conflictsBefore + 1);
    expect(await transactionByExternalId(reusedId.data.externalTransactionId as string)).toBeUndefined();
  });

  test("a mesma operação por HTTP e depois pela fila é replay (o hash não depende do canal)", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    const externalTransactionId = `bet-${Bun.randomUUIDv7()}`;
    const viaHttp = await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "15.00", externalTransactionId });
    expect(viaHttp.status).toBe(200);
    const message = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "15.00", externalTransactionId });
    await sendWagerMessage(queues.inputQueueName, message);
    await waitUntil("mensagem confirmada", async () => (await messagesInQueue(queues.inputQueueName)) === 0);
    const [counts] = await testSql()`select count(*)::int as total from wager_transactions where external_transaction_id = ${externalTransactionId}`;
    expect(counts.total).toBe(1);
    expect(await drainQueue(queues.deadLetterQueueName, 300)).toHaveLength(0);
  });
});

describe("erros permanentes vão para a DLQ explicitamente, sem esperar o maxReceiveCount", () => {
  test.each([
    ["JSON inválido", () => ({ body: "{nao-e-json", groupId: "g-json" }), "INVALID_JSON"],
    ["type desconhecido", () => ({ body: JSON.stringify({ messageId: `m-${Bun.randomUUIDv7()}`, type: "Outro", occurredAt: new Date().toISOString(), data: {} }), groupId: "g-type" }), "UNKNOWN_TYPE"],
  ])("%s", async (_name, build, reason) => {
    const { body, groupId } = build();
    await sendWagerMessage(queues.inputQueueName, {}, { body, groupId, deduplicationId: Bun.randomUUIDv7() });
    const deadLetters = await waitUntil("mensagem na DLQ", async () => {
      const received = await drainQueue(queues.deadLetterQueueName, 300);
      return received.length > 0 ? received : undefined;
    });
    expect(deadLetters).toHaveLength(1);
    expect(deadLetters[0]!.attributes.reason).toBe(reason);
    expect(deadLetters[0]!.body).toBe(body);                                                    // o corpo original, intacto
  });

  test("kind OPENING, payload inválido e wallet inexistente", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    const opening = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "OPENING", amount: "10.00" });
    const invalidAmount = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "10.5" });
    const missingWalletId = Bun.randomUUIDv7();
    const missingWallet = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId: missingWalletId, playerId, kind: "BET", amount: "1.00" });
    for (const message of [opening, invalidAmount, missingWallet]) await sendWagerMessage(queues.inputQueueName, message);

    const reasons: string[] = [];
    await waitUntil("três mensagens na DLQ", async () => {
      reasons.push(...(await drainQueue(queues.deadLetterQueueName, 300)).map((message) => message.attributes.reason ?? ""));
      return reasons.length >= 3 ? true : undefined;
    });
    expect(reasons.sort()).toEqual(["VALIDATION_ERROR", "VALIDATION_ERROR", "WALLET_NOT_FOUND"]);
    const [counts] = await testSql()`select count(*)::int as total from wager_transactions where wallet_id = ${walletId}`;
    expect(counts.total).toBe(1);                                                               // só o OPENING de verdade, da criação
  });

  test("conflito de idempotência pela fila", async () => {
    const { walletId, playerId } = await newWallet("100.00");
    const externalTransactionId = `bet-${Bun.randomUUIDv7()}`;
    await submitWager(app.baseUrl, { walletId, playerId, kind: "BET", amount: "1.00", externalTransactionId }, "fila-conflito");
    const conflicting = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "2.00", externalTransactionId }, "fila-conflito");
    await sendWagerMessage(queues.inputQueueName, conflicting);
    const deadLetters = await waitUntil("mensagem na DLQ", async () => {
      const received = await drainQueue(queues.deadLetterQueueName, 300);
      return received.length > 0 ? received : undefined;
    });
    expect(deadLetters[0]!.attributes.reason).toBe("IDEMPOTENCY_KEY_CONFLICT");
  });
});
