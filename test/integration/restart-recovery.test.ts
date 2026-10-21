// ====================================================================================================
//       RECUPERAÇÃO APÓS REINICIALIZAÇÃO: O QUE FICOU PELO CAMINHO É TERMINADO PELA NOVA INSTÂNCIA
// ====================================================================================================
// Com a aplicação fora: mensagens chegam na fila, uma pendente espera a referência e eventos ficam na
// outbox sem publicar. Uma instância nova, sem nada em memória, termina tudo sem perder nem duplicar.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { createTestQueues, createWallet, drainQueue, httpRequest, sendWagerMessage, startInProcessApp, submitWager, testConfig, testSql, wagerMessage, waitUntil, type RunningApp, type TestQueues } from "../support/harness";
import { expectWalletInvariants } from "../support/invariants";

let queues: TestQueues;
let restarted: RunningApp | undefined;
const touchedWallets: string[] = [];

beforeAll(async () => {
  queues = await createTestQueues("restart");
});

afterAll(async () => {
  await expectWalletInvariants(touchedWallets);
  await restarted?.stop();
});

test("depois de reiniciar: fila consumida, pendente resolvida, outbox publicada, saldo consistente", async () => {
  const firstLife = await startInProcessApp(testConfig(queues, { OUTBOX_PUBLISHER_ENABLED: "false", PENDING_REFERENCE_TTL_MS: "60000" }));
  const { walletId, playerId } = await createWallet(firstLife.baseUrl, "100.00");
  touchedWallets.push(walletId);
  const betExternalId = `bet-${Bun.randomUUIDv7()}`;
  const refund = await submitWager(firstLife.baseUrl, { walletId, playerId, kind: "REFUND", amount: "40.00", reference: betExternalId, externalTransactionId: `refund-${Bun.randomUUIDv7()}` });
  expect(refund.status).toBe(202);
  await firstLife.stop();                                                                       // a aplicação sai do ar

  const betMessage = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "40.00", externalTransactionId: betExternalId });
  const winMessage = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "WIN", amount: "15.00" });
  await sendWagerMessage(queues.inputQueueName, betMessage);                                    // chegam enquanto ninguém consome
  await sendWagerMessage(queues.inputQueueName, winMessage);
  const [unpublishedWhileDown] = await testSql()`select count(*)::int as total from outbox_messages where aggregate_id = ${walletId} and published_at is null`;
  expect(unpublishedWhileDown.total).toBe(3);                                                   // abertura (2) e a entrada em espera (1)

  restarted = await startInProcessApp(testConfig(queues, { PENDING_REFERENCE_TTL_MS: "60000" }));   // instância nova, nada em memória

  const refundAfterRestart = await waitUntil("pendente resolvida pelo worker", async () => {
    const transaction = await httpRequest(restarted!.baseUrl, "GET", `/wagering/transactions/${refund.body.transactionId}`);
    return transaction.body.status === "PROCESSED" ? transaction.body : undefined;
  });
  expect(refundAfterRestart.kind).toBe("REFUND");
  await waitUntil("WIN da fila processado", async () => {
    const transaction = await httpRequest(restarted!.baseUrl, "GET", `/providers/provider-a/wagering/transactions/${winMessage.data.externalTransactionId}`);
    return transaction.body.status === "PROCESSED";
  });
  await waitUntil("outbox toda publicada", async () => {
    const [row] = await testSql()`select count(*)::int as total from outbox_messages where aggregate_id = ${walletId} and published_at is null`;
    return row.total === 0;
  });

  const wallet = await httpRequest(restarted.baseUrl, "GET", `/wallets/${walletId}`);
  expect(wallet.body.balance).toEqual({ amount: "115.00", currency: "BRL" });                  // 100 - 40 + 40 + 15
  const events = (await drainQueue(queues.eventsQueueName)).map((message) => JSON.parse(message.body)).filter((event) => event.aggregateId === walletId);
  expect(new Set(events.map((event) => event.eventId)).size).toBe(events.length);              // nenhum evento duplicado
  expect(events.filter((event) => event.eventType === "WalletBalanceChanged").map((event) => event.data.walletVersion).sort()).toEqual([1, 2, 3, 4]);
});
