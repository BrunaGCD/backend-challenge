// ====================================================================================================
//    CRASH E REINÍCIO: MORTO DEPOIS DO COMMIT E ANTES DO ACK, SIGKILL NO MEIO DA CARGA, E SIGTERM
// ====================================================================================================
// Cenários 5 e 8 da seção 13, com processos de verdade e SIGKILL de verdade.
import { afterAll, describe, expect, test } from "bun:test";
import { GetQueueAttributesCommand } from "@aws-sdk/client-sqs";
import { createTestQueues, createWallet, drainQueue, httpRequest, queueUrl, seededRandom, sendWagerMessage, submitWager, testEnvironment, testSql, testSqsClient, wagerMessage, waitUntil, type HttpResult, type TestQueues, type WagerInput } from "../support/harness";
import { instanceLogLines, spawnInstance, stopInstance, type Instance } from "../support/instances";
import { expectWalletInvariants } from "../support/invariants";

const running: Instance[] = [];
const touchedWallets: string[] = [];

async function queueIsEmpty(queueName: string): Promise<boolean> {
  const response = await testSqsClient().send(new GetQueueAttributesCommand({ QueueUrl: await queueUrl(queueName), AttributeNames: ["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"] }));
  return Number(response.Attributes?.ApproximateNumberOfMessages) === 0 && Number(response.Attributes?.ApproximateNumberOfMessagesNotVisible) === 0;
}

async function start(name: string, queues: TestQueues, entrypoint?: string, extra: Record<string, string> = {}): Promise<Instance> {
  const instance = await spawnInstance(name, testEnvironment(queues, { LOG_LEVEL: "info", ...extra }), entrypoint);
  running.push(instance);
  return instance;
}

async function metricValue(instance: Instance, name: string, labels: string): Promise<number> {
  const metrics = await httpRequest(instance.baseUrl, "GET", "/metrics");
  const line = (metrics.body as string).split("\n").find((candidate) => candidate.startsWith(`${name}${labels}`));
  return line ? Number(line.split(" ").at(-1)) : 0;
}

afterAll(async () => {
  await expectWalletInvariants(touchedWallets);
  await Promise.all(running.map((instance) => stopInstance(instance, "SIGKILL")));
});

describe("cenário 5: worker morto depois do commit e antes do ack", () => {
  test("a mensagem volta, a outra instância reconhece a operação e confirma sem efeito duplicado", async () => {
    const queues = await createTestQueues("crash");
    const crashing = await start("crash-before-ack", queues, "test/support/crash-after-commit-main.ts");
    const { walletId, playerId } = await createWallet(crashing.baseUrl, "100.00");
    touchedWallets.push(walletId);

    const message = wagerMessage(`msg-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "30.00" });
    await sendWagerMessage(queues.inputQueueName, message);
    await crashing.exited;                                                                      // o processo morreu sozinho no delete
    expect(crashing.output.join("")).toContain("crash_before_ack");

    const [committed] = await testSql()`select status from wager_transactions where external_transaction_id = ${message.data.externalTransactionId}`;
    expect(committed.status).toBe("PROCESSED");                                                 // o commit aconteceu antes de morrer
    const [inbox] = await testSql()`select count(*)::int as total from inbox_messages where message_id = ${message.messageId}`;
    expect(inbox.total).toBe(1);
    expect(await queueIsEmpty(queues.inputQueueName)).toBe(false);                              // sem DeleteMessage: a mensagem continua lá

    const survivor = await start("survivor", queues);
    await waitUntil("a mensagem reentregue ser confirmada", () => queueIsEmpty(queues.inputQueueName), 30_000);
    expect(await metricValue(survivor, "wagering_duplicates_total", '{source="idempotency_key"}')).toBeGreaterThanOrEqual(1);
    const [counts] = await testSql()`select
        (select count(*)::int from wager_transactions where external_transaction_id = ${message.data.externalTransactionId}) as transactions,
        (select count(*)::int from wallet_ledger_entries where wallet_id = ${walletId} and direction = 'DEBIT') as debits,
        (select balance from wallets where id = ${walletId}) as balance`;
    expect(counts).toEqual({ transactions: 1, debits: 1, balance: "70.00" });
    expect(await drainQueue(queues.deadLetterQueueName, 500)).toHaveLength(0);
    await stopInstance(survivor);
  });
});

describe("cenário 8: reinício no meio da carga, com consistência final comprovada", () => {
  test("SIGKILL numa das 3 instâncias durante a carga, a instância volta, e nada se perde nem duplica", async () => {
    const queues = await createTestQueues("restart");
    const instances = [await start("load-1", queues), await start("load-2", queues), await start("load-3", queues)];
    const wallets = [];
    for (let index = 0; index < 5; index++) {
      const wallet = await createWallet(instances[0]!.baseUrl, "1000.00");
      wallets.push(wallet);
      touchedWallets.push(wallet.walletId);
    }

    const random = seededRandom(42);
    const sent: { input: WagerInput; channel: "http" | "sqs" }[] = [];
    let killed = false;

    // o provedor reenvia com a mesma key quando a instância não responde: é seguro por causa da idempotência
    async function sendOverHttp(input: WagerInput, attempt = 0): Promise<HttpResult> {
      const target = instances[(sent.length + attempt) % instances.length]!;
      try {
        const response = await submitWager(target.baseUrl, input);
        if (response.status >= 500) throw new Error(`status ${response.status}`);
        return response;
      } catch (error) {
        if (attempt > 20) throw error;
        await Bun.sleep(50);
        return sendOverHttp(input, attempt + 1);
      }
    }

    const pendingHttp: Promise<HttpResult>[] = [];
    for (let index = 0; index < 150; index++) {
      const wallet = wallets[Math.floor(random() * wallets.length)]!;
      const kind = random() < 0.7 ? "BET" : "WIN";
      const amount = `${1 + Math.floor(random() * 5)}.${String(Math.floor(random() * 100)).padStart(2, "0")}`;
      const input: WagerInput = { ...wallet, kind, amount, externalTransactionId: `load-${index}-${Bun.randomUUIDv7()}` };
      const channel = random() < 0.5 ? "http" : "sqs";
      sent.push({ input, channel });
      if (channel === "http") pendingHttp.push(sendOverHttp(input));
      else await sendWagerMessage(queues.inputQueueName, wagerMessage(`msg-${index}-${Bun.randomUUIDv7()}`, input));

      if (index === 60 && !killed) {
        killed = true;
        await stopInstance(instances[1]!, "SIGKILL");                                           // morre no meio do trabalho
        instances[1] = await start("load-2-restarted", queues);                                 // e volta
      }
      if (index % 10 === 0) await Bun.sleep(20);                                                // espalha a carga no tempo
    }
    await Promise.all(pendingHttp);

    await waitUntil("fila consumida", () => queueIsEmpty(queues.inputQueueName), 60_000);
    await waitUntil("outbox publicada", async () => {
      const [row] = await testSql()`select count(*)::int as total from outbox_messages where published_at is null`;
      return row.total === 0;
    }, 30_000);

    // cada operação enviada virou exatamente uma transação; o saldo esperado sai das que terminaram PROCESSED
    for (const wallet of wallets) {
      const rows = await testSql()`select external_transaction_id, kind, status, amount from wager_transactions where wallet_id = ${wallet.walletId} and kind <> 'OPENING'`;
      const sentForWallet = sent.filter((operation) => operation.input.walletId === wallet.walletId);
      expect(rows.length).toBe(sentForWallet.length);
      let expectedCents = 1000_00n;
      for (const row of rows) {
        if (row.status !== "PROCESSED") continue;
        const cents = BigInt(row.amount.replace(".", ""));
        expectedCents = row.kind === "BET" ? expectedCents - cents : expectedCents + cents;
      }
      const current = await httpRequest(instances[0]!.baseUrl, "GET", `/wallets/${wallet.walletId}`);
      const expectedAmount = `${expectedCents / 100n}.${String(expectedCents % 100n).padStart(2, "0")}`;
      expect(current.body.balance.amount).toBe(expectedAmount);
    }
    const events = (await drainQueue(queues.eventsQueueName, 2000)).map((message) => JSON.parse(message.body).eventId as string);
    expect(new Set(events).size).toBe(events.length);                                          // nenhum evento duplicado
    for (const instance of instances) await stopInstance(instance);
  });
});

describe("SIGTERM: desligamento gracioso", () => {
  // O Windows não entrega SIGTERM (E1, item 9): lá o processo morre na hora, sem rodar os hooks.
  test.skipIf(process.platform === "win32")("para o consumidor, termina o que está em andamento, fecha o HTTP e o pool, e nada se perde", async () => {
    const queues = await createTestQueues("sigterm");
    const leaving = await start("leaving", queues);
    const { walletId, playerId } = await createWallet(leaving.baseUrl, "100.00");
    touchedWallets.push(walletId);
    const messages = Array.from({ length: 12 }, (_, index) => wagerMessage(`msg-${index}-${Bun.randomUUIDv7()}`, { walletId, playerId, kind: "BET", amount: "1.00" }));
    for (const message of messages) await sendWagerMessage(queues.inputQueueName, message);

    leaving.process.kill("SIGTERM");
    await leaving.exited;
    const events = instanceLogLines(leaving).map((line) => line.event);
    expect(events).toContain("shutdown_started");
    expect(events.indexOf("shutdown_finished")).toBeGreaterThan(events.indexOf("shutdown_started"));

    const staying = await start("staying", queues);
    await waitUntil("o resto da fila consumido por outra instância", () => queueIsEmpty(queues.inputQueueName), 30_000);
    const [counts] = await testSql()`select count(*)::int as total from wager_transactions where wallet_id = ${walletId} and kind = 'BET'`;
    expect(counts.total).toBe(12);                                                              // cada mensagem uma vez
    await stopInstance(staying);
  });
});
