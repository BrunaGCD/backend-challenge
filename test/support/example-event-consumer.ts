// ====================================================================================================
//       CONSUMIDOR DE EVENTOS DE EXEMPLO (SÓ DE TESTE): DEDUPLICA PELO eventId NO COMMIT DO EFEITO
// ====================================================================================================
// O projeto não tem consumidor dos eventos de integração; este mostra o contrato do ARCHITECTURE §14
// funcionando. O efeito escolhido não é idempotente por natureza (somar o valor movimentado por
// wallet), então uma cópia aplicada duas vezes apareceria na soma. A deduplicação é a mesma ideia da
// inbox da aplicação: INSERT do eventId com ON CONFLICT DO NOTHING, na mesma transação do efeito.
import { DeleteMessageCommand, ReceiveMessageCommand } from "@aws-sdk/client-sqs";              // leitura real da fila de eventos
import { queueUrl, testSql, testSqsClient } from "./harness";

export interface ConsumeStats {
  received: number;                                                                             // mensagens recebidas da fila, com as cópias
  applied: number;                                                                              // eventos que tiveram efeito
  duplicates: number;                                                                           // cópias reconhecidas pelo eventId e ignoradas
}

export async function createExampleConsumerTables(): Promise<void> {
  await testSql()`create table if not exists example_consumer_processed_events (event_id uuid primary key, processed_at timestamptz not null default now())`;
  await testSql()`create table if not exists example_consumer_moved_totals (wallet_id uuid primary key, moved numeric(18,2) not null)`;
}

// recebe até ficar um tempo sem chegar nada; apaga cada mensagem só depois do commit do efeito
export async function consumeEventsUntilIdle(queueName: string, quietMilliseconds = 1500): Promise<ConsumeStats> {
  const url = await queueUrl(queueName);
  const stats: ConsumeStats = { received: 0, applied: 0, duplicates: 0 };
  let quietSince = Date.now();
  while (Date.now() - quietSince < quietMilliseconds) {
    const response = await testSqsClient().send(new ReceiveMessageCommand({ QueueUrl: url, MaxNumberOfMessages: 10, WaitTimeSeconds: 0 }));
    const messages = response.Messages ?? [];
    if (messages.length === 0) {
      await Bun.sleep(100);
      continue;
    }
    for (const message of messages) {
      stats.received = stats.received + 1;
      const wasApplied = await applyOnce(JSON.parse(message.Body ?? "{}"));
      if (wasApplied) stats.applied = stats.applied + 1;
      else stats.duplicates = stats.duplicates + 1;
      await testSqsClient().send(new DeleteMessageCommand({ QueueUrl: url, ReceiptHandle: message.ReceiptHandle }));   // ack depois do commit
    }
    quietSince = Date.now();
  }
  return stats;
}

async function applyOnce(event: { eventId: string; eventType: string; data: { walletId?: string; money?: { amount: string } } }): Promise<boolean> {
  return testSql().begin(async (transaction) => {
    const firstTime = await transaction`insert into example_consumer_processed_events (event_id) values (${event.eventId}) on conflict do nothing returning event_id`;
    if (firstTime.length === 0) return false;                                                   // eventId já aplicado: a cópia não tem efeito
    if (event.eventType === "WalletBalanceChanged") {
      await transaction`insert into example_consumer_moved_totals (wallet_id, moved) values (${event.data.walletId}, ${event.data.money!.amount}::numeric)
                        on conflict (wallet_id) do update set moved = example_consumer_moved_totals.moved + excluded.moved`;   // soma: aplicar duas vezes dobraria
    }
    return true;
  });
}

export async function movedTotal(walletId: string): Promise<string | undefined> {
  const [row] = await testSql()`select moved::text as moved from example_consumer_moved_totals where wallet_id = ${walletId}`;
  return row?.moved;
}
