// ====================================================================================================
//                         E2: FIDELIDADE DO MINISTACK AO SQS FIFO (9 ITENS)
// ====================================================================================================
// Roda com: bun e2.ts   (precisa do ministack do docker compose em localhost:4566)
import {
  SQSClient, CreateQueueCommand, GetQueueAttributesCommand, SendMessageCommand, SendMessageBatchCommand,
  ReceiveMessageCommand, DeleteMessageCommand, ChangeMessageVisibilityCommand,
} from "@aws-sdk/client-sqs";                                                                   // SDK oficial da AWS

const sqs = new SQSClient({
  endpoint: process.env.SQS_ENDPOINT ?? "http://localhost:4566",                                // emulador
  region: "us-east-1",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },                                // credenciais fictícias do emulador
});

const results: { item: string; passed: boolean; detail: string }[] = [];
function record(item: string, passed: boolean, detail: string) {
  results.push({ item, passed, detail });
  console.log(`[${passed ? "PASSOU" : "FALHOU"}] ${item}: ${detail}`);
}

const runSuffix = Date.now().toString(36);                                                      // nomes únicos por execução
async function createQueuePair(name: string, visibilitySeconds: number, maxReceiveCount: number) {
  const dlq = await sqs.send(new CreateQueueCommand({ QueueName: `${name}-dlq-${runSuffix}.fifo`, Attributes: { FifoQueue: "true", ContentBasedDeduplication: "false" } }));
  const dlqAttributes = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: dlq.QueueUrl, AttributeNames: ["QueueArn"] }));
  const main = await sqs.send(new CreateQueueCommand({
    QueueName: `${name}-${runSuffix}.fifo`,
    Attributes: {
      FifoQueue: "true",
      ContentBasedDeduplication: "false",
      VisibilityTimeout: String(visibilitySeconds),
      RedrivePolicy: JSON.stringify({ deadLetterTargetArn: dlqAttributes.Attributes!.QueueArn, maxReceiveCount }),
    },
  }));
  return { mainUrl: main.QueueUrl!, dlqUrl: dlq.QueueUrl! };
}

async function send(queueUrl: string, body: string, groupId: string, deduplicationId: string) {
  return sqs.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: body, MessageGroupId: groupId, MessageDeduplicationId: deduplicationId }));
}

async function receive(queueUrl: string, maxMessages: number, waitSeconds = 0) {
  const response = await sqs.send(new ReceiveMessageCommand({ QueueUrl: queueUrl, MaxNumberOfMessages: maxMessages, WaitTimeSeconds: waitSeconds, MessageSystemAttributeNames: ["ApproximateReceiveCount", "MessageGroupId"] }));
  return response.Messages ?? [];
}

// ----------------------------------------------------------------------------------------------------
//                       ITEM 1: FILAS FIFO COM REDRIVEPOLICY E MAXRECEIVECOUNT = 2
// ----------------------------------------------------------------------------------------------------
const { mainUrl, dlqUrl } = await createQueuePair("wager-transactions", 2, 2);
const createdAttributes = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: mainUrl, AttributeNames: ["All"] }));
record("1 cria filas FIFO com RedrivePolicy", createdAttributes.Attributes?.FifoQueue === "true" && JSON.parse(createdAttributes.Attributes?.RedrivePolicy ?? "{}").maxReceiveCount == 2, `atributos: ${JSON.stringify(createdAttributes.Attributes)}`);

// ----------------------------------------------------------------------------------------------------
//                ITEM 2: NO MESMO GRUPO, A SEGUNDA NÃO VEM ENQUANTO A PRIMEIRA ESTÁ EM VOO
// ----------------------------------------------------------------------------------------------------
{
  const queues = await createQueuePair("e2-item2", 30, 5);
  await send(queues.mainUrl, "group-a-1", "group-a", "a-1");
  await send(queues.mainUrl, "group-a-2", "group-a", "a-2");
  const firstBatch = await receive(queues.mainUrl, 1);
  const secondBatch = await receive(queues.mainUrl, 10);                                        // a primeira continua invisível e não foi apagada
  await sqs.send(new DeleteMessageCommand({ QueueUrl: queues.mainUrl, ReceiptHandle: firstBatch[0]!.ReceiptHandle }));
  const afterDelete = await receive(queues.mainUrl, 10);
  record("2 um por vez no mesmo grupo", firstBatch[0]?.Body === "group-a-1" && secondBatch.length === 0 && afterDelete[0]?.Body === "group-a-2", `primeiro: ${firstBatch.map((m) => m.Body)}, enquanto em voo: ${JSON.stringify(secondBatch.map((m) => m.Body))}, depois do delete: ${afterDelete.map((m) => m.Body)}`);
}

// ----------------------------------------------------------------------------------------------------
//                         ITEM 3: GRUPOS DIFERENTES SAEM PARA DOIS CONSUMIDORES
// ----------------------------------------------------------------------------------------------------
{
  const queues = await createQueuePair("e2-item3", 30, 5);
  await send(queues.mainUrl, "group-x-1", "group-x", "x-1");
  await send(queues.mainUrl, "group-y-1", "group-y", "y-1");
  const [consumerOne, consumerTwo] = await Promise.all([receive(queues.mainUrl, 1), receive(queues.mainUrl, 1)]);
  const bodies = [...consumerOne, ...consumerTwo].map((m) => m.Body).sort();
  record("3 grupos diferentes ao mesmo tempo", bodies.length === 2 && bodies[0] === "group-x-1" && bodies[1] === "group-y-1", `consumidor 1: ${consumerOne.map((m) => m.Body)}, consumidor 2: ${consumerTwo.map((m) => m.Body)}`);
}

// ----------------------------------------------------------------------------------------------------
//                       ITEM 4: MESMO MESSAGEDEDUPLICATIONID DENTRO DE 5 MINUTOS
// ----------------------------------------------------------------------------------------------------
{
  // caso do projeto: a cópia tem o mesmo corpo (evento republicado, mensagem reenviada à DLQ)
  const queues = await createQueuePair("e2-item4", 30, 5);
  await send(queues.mainUrl, "same-body", "group-d", "same-dedup-id");
  await send(queues.mainUrl, "same-body", "group-d", "same-dedup-id");
  const firstBatch = await receive(queues.mainUrl, 10);
  for (const message of firstBatch) await sqs.send(new DeleteMessageCommand({ QueueUrl: queues.mainUrl, ReceiptHandle: message.ReceiptHandle }));
  const secondBatch = await receive(queues.mainUrl, 10);
  record("4a deduplicação, cópia com o mesmo corpo", firstBatch.length === 1 && secondBatch.length === 0, `primeiro recebimento: ${JSON.stringify(firstBatch.map((m) => m.Body))}, depois de apagar: ${JSON.stringify(secondBatch.map((m) => m.Body))}`);

  // cópia com corpo diferente: o MiniStack devolve o MD5 do corpo original e o SDK, com a checagem ligada, lança erro
  const otherQueues = await createQueuePair("e2-item4b", 30, 5);
  await send(otherQueues.mainUrl, "copy-1", "group-d", "same-dedup-id");
  let checksumError = "nenhum";
  try { await send(otherQueues.mainUrl, "copy-2", "group-d", "same-dedup-id"); } catch (error) { checksumError = String(error); }
  const sqsWithoutMd5 = new SQSClient({ endpoint: process.env.SQS_ENDPOINT ?? "http://localhost:4566", region: "us-east-1", credentials: { accessKeyId: "test", secretAccessKey: "test" }, md5: false });
  await sqsWithoutMd5.send(new SendMessageCommand({ QueueUrl: otherQueues.mainUrl, MessageBody: "copy-3", MessageGroupId: "group-d", MessageDeduplicationId: "same-dedup-id" }));
  const delivered = await receive(otherQueues.mainUrl, 10);
  record("4b deduplicação, cópia com corpo diferente", delivered.length === 1 && delivered[0]?.Body === "copy-1", `entregues: ${JSON.stringify(delivered.map((m) => m.Body))}; erro do SDK com md5 ligado: ${checksumError}`);
}

// ----------------------------------------------------------------------------------------------------
//                      ITEM 5: CHANGEMESSAGEVISIBILITY PARA 0 E PARA N SEGUNDOS
// ----------------------------------------------------------------------------------------------------
{
  const queues = await createQueuePair("e2-item5", 30, 10);
  await send(queues.mainUrl, "visibility", "group-v", "v-1");
  const first = await receive(queues.mainUrl, 1);
  await sqs.send(new ChangeMessageVisibilityCommand({ QueueUrl: queues.mainUrl, ReceiptHandle: first[0]!.ReceiptHandle, VisibilityTimeout: 0 }));
  const rightAfterZero = await receive(queues.mainUrl, 1);                                      // tem que voltar na hora
  await sqs.send(new ChangeMessageVisibilityCommand({ QueueUrl: queues.mainUrl, ReceiptHandle: rightAfterZero[0]!.ReceiptHandle, VisibilityTimeout: 3 }));
  const beforeDelay = await receive(queues.mainUrl, 1);                                         // ainda invisível
  await Bun.sleep(3500);
  const afterDelay = await receive(queues.mainUrl, 1);                                          // voltou depois dos 3 s
  record("5 ChangeMessageVisibility 0 e N", rightAfterZero.length === 1 && beforeDelay.length === 0 && afterDelay.length === 1, `depois do 0: ${rightAfterZero.length}, antes dos 3 s: ${beforeDelay.length}, depois dos 3 s: ${afterDelay.length}`);
}

// ----------------------------------------------------------------------------------------------------
//                     ITEM 6: APPROXIMATERECEIVECOUNT SOBE E O REDRIVE LEVA PARA A DLQ
// ----------------------------------------------------------------------------------------------------
{
  await send(mainUrl, "poison", "group-p", "p-1");
  const receiveCounts: string[] = [];
  for (let attempt = 1; attempt <= 2; attempt++) {
    const messages = await receive(mainUrl, 1, 5);
    receiveCounts.push(messages[0]?.Attributes?.ApproximateReceiveCount ?? "(nada)");
    if (messages[0]) await sqs.send(new ChangeMessageVisibilityCommand({ QueueUrl: mainUrl, ReceiptHandle: messages[0].ReceiptHandle, VisibilityTimeout: 0 }));
  }
  const thirdReceive = await receive(mainUrl, 1, 1);                                            // passou do maxReceiveCount: não deve vir da principal
  const inDlq = await receive(dlqUrl, 1, 5);
  record("6 ApproximateReceiveCount e redrive para a DLQ", receiveCounts.join(",") === "1,2" && thirdReceive.length === 0 && inDlq[0]?.Body === "poison", `contagens: ${receiveCounts.join(",")}, terceiro recebimento da principal: ${thirdReceive.length}, na DLQ: ${inDlq.map((m) => m.Body)}`);
}

// ----------------------------------------------------------------------------------------------------
//                       ITEM 7: WAITTIMESECONDS = 20 ESPERA E VOLTA AO CHEGAR
// ----------------------------------------------------------------------------------------------------
{
  const queues = await createQueuePair("e2-item7", 30, 5);
  const startedAt = performance.now();
  const longPoll = receive(queues.mainUrl, 1, 20);
  await Bun.sleep(2000);
  await send(queues.mainUrl, "late-arrival", "group-l", "l-1");
  const messages = await longPoll;
  const elapsedSeconds = (performance.now() - startedAt) / 1000;
  record("7 long polling de 20 s", messages[0]?.Body === "late-arrival" && elapsedSeconds >= 1.9 && elapsedSeconds < 10, `voltou em ${elapsedSeconds.toFixed(2)} s com ${messages.map((m) => m.Body)}`);
}

// ----------------------------------------------------------------------------------------------------
//                        ITEM 8: APPROXIMATENUMBEROFMESSAGES DA DLQ
// ----------------------------------------------------------------------------------------------------
{
  const queues = await createQueuePair("e2-item8", 30, 5);
  await send(queues.dlqUrl, "dlq-1", "group-q", "q-1");
  await send(queues.dlqUrl, "dlq-2", "group-q", "q-2");
  const attributes = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: queues.dlqUrl, AttributeNames: ["ApproximateNumberOfMessages"] }));
  record("8 ApproximateNumberOfMessages da DLQ", attributes.Attributes?.ApproximateNumberOfMessages === "2", `valor: ${attributes.Attributes?.ApproximateNumberOfMessages}`);
}

// ----------------------------------------------------------------------------------------------------
//                                ITEM 9: SENDMESSAGEBATCH COM 10
// ----------------------------------------------------------------------------------------------------
{
  const queues = await createQueuePair("e2-item9", 30, 5);
  const entries = Array.from({ length: 10 }, (_, index) => ({ Id: `entry-${index}`, MessageBody: `batch-${index}`, MessageGroupId: `group-${index}`, MessageDeduplicationId: `b-${index}` }));
  const response = await sqs.send(new SendMessageBatchCommand({ QueueUrl: queues.mainUrl, Entries: entries }));
  const attributes = await sqs.send(new GetQueueAttributesCommand({ QueueUrl: queues.mainUrl, AttributeNames: ["ApproximateNumberOfMessages"] }));
  record("9 SendMessageBatch com 10", (response.Successful?.length ?? 0) === 10 && (response.Failed?.length ?? 0) === 0 && attributes.Attributes?.ApproximateNumberOfMessages === "10", `aceitas: ${response.Successful?.length}, recusadas: ${response.Failed?.length ?? 0}, na fila: ${attributes.Attributes?.ApproximateNumberOfMessages}`);
}

console.log(JSON.stringify({ emulator: process.env.SQS_ENDPOINT ?? "http://localhost:4566", results }, null, 2));
