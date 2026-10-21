// ====================================================================================================
//                  SQS: CLIENTE, FILA DE ENTRADA E PUBLICAÇÃO DOS EVENTOS DA OUTBOX
// ====================================================================================================
import {
  SQSClient, GetQueueUrlCommand, GetQueueAttributesCommand, ReceiveMessageCommand, DeleteMessageCommand,
  ChangeMessageVisibilityCommand, SendMessageCommand, SendMessageBatchCommand, type Message,
} from "@aws-sdk/client-sqs";                                                                   // SDK oficial da AWS
import type { EventPublisher, PublishOutcome } from "../../application/ports";
import type { OutboxMessage } from "../../domain/outbox-message";

export interface SqsSettings {
  endpoint: string;
  region: string;
  accessKeyId: string;
  secretAccessKey: string;
  clientTimeoutMilliseconds: number;
}

// requestTimeoutMilliseconds: 5 s para as chamadas comuns; o long polling precisa de mais que o WaitTimeSeconds
export function createSqsClient(settings: SqsSettings, requestTimeoutMilliseconds = settings.clientTimeoutMilliseconds): SQSClient {
  return new SQSClient({
    endpoint: settings.endpoint,
    region: settings.region,
    credentials: { accessKeyId: settings.accessKeyId, secretAccessKey: settings.secretAccessKey },   // fictícias no emulador
    requestHandler: { requestTimeout: requestTimeoutMilliseconds, connectionTimeout: settings.clientTimeoutMilliseconds, throwOnRequestTimeout: true },   // sem throw, o SDK só avisa e a chamada segue pendurada
    maxAttempts: 1,                                                                             // o retry é nosso, com o backoff comum
  });
}

// resolve o nome em URL uma vez e guarda
export class QueueUrls {
  private readonly cache = new Map<string, string>();

  constructor(private readonly client: SQSClient) {}

  async urlOf(queueName: string): Promise<string> {
    const cached = this.cache.get(queueName);
    if (cached) return cached;
    const response = await this.client.send(new GetQueueUrlCommand({ QueueName: queueName }));
    if (!response.QueueUrl) throw new Error(`fila ${queueName} não encontrada`);
    this.cache.set(queueName, response.QueueUrl);
    return response.QueueUrl;
  }
}

// ----------------------------------------------------------------------------------------------------
//                                    FILA DE ENTRADA
// ----------------------------------------------------------------------------------------------------
export interface ReceivedMessage {
  sqsMessageId: string;
  receiptHandle: string;
  body: string;
  groupId: string;
  deduplicationId: string;
  receiveCount: number;                                                                         // ApproximateReceiveCount
  correlationIdAttribute: string | undefined;                                                   // atributo de mensagem opcional
}

// A porta que o consumidor usa. O teste de "morto depois do commit e antes do ack" troca o delete.
export interface InputQueue {
  receive(maxMessages: number, waitTimeSeconds: number, abortSignal: AbortSignal): Promise<ReceivedMessage[]>;
  delete(message: ReceivedMessage): Promise<void>;
  changeVisibility(message: ReceivedMessage, visibilityTimeoutSeconds: number): Promise<void>;
  sendToDeadLetter(message: ReceivedMessage, reason: string): Promise<void>;
}

function toReceivedMessage(message: Message): ReceivedMessage {
  return {
    sqsMessageId: message.MessageId ?? "",
    receiptHandle: message.ReceiptHandle ?? "",
    body: message.Body ?? "",
    groupId: message.Attributes?.MessageGroupId ?? "",
    deduplicationId: message.Attributes?.MessageDeduplicationId ?? message.MessageId ?? "",
    receiveCount: Number(message.Attributes?.ApproximateReceiveCount ?? "1"),
    correlationIdAttribute: message.MessageAttributes?.correlationId?.StringValue,
  };
}

export class SqsInputQueue implements InputQueue {
  constructor(
    private readonly client: SQSClient,
    private readonly longPollingClient: SQSClient,                                              // timeout maior que o WaitTimeSeconds
    private readonly urls: QueueUrls,
    private readonly inputQueueName: string,
    private readonly deadLetterQueueName: string,
  ) {}

  async receive(maxMessages: number, waitTimeSeconds: number, abortSignal: AbortSignal): Promise<ReceivedMessage[]> {
    const queueUrl = await this.urls.urlOf(this.inputQueueName);
    const response = await this.longPollingClient.send(new ReceiveMessageCommand({
      QueueUrl: queueUrl,
      MaxNumberOfMessages: maxMessages,
      WaitTimeSeconds: waitTimeSeconds,                                                         // long polling
      MessageSystemAttributeNames: ["ApproximateReceiveCount", "MessageGroupId", "MessageDeduplicationId"],
      MessageAttributeNames: ["correlationId"],
    }), { abortSignal });                                                                       // o SIGTERM cancela a espera
    return (response.Messages ?? []).map(toReceivedMessage);
  }

  async delete(message: ReceivedMessage): Promise<void> {
    const queueUrl = await this.urls.urlOf(this.inputQueueName);
    await this.client.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: message.receiptHandle }));
  }

  async changeVisibility(message: ReceivedMessage, visibilityTimeoutSeconds: number): Promise<void> {
    const queueUrl = await this.urls.urlOf(this.inputQueueName);
    await this.client.send(new ChangeMessageVisibilityCommand({ QueueUrl: queueUrl, ReceiptHandle: message.receiptHandle, VisibilityTimeout: visibilityTimeoutSeconds }));
  }

  async sendToDeadLetter(message: ReceivedMessage, reason: string): Promise<void> {
    const queueUrl = await this.urls.urlOf(this.deadLetterQueueName);
    await this.client.send(new SendMessageCommand({
      QueueUrl: queueUrl,
      MessageBody: message.body,                                                                // o corpo original, intacto
      MessageGroupId: message.groupId || "unknown-group",
      MessageDeduplicationId: message.deduplicationId,                                          // reenvio depois de um crash cai na deduplicação
      MessageAttributes: { reason: { DataType: "String", StringValue: reason } },
    }));
  }
}

export async function approximateNumberOfMessages(client: SQSClient, urls: QueueUrls, queueName: string): Promise<number> {
  const queueUrl = await urls.urlOf(queueName);
  const response = await client.send(new GetQueueAttributesCommand({ QueueUrl: queueUrl, AttributeNames: ["ApproximateNumberOfMessages"] }));
  return Number(response.Attributes?.ApproximateNumberOfMessages ?? "0");
}

// ----------------------------------------------------------------------------------------------------
//                               PUBLICAÇÃO DOS EVENTOS DA OUTBOX
// ----------------------------------------------------------------------------------------------------
export class SqsEventPublisher implements EventPublisher {
  constructor(
    private readonly client: SQSClient,
    private readonly urls: QueueUrls,
    private readonly eventsQueueName: string,
  ) {}

  async publishBatch(messages: OutboxMessage[]): Promise<PublishOutcome> {
    if (messages.length === 0) return { publishedIds: [], failures: [] };
    try {
      const queueUrl = await this.urls.urlOf(this.eventsQueueName);
      const response = await this.client.send(new SendMessageBatchCommand({
        QueueUrl: queueUrl,
        Entries: messages.map((message) => ({
          Id: message.id,
          MessageBody: JSON.stringify(message.payload),
          MessageGroupId: message.aggregateId,                                                  // todos os eventos de uma wallet no mesmo grupo
          MessageDeduplicationId: message.id,                                                   // eventId: a republicação cai na deduplicação
        })),
      }));
      const publishedIds = (response.Successful ?? []).map((entry) => entry.Id ?? "");
      const failures = (response.Failed ?? []).map((entry) => ({ id: entry.Id ?? "", error: `${entry.Code ?? "erro"}: ${entry.Message ?? ""}` }));
      return { publishedIds, failures };
    } catch (error) {
      const description = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
      return { publishedIds: [], failures: messages.map((message) => ({ id: message.id, error: description })) };   // chamada inteira falhou: todas voltam
    }
  }
}
