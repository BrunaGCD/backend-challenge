// ====================================================================================================
//                       CRIAÇÃO DAS FILAS COM A CONFIGURAÇÃO DA SEÇÃO 10
// ====================================================================================================
// CreateQueue é idempotente quando os atributos são os mesmos, então rodar de novo não estraga nada.
import { CreateQueueCommand, GetQueueAttributesCommand, type SQSClient } from "@aws-sdk/client-sqs";

export interface QueueNames {
  inputQueueName: string;
  deadLetterQueueName: string;
  eventsQueueName: string;
}

export interface QueueSettings {
  visibilityTimeoutSeconds: number;
  maxReceiveCount: number;
  waitTimeSeconds: number;
}

const FOURTEEN_DAYS_IN_SECONDS = String(14 * 24 * 60 * 60);                                     // o máximo do SQS

export async function createQueues(client: SQSClient, names: QueueNames, settings: QueueSettings): Promise<void> {
  const deadLetter = await client.send(new CreateQueueCommand({
    QueueName: names.deadLetterQueueName,
    Attributes: { FifoQueue: "true", ContentBasedDeduplication: "false", MessageRetentionPeriod: FOURTEEN_DAYS_IN_SECONDS },   // DLQ de FIFO também é FIFO
  }));
  const deadLetterAttributes = await client.send(new GetQueueAttributesCommand({ QueueUrl: deadLetter.QueueUrl, AttributeNames: ["QueueArn"] }));

  await client.send(new CreateQueueCommand({
    QueueName: names.inputQueueName,
    Attributes: {
      FifoQueue: "true",
      ContentBasedDeduplication: "false",                                                       // dedup pelo messageId, não pelo conteúdo
      VisibilityTimeout: String(settings.visibilityTimeoutSeconds),
      ReceiveMessageWaitTimeSeconds: String(settings.waitTimeSeconds),
      RedrivePolicy: JSON.stringify({ deadLetterTargetArn: deadLetterAttributes.Attributes?.QueueArn, maxReceiveCount: settings.maxReceiveCount }),
    },
  }));

  await client.send(new CreateQueueCommand({
    QueueName: names.eventsQueueName,
    Attributes: { FifoQueue: "true", ContentBasedDeduplication: "false" },                      // dedup pelo eventId
  }));
}
