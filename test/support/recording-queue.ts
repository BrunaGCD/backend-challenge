// ====================================================================================================
//        FILA DE ENTRADA EMBRULHADA: ANOTA RECEBIMENTOS E MUDANÇAS DE VISIBILIDADE, E INJETA FALHAS
// ====================================================================================================
// A fila por trás continua a real. O ApproximateReceiveCount anotado em cada recebimento é o que diz se
// um recebimento foi gasto: uma mensagem processada sem sair das mãos do consumidor fica com [1].
import type { InputQueue } from "../../src/infrastructure/messaging/sqs";

export interface QueueRecord {
  receives: { at: number; messages: { sqsMessageId: string; messageId: string | undefined; receiveCount: number }[] }[];
  visibilityChanges: { at: number; sqsMessageId: string; messageId: string | undefined; seconds: number }[];
  deleteFailuresLeft: Map<string, number>;                                                      // messageId do envelope -> quantos DeleteMessage falham
}

export function newQueueRecord(): QueueRecord {
  return { receives: [], visibilityChanges: [], deleteFailuresLeft: new Map() };
}

function envelopeMessageId(body: string): string | undefined {
  try {
    const envelope = JSON.parse(body) as { messageId?: unknown };
    return typeof envelope.messageId === "string" ? envelope.messageId : undefined;
  } catch {
    return undefined;                                                                           // corpo que não é JSON
  }
}

export function recordingQueue(record: QueueRecord): (real: InputQueue) => InputQueue {
  return (real) => ({
    async receive(maxMessages, waitTimeSeconds, abortSignal) {
      const received = await real.receive(maxMessages, waitTimeSeconds, abortSignal);
      if (received.length > 0) record.receives.push({ at: performance.now(), messages: received.map((message) => ({ sqsMessageId: message.sqsMessageId, messageId: envelopeMessageId(message.body), receiveCount: message.receiveCount })) });
      return received;
    },
    async delete(message) {
      const messageId = envelopeMessageId(message.body) ?? "";
      const failuresLeft = record.deleteFailuresLeft.get(messageId) ?? 0;
      if (failuresLeft > 0) {
        record.deleteFailuresLeft.set(messageId, failuresLeft - 1);
        throw Object.assign(new Error("DeleteMessage falhou (injetado)"), { name: "ServiceUnavailable" });   // como o SQS fora por um instante
      }
      await real.delete(message);
    },
    async changeVisibility(message, seconds) {
      record.visibilityChanges.push({ at: performance.now(), sqsMessageId: message.sqsMessageId, messageId: envelopeMessageId(message.body), seconds });
      await real.changeVisibility(message, seconds);
    },
    sendToDeadLetter: (message, reason) => real.sendToDeadLetter(message, reason),
  });
}

// os ApproximateReceiveCount de cada vez que a mensagem foi recebida, em ordem
export function receiveCountsOf(record: QueueRecord, messageId: string): number[] {
  return record.receives.flatMap((batch) => batch.messages).filter((message) => message.messageId === messageId).map((message) => message.receiveCount);
}

export function releasesOf(record: QueueRecord, messageId: string): number {
  return record.visibilityChanges.filter((change) => change.messageId === messageId && change.seconds === 0).length;
}
