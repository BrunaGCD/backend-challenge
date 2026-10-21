// ====================================================================================================
//        MANDA UMA OPERAÇÃO PELA FILA DE ENTRADA: bun scripts/send-wager-message.ts --wallet ... --player ...
// ====================================================================================================
// Monta a mensagem no formato da seção 10 do desafio e envia com o contrato da fila
// (MessageGroupId = walletId, MessageDeduplicationId = messageId). Pelo Docker:
//   docker compose run --rm --no-deps tests bun scripts/send-wager-message.ts --wallet <id> --player <id> --kind BET --amount 25.00
import { parseArgs } from "node:util";                                                          // leitura das opções da linha de comando
import { SendMessageCommand } from "@aws-sdk/client-sqs";                                       // envio para a fila
import { loadConfig } from "../src/config";                                                     // endpoint e nome da fila
import { createSqsClient, QueueUrls } from "../src/infrastructure/messaging/sqs";

const { values: options } = parseArgs({
  options: {
    wallet: { type: "string" },
    player: { type: "string" },
    kind: { type: "string", default: "BET" },
    amount: { type: "string", default: "10.00" },
    currency: { type: "string", default: "BRL" },
    provider: { type: "string", default: "provider-a" },
    "external-id": { type: "string" },                                                          // repetir o mesmo id externo testa o replay
    reference: { type: "string" },                                                              // id externo da referência (REFUND, ROLLBACK, WIN)
    round: { type: "string", default: "round-1" },
    "message-id": { type: "string" },                                                           // repetir o mesmo messageId testa a inbox
  },
});

if (!options.wallet || !options.player) {
  console.error("uso: bun scripts/send-wager-message.ts --wallet <walletId> --player <playerId> [--kind BET] [--amount 10.00] [--reference <id externo>] [--external-id <id>] [--message-id <id>]");
  process.exit(1);
}

const config = loadConfig();
const client = createSqsClient(config.sqs);
const externalTransactionId = options["external-id"] ?? `ext-${Bun.randomUUIDv7()}`;
const messageId = options["message-id"] ?? `msg-${Bun.randomUUIDv7()}`;

const data: Record<string, unknown> = {
  providerId: options.provider,
  externalTransactionId,
  idempotencyKey: `${options.provider}:${externalTransactionId}`,                              // o default recomendado pelo desafio
  playerId: options.player,
  walletId: options.wallet,
  roundId: options.round,
  gameId: "fortune-chimp",
  kind: options.kind,
  money: { amount: options.amount, currency: options.currency },
};
if (options.reference) data.referenceExternalTransactionId = options.reference;
const message = { messageId, type: "WagerTransactionRequested", occurredAt: new Date().toISOString(), data };

const queueUrl = await new QueueUrls(client).urlOf(config.sqs.inputQueueName);
await client.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: JSON.stringify(message), MessageGroupId: options.wallet, MessageDeduplicationId: messageId }));
console.log(`enviada para ${config.sqs.inputQueueName}:`);
console.log(JSON.stringify(message, null, 2));
console.log(`para ver o resultado: GET /providers/${options.provider}/wagering/transactions/${externalTransactionId}`);
