// ====================================================================================================
//                        CRIA AS FILAS NO EMULADOR: bun scripts/create-queues.ts
// ====================================================================================================
import { loadConfig } from "../src/config";
import { createQueues } from "../src/infrastructure/messaging/create-queues";
import { createSqsClient } from "../src/infrastructure/messaging/sqs";
import { retryOnStartup } from "./retry-on-startup";                                            // emulador recém-subido pode demorar

const config = loadConfig();
const client = createSqsClient(config.sqs);
await retryOnStartup("criação das filas", () => createQueues(client, config.sqs, config.sqs));   // CreateQueue é idempotente: repetir não estraga nada
console.log(`filas criadas: ${config.sqs.inputQueueName}, ${config.sqs.deadLetterQueueName}, ${config.sqs.eventsQueueName}`);
