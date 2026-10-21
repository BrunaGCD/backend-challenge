// ====================================================================================================
//                        CRIA AS FILAS NO EMULADOR: bun scripts/create-queues.ts
// ====================================================================================================
import { loadConfig } from "../src/config";
import { createQueues } from "../src/infrastructure/messaging/create-queues";
import { createSqsClient } from "../src/infrastructure/messaging/sqs";

const config = loadConfig();
const client = createSqsClient(config.sqs);
await createQueues(client, config.sqs, config.sqs);
console.log(`filas criadas: ${config.sqs.inputQueueName}, ${config.sqs.deadLetterQueueName}, ${config.sqs.eventsQueueName}`);
