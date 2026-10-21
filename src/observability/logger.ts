// ====================================================================================================
//                               LOGGER JSON (PINO) COM O CONTEXTO
// ====================================================================================================
// Nunca vai para log: playerId, valor monetário (amount, saldos, diferença da reconciliação), corpo
// de requisição ou de mensagem, header Authorization. O redact é a segunda barreira, caso algum
// desses caminhos escape por engano.
import pino from "pino";                                                                        // logger JSON em stdout
import { currentLogContext } from "./log-context";                                              // ids do trabalho atual

export const logger = pino({
  level: process.env.LOG_LEVEL ?? "info",
  base: { pid: process.pid, instance: process.env.INSTANCE_NAME ?? "local" },                  // qual instância escreveu
  mixin: () => ({ ...currentLogContext() }),                                                   // correlationId, messageId, transactionId, walletId, providerId
  redact: {
    paths: ["req.headers.authorization", "headers.authorization", "authorization", "playerId", "*.playerId", "amount", "*.amount", "money", "*.money", "balance", "*.balance", "body", "*.body"],
    censor: "[omitido]",
  },
  timestamp: pino.stdTimeFunctions.isoTime,
});

export function errorSummary(error: unknown): Record<string, unknown> {
  if (error instanceof Error) {
    const withCode = error as Error & { code?: string; constraint?: string };
    return { errorName: error.name, errorCode: withCode.code, constraint: withCode.constraint, errorMessage: error.message.split("\n")[0] };   // primeira linha: o detalhe do pg pode trazer valores
  }
  return { errorMessage: String(error) };
}
