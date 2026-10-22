// ====================================================================================================
//                  CLASSIFICAÇÃO DE ERROS DE INFRAESTRUTURA: TRANSITÓRIO OU NÃO
// ====================================================================================================
// Transitório: vale tentar de novo (503 no HTTP, retry na fila, próxima varredura no worker).
// O resto é inesperado: 500 no HTTP, retry na fila (nada foi gravado), FAILED no worker.
// Dentro dos transitórios, banco indisponível ou saturado é o caso em que o problema não é da operação:
// o consumidor da fila para de buscar mensagens até o banco responder de novo.
// Fica na aplicação porque os casos de uso decidem com ela (consultar de novo depois de uma violação de
// unicidade, o tipo do conflito na métrica). Ela só lê os campos code, constraint e message do erro, sem
// importar o driver, então a aplicação continua sem depender da infraestrutura.
import { ConcurrentUpdateError } from "./errors";                                               // conflito que aparece no flush

const DATABASE_UNAVAILABLE_SQLSTATES = new Set([
  "57P01",                                                                                      // admin_shutdown
  "57P02",                                                                                      // crash_shutdown
  "57P03",                                                                                      // cannot_connect_now
  "53300",                                                                                      // too_many_connections
]);

const OTHER_TRANSIENT_SQLSTATES = new Set([
  "40001",                                                                                      // serialization_failure
  "40P01",                                                                                      // deadlock_detected
  "55P03",                                                                                      // lock_not_available (lock_timeout)
  "57014",                                                                                      // query_canceled (statement_timeout)
]);

const DATABASE_UNAVAILABLE_NETWORK_CODES = new Set(["ECONNREFUSED", "ECONNRESET", "ETIMEDOUT", "EPIPE", "ENOTFOUND", "EAI_AGAIN", "EHOSTUNREACH", "ECONNABORTED"]);

const DATABASE_UNAVAILABLE_MESSAGES = [
  "Connection terminated",                                                                      // pg, quando o servidor cai no meio
  "timeout exceeded when trying to connect",                                                    // pg-pool sem conexão livre ou banco fora
  "Client has encountered a connection error",
  "connect ECONNREFUSED",
];

export type ConcurrencyConflictType = "lock_timeout" | "deadlock" | "unique_violation" | "optimistic_lock" | undefined;

function errorCode(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null) return undefined;
  const code = (error as { code?: unknown }).code;
  return typeof code === "string" ? code : undefined;
}

// sem conexão, conexão caída, servidor desligando, ou sem conexão livre (no PostgreSQL ou no pool)
export function isDatabaseUnavailableError(error: unknown): boolean {
  const code = errorCode(error);
  if (code !== undefined && (DATABASE_UNAVAILABLE_SQLSTATES.has(code) || code.startsWith("08") || DATABASE_UNAVAILABLE_NETWORK_CODES.has(code))) return true;   // classe 08: conexão
  const message = error instanceof Error ? error.message : String(error);
  return DATABASE_UNAVAILABLE_MESSAGES.some((fragment) => message.includes(fragment));
}

export function isTransientInfrastructureError(error: unknown): boolean {
  const code = errorCode(error);
  if (code !== undefined && OTHER_TRANSIENT_SQLSTATES.has(code)) return true;                  // lock, statement_timeout, deadlock, serialização
  return isDatabaseUnavailableError(error);
}

export function concurrencyConflictType(error: unknown): ConcurrencyConflictType {
  const code = errorCode(error);
  if (code === "55P03" || code === "57014") return "lock_timeout";
  if (code === "40P01" || code === "40001") return "deadlock";
  if (code === "23505") return "unique_violation";
  if (error instanceof ConcurrentUpdateError) return "optimistic_lock";                         // version ou status mudou entre a leitura e o flush
  return undefined;
}

export function uniqueViolationConstraint(error: unknown): string | undefined {
  if (errorCode(error) !== "23505") return undefined;
  const constraint = (error as { constraint?: unknown }).constraint;
  return typeof constraint === "string" ? constraint : undefined;
}
