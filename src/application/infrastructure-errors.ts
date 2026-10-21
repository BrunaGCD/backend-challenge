// ====================================================================================================
//                  CLASSIFICAÇÃO DE ERROS DE INFRAESTRUTURA: TRANSITÓRIO OU NÃO
// ====================================================================================================
// Transitório: vale tentar de novo (503 no HTTP, retry na fila, próxima varredura no worker).
// O resto é inesperado: 500 no HTTP, retry na fila (nada foi gravado), FAILED no worker.

const TRANSIENT_SQLSTATES = new Set([
  "40001",                                                                                      // serialization_failure
  "40P01",                                                                                      // deadlock_detected
  "55P03",                                                                                      // lock_not_available (lock_timeout)
  "57014",                                                                                      // query_canceled (statement_timeout)
  "57P01",                                                                                      // admin_shutdown
  "57P02",                                                                                      // crash_shutdown
  "57P03",                                                                                      // cannot_connect_now
  "53300",                                                                                      // too_many_connections
]);

const TRANSIENT_NETWORK_CODES = new Set(["ECONNREFUSED", "ECONNRESET", "ETIMEDOUT", "EPIPE", "ENOTFOUND", "EAI_AGAIN", "EHOSTUNREACH", "ECONNABORTED"]);

const TRANSIENT_MESSAGES = [
  "Connection terminated",                                                                      // pg, quando o servidor cai no meio
  "timeout exceeded when trying to connect",                                                    // pg-pool sem conexão livre ou banco fora
  "Client has encountered a connection error",
  "connect ECONNREFUSED",
];

export type ConcurrencyConflictType = "lock_timeout" | "deadlock" | "unique_violation" | undefined;

function errorCode(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null) return undefined;
  const code = (error as { code?: unknown }).code;
  return typeof code === "string" ? code : undefined;
}

export function isTransientInfrastructureError(error: unknown): boolean {
  const code = errorCode(error);
  if (code !== undefined && (TRANSIENT_SQLSTATES.has(code) || code.startsWith("08") || TRANSIENT_NETWORK_CODES.has(code))) return true;   // classe 08: conexão
  const message = error instanceof Error ? error.message : String(error);
  return TRANSIENT_MESSAGES.some((fragment) => message.includes(fragment));
}

export function concurrencyConflictType(error: unknown): ConcurrencyConflictType {
  const code = errorCode(error);
  if (code === "55P03" || code === "57014") return "lock_timeout";
  if (code === "40P01" || code === "40001") return "deadlock";
  if (code === "23505") return "unique_violation";
  return undefined;
}

export function uniqueViolationConstraint(error: unknown): string | undefined {
  if (errorCode(error) !== "23505") return undefined;
  const constraint = (error as { constraint?: unknown }).constraint;
  return typeof constraint === "string" ? constraint : undefined;
}
