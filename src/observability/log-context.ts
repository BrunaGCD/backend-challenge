// ====================================================================================================
//                     CONTEXTO DO TRABALHO EM ANDAMENTO (ASYNCLOCALSTORAGE)
// ====================================================================================================
// Guarda os ids do trabalho atual para todo log carregá-los sem passar à mão. O E1 (item 8) confirmou
// que o valor sobrevive aos await e às consultas ao banco no Bun.
import { AsyncLocalStorage } from "node:async_hooks";                                           // contexto que atravessa os await

export interface LogContext {
  correlationId?: string;
  messageId?: string;
  transactionId?: string;
  walletId?: string;
  providerId?: string;
}

const storage = new AsyncLocalStorage<LogContext>();

export function runWithLogContext<T>(context: LogContext, work: () => T): T {
  const parent = storage.getStore() ?? {};
  return storage.run({ ...parent, ...context }, work);                                          // herda o que já existia e acrescenta
}

export function addToLogContext(fields: LogContext): void {
  const current = storage.getStore();
  if (current) Object.assign(current, fields);                                                  // ids que só aparecem no meio do caminho
}

export function currentLogContext(): LogContext {
  return storage.getStore() ?? {};
}
