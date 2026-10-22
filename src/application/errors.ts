// ====================================================================================================
//                       ERROS DA APLICAÇÃO (OS QUE NÃO VIRAM TRANSAÇÃO GRAVADA)
// ====================================================================================================
// Cada um tem um code estável (a tabela de docs/regras-de-negocio.md); a borda (HTTP ou fila) decide o status e a ação.

export interface FieldError {
  field: string;
  message: string;
}

export abstract class ApplicationError extends Error {
  abstract readonly code: string;

  constructor(message: string) {
    super(message);
    this.name = new.target.name;
  }
}

export class ValidationError extends ApplicationError {
  readonly code = "VALIDATION_ERROR";

  constructor(public readonly errors: FieldError[]) {
    super(`payload inválido: ${errors.map((error) => error.field).join(", ")}`);
  }
}

export class IdempotencyKeyMissingError extends ApplicationError {
  readonly code = "IDEMPOTENCY_KEY_MISSING";
}

export class IdempotencyKeyConflictError extends ApplicationError {
  readonly code = "IDEMPOTENCY_KEY_CONFLICT";
}

export class ExternalTransactionIdConflictError extends ApplicationError {
  readonly code = "EXTERNAL_TRANSACTION_ID_CONFLICT";
}

export class WalletNotFoundError extends ApplicationError {
  readonly code = "WALLET_NOT_FOUND";
}

export class WalletAlreadyExistsError extends ApplicationError {
  readonly code = "WALLET_ALREADY_EXISTS";

  constructor(public readonly walletId: string) {
    super(`já existe uma wallet para esse player e moeda: ${walletId}`);
  }
}

export class InvalidCursorError extends ApplicationError {
  readonly code = "INVALID_CURSOR";
}

export class NotFoundError extends ApplicationError {
  readonly code = "NOT_FOUND";
}

export class InboxPayloadConflictError extends ApplicationError {
  readonly code = "INBOX_PAYLOAD_CONFLICT";                                                     // mesmo messageId com conteúdo diferente
}

// UPDATE com 0 linhas no flush (version da wallet ou status da pendente mudou depois da leitura).
// Com o lock da linha isso não acontece; se acontecer é bug, e a transação inteira é desfeita.
export class ConcurrentUpdateError extends ApplicationError {
  readonly code = "INTERNAL_ERROR";
}
