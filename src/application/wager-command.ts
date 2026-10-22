// ====================================================================================================
//                     VALIDAÇÃO DO COMANDO DE TRANSAÇÃO (HTTP E FILA USAM A MESMA)
// ====================================================================================================
// Qualquer falha vira 400 VALIDATION_ERROR no HTTP e DLQ na fila, com nada gravado. Campo desconhecido
// é rejeitado, para um erro de digitação não passar em silêncio.
import { InvalidMoneyError } from "../domain/errors";                                           // formato de valor
import { Money } from "../domain/money";                                                        // valor exato
import { WagerTransactionKind } from "../domain/wager-transaction";                             // tipos aceitos
import type { FieldError } from "./errors";                                                     // erro por campo

export interface WagerCommand {
  providerId: string;
  externalTransactionId: string;
  playerId: string;                                                                             // UUID em minúsculas
  walletId: string;                                                                             // UUID em minúsculas
  roundId: string;
  gameId: string;
  kind: WagerTransactionKind;
  money: Money;
  referenceExternalTransactionId: string | undefined;                                           // ausente e null são a mesma coisa
}

export type ParseResult<T> = { ok: true; value: T } | { ok: false; errors: FieldError[] };

export const UUID_PATTERN = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;   // qualquer versão; os ids no caminho das rotas usam o mesmo
const IDENTIFIER_PATTERN = /^[A-Za-z0-9._:-]+$/;
const IDENTIFIER_MAX_LENGTH = 128;
const IDEMPOTENCY_KEY_PATTERN = /^[\x21-\x7E]{1,255}$/;                                         // ASCII visível, 1 a 255
const CORRELATION_ID_PATTERN = /^[A-Za-z0-9._:-]{1,128}$/;                                      // até 128 caracteres seguros
const RESERVED_PROVIDER_ID = "internal";
const PROVIDER_KINDS = [WagerTransactionKind.Bet, WagerTransactionKind.Win, WagerTransactionKind.Loss, WagerTransactionKind.Refund, WagerTransactionKind.Rollback];
const COMMAND_FIELDS = ["providerId", "externalTransactionId", "playerId", "walletId", "roundId", "gameId", "kind", "money", "referenceExternalTransactionId"];

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isValidIdempotencyKey(value: unknown): value is string {
  return typeof value === "string" && IDEMPOTENCY_KEY_PATTERN.test(value);
}

// o header X-Correlation-Id do HTTP e o atributo correlationId da fila: fora do formato, nasce outro
export function isValidCorrelationId(value: unknown): value is string {
  return typeof value === "string" && CORRELATION_ID_PATTERN.test(value);
}

export function rejectUnknownFields(input: Record<string, unknown>, allowedFields: string[], prefix: string, errors: FieldError[]): void {
  for (const field of Object.keys(input)) {
    if (!allowedFields.includes(field)) errors.push({ field: `${prefix}${field}`, message: "campo desconhecido" });
  }
}

export function readUuid(input: Record<string, unknown>, field: string, errors: FieldError[]): string {
  const value = input[field];
  if (typeof value !== "string" || !UUID_PATTERN.test(value)) {
    errors.push({ field, message: "precisa ser um UUID" });
    return "";
  }
  return value.toLowerCase();                                                                   // normalizado antes do hash e da gravação
}

function readIdentifier(input: Record<string, unknown>, field: string, errors: FieldError[]): string {
  const value = input[field];
  if (typeof value !== "string" || value.length < 1 || value.length > IDENTIFIER_MAX_LENGTH || !IDENTIFIER_PATTERN.test(value)) {
    errors.push({ field, message: `precisa ter de 1 a ${IDENTIFIER_MAX_LENGTH} caracteres em [A-Za-z0-9._:-]` });
    return "";
  }
  return value;
}

export function readMoney(input: Record<string, unknown>, field: string, acceptedCurrencies: string[], errors: FieldError[]): Money | undefined {
  const value = input[field];
  if (!isPlainObject(value)) {
    errors.push({ field, message: 'precisa ser um objeto { "amount": "25.00", "currency": "BRL" }' });
    return undefined;
  }
  const errorsBefore = errors.length;
  rejectUnknownFields(value, ["amount", "currency"], `${field}.`, errors);
  if (errors.length > errorsBefore) return undefined;
  try {
    const money = Money.from({ amount: value.amount as string, currency: value.currency as string });
    if (!acceptedCurrencies.includes(money.currency)) {
      errors.push({ field: `${field}.currency`, message: `moeda não aceita; aceitas: ${acceptedCurrencies.join(", ")}` });
      return undefined;
    }
    return money;
  } catch (error) {
    if (!(error instanceof InvalidMoneyError)) throw error;
    errors.push({ field, message: error.message });
    return undefined;
  }
}

export function parseWagerCommand(body: unknown, acceptedCurrencies: string[]): ParseResult<WagerCommand> {
  if (!isPlainObject(body)) return { ok: false, errors: [{ field: "body", message: "precisa ser um objeto JSON" }] };
  const errors: FieldError[] = [];
  rejectUnknownFields(body, COMMAND_FIELDS, "", errors);

  const providerId = readIdentifier(body, "providerId", errors);
  if (providerId === RESERVED_PROVIDER_ID) errors.push({ field: "providerId", message: `"${RESERVED_PROVIDER_ID}" é reservado` });
  const externalTransactionId = readIdentifier(body, "externalTransactionId", errors);
  const playerId = readUuid(body, "playerId", errors);
  const walletId = readUuid(body, "walletId", errors);
  const roundId = readIdentifier(body, "roundId", errors);
  const gameId = readIdentifier(body, "gameId", errors);
  const money = readMoney(body, "money", acceptedCurrencies, errors);

  const kind = body.kind as WagerTransactionKind;
  if (!PROVIDER_KINDS.includes(kind)) errors.push({ field: "kind", message: `precisa ser um de ${PROVIDER_KINDS.join(", ")}` });

  const rawReference = body.referenceExternalTransactionId;
  const hasReference = rawReference !== undefined && rawReference !== null;
  const referenceExternalTransactionId = hasReference ? readIdentifier(body, "referenceExternalTransactionId", errors) : undefined;
  const referenceRequired = kind === WagerTransactionKind.Refund || kind === WagerTransactionKind.Rollback;
  if (referenceRequired && !hasReference) errors.push({ field: "referenceExternalTransactionId", message: `obrigatório em ${kind}` });
  if (kind === WagerTransactionKind.Bet && hasReference) errors.push({ field: "referenceExternalTransactionId", message: "BET não aceita referência" });

  if (money && money.isZero() && kind !== WagerTransactionKind.Loss && PROVIDER_KINDS.includes(kind)) {
    errors.push({ field: "money.amount", message: `${kind} exige valor maior que zero; rodada sem prêmio é LOSS` });
  }

  if (errors.length > 0 || !money) return { ok: false, errors };
  return { ok: true, value: { providerId, externalTransactionId, playerId, walletId, roundId, gameId, kind, money, referenceExternalTransactionId } };
}
