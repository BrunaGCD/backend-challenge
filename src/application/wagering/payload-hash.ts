// ====================================================================================================
//                     PAYLOAD HASH: SHA-256 DO JSON CANÔNICO (RFC 8785, JCS)
// ====================================================================================================
// Montado a partir do comando já validado e normalizado, nunca do corpo bruto. O header, o
// data.idempotencyKey da fila e qualquer metadado de transporte ficam de fora, então a mesma operação
// chegando uma vez por HTTP e outra pela fila dá o mesmo hash (é replay).
import { createHash } from "node:crypto";                                                       // SHA-256
import canonicalize from "canonicalize";                                                        // JCS, do autor da RFC 8785
import type { Money } from "../../domain/money";                                                // valor serializado pelo toJSON
import type { WagerCommand } from "./wager-command";                                            // comando validado

function sha256Hex(text: string): string {
  return createHash("sha256").update(text, "utf8").digest("hex");                              // 64 caracteres em minúsculas
}

export function canonicalPayload(command: WagerCommand): string {
  const businessFields: Record<string, unknown> = {
    providerId: command.providerId,
    externalTransactionId: command.externalTransactionId,
    playerId: command.playerId,
    walletId: command.walletId,
    roundId: command.roundId,
    gameId: command.gameId,
    kind: command.kind,
    money: command.money.toJSON(),
  };
  if (command.referenceExternalTransactionId !== undefined) {
    businessFields.referenceExternalTransactionId = command.referenceExternalTransactionId;   // só quando presente
  }
  return canonicalize(businessFields) as string;
}

export function computePayloadHash(command: WagerCommand): string {
  return sha256Hex(canonicalPayload(command));
}

export function computeOpeningPayloadHash(walletId: string, money: Money): string {
  return sha256Hex(canonicalize({ kind: "OPENING", walletId, money: money.toJSON() }) as string);   // I8: kind, walletId e money
}
