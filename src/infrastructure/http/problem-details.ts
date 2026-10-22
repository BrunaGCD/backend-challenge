// ====================================================================================================
//          ERROS EM PROBLEM DETAILS (RFC 9457) COM O CODE COMO EXTENSÃO (ARCHITECTURE §10)
// ====================================================================================================
// Se existe uma WagerTransaction, o corpo é a transação (isso é feito no controller). Se não existe,
// o corpo é um erro neste formato. A mesma situação tem o mesmo status em todos os endpoints.
import { Catch, HttpException, type ArgumentsHost, type ExceptionFilter } from "@nestjs/common";
import type { Response } from "express";
import { ApplicationError, ValidationError, WalletAlreadyExistsError } from "../../application/errors";
import { isTransientInfrastructureError } from "../../application/infrastructure-errors";
import { errorSummary, logger } from "../../observability/logger";

const STATUS_BY_CODE: Record<string, number> = {
  VALIDATION_ERROR: 400,
  IDEMPOTENCY_KEY_MISSING: 400,
  INVALID_CURSOR: 400,
  NOT_FOUND: 404,
  IDEMPOTENCY_KEY_CONFLICT: 409,
  EXTERNAL_TRANSACTION_ID_CONFLICT: 409,
  WALLET_ALREADY_EXISTS: 409,
  WALLET_NOT_FOUND: 422,
};

const TITLE_BY_STATUS: Record<number, string> = {
  400: "Bad Request",
  404: "Not Found",
  409: "Conflict",
  422: "Unprocessable Content",
  500: "Internal Server Error",
  503: "Service Unavailable",
};

export function sendProblem(response: Response, status: number, code: string, detail: string, extensions: Record<string, unknown> = {}): void {
  response
    .status(status)
    .type("application/problem+json")
    .json({ type: `/problems/${code.toLowerCase().replaceAll("_", "-")}`, title: TITLE_BY_STATUS[status] ?? "Error", status, detail, code, ...extensions });
}

@Catch()
export class ProblemDetailsFilter implements ExceptionFilter {
  catch(exception: unknown, host: ArgumentsHost): void {
    const response = host.switchToHttp().getResponse<Response>();

    if (exception instanceof ApplicationError && STATUS_BY_CODE[exception.code] !== undefined) {
      const extensions: Record<string, unknown> = {};
      if (exception instanceof ValidationError) extensions.errors = exception.errors;
      if (exception instanceof WalletAlreadyExistsError) extensions.walletId = exception.walletId;
      sendProblem(response, STATUS_BY_CODE[exception.code]!, exception.code, exception.message, extensions);
      return;
    }

    if (exception instanceof HttpException && exception.getStatus() === 404) {
      sendProblem(response, 404, "NOT_FOUND", "rota não encontrada");                          // rota inexistente
      return;
    }
    if (exception instanceof HttpException && exception.getStatus() < 500) {
      sendProblem(response, 400, "VALIDATION_ERROR", exception.message);
      return;
    }

    if (isTransientInfrastructureError(exception)) {
      logger.warn({ event: "http_transient_error", ...errorSummary(exception) }, "falha transitória de infraestrutura; nada foi gravado");
      response.setHeader("Retry-After", "1");
      sendProblem(response, 503, "SERVICE_UNAVAILABLE", "falha transitória de infraestrutura; reenvie com a mesma Idempotency-Key");
      return;
    }

    logger.error({ event: "http_unexpected_error", ...errorSummary(exception) }, "erro inesperado");
    sendProblem(response, 500, "INTERNAL_ERROR", "erro inesperado; reenviar com a mesma Idempotency-Key é seguro");
  }
}
