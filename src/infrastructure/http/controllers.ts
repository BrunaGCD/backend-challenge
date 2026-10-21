// ====================================================================================================
//                                    CONTROLLERS HTTP (SEÇÃO 9)
// ====================================================================================================
// Só traduzem HTTP em chamada de caso de uso e o resultado em status e corpo. Nenhuma regra mora aqui.
import { Body, Controller, Get, Headers, HttpCode, Inject, Param, Post, Query, Req, Res } from "@nestjs/common";
import type { Request, Response } from "express";
import { IdempotencyKeyMissingError, ValidationError, type FieldError } from "../../application/errors";
import { isPlainObject, isValidIdempotencyKey, parseWagerCommand, readMoney, readUuid, rejectUnknownFields } from "../../application/wager-command";
import type { Application } from "../../composition";
import { metricsRegistry } from "../../observability/metrics";
import { approximateNumberOfMessages } from "../messaging/sqs";
import { PublicRoute } from "./auth";
import { presentLedgerEntry, presentReconciliation, presentTransaction, presentTransactionResult, presentWallet, statusCodeForTransaction } from "./presenters";
import { sendProblem } from "./problem-details";

export const APPLICATION = "APPLICATION";                                                       // token de injeção das peças montadas

type RequestWithContext = Request & { correlationId: string };

const UUID_PATTERN = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;

function pathUuid(value: string, field: string): string {
  if (!UUID_PATTERN.test(value)) throw new ValidationError([{ field, message: "precisa ser um UUID" }]);
  return value.toLowerCase();
}

// ----------------------------------------------------------------------------------------------------
//                                           WALLETS
// ----------------------------------------------------------------------------------------------------
@Controller("wallets")
export class WalletsController {
  constructor(@Inject(APPLICATION) private readonly application: Application) {}

  @Post()
  @HttpCode(201)
  async create(@Body() body: unknown, @Req() request: RequestWithContext) {
    if (!isPlainObject(body)) throw new ValidationError([{ field: "body", message: "precisa ser um objeto JSON" }]);
    const errors: FieldError[] = [];
    rejectUnknownFields(body, ["playerId", "initialBalance"], "", errors);
    const playerId = readUuid(body, "playerId", errors);
    const initialBalance = readMoney(body, "initialBalance", this.application.config.acceptedCurrencies, errors);
    if (errors.length > 0 || !initialBalance) throw new ValidationError(errors);
    const wallet = await this.application.createWallet.execute({ playerId, initialBalance }, request.correlationId);
    return presentWallet(wallet, false);
  }

  @Get(":walletId")
  async get(@Param("walletId") walletId: string) {
    const wallet = await this.application.queries.getWallet(pathUuid(walletId, "walletId"));
    return presentWallet(wallet, true);
  }

  @Get(":walletId/ledger")
  async ledger(@Param("walletId") walletId: string, @Query("cursor") cursor: string | undefined, @Query("limit") limit: string | undefined) {
    const page = await this.application.queries.getLedgerPage(pathUuid(walletId, "walletId"), cursor, limit);
    return { entries: page.entries.map(presentLedgerEntry), nextCursor: page.nextCursor };
  }

  @Post(":walletId/reconciliation")
  @HttpCode(200)                                                                                // divergência não é erro HTTP
  async reconcile(@Param("walletId") walletId: string) {
    const result = await this.application.queries.reconcile(pathUuid(walletId, "walletId"));
    return presentReconciliation(result);
  }
}

// ----------------------------------------------------------------------------------------------------
//                                        TRANSAÇÕES
// ----------------------------------------------------------------------------------------------------
@Controller()
export class WageringController {
  constructor(@Inject(APPLICATION) private readonly application: Application) {}

  @Post("wagering/transactions")
  async submit(@Body() body: unknown, @Headers("idempotency-key") idempotencyKey: string | undefined, @Req() request: RequestWithContext, @Res({ passthrough: true }) response: Response) {
    if (idempotencyKey === undefined || idempotencyKey === "") throw new IdempotencyKeyMissingError("header Idempotency-Key é obrigatório");
    if (!isValidIdempotencyKey(idempotencyKey)) throw new ValidationError([{ field: "Idempotency-Key", message: "de 1 a 255 caracteres ASCII visíveis" }]);
    const parsed = parseWagerCommand(body, this.application.config.acceptedCurrencies);
    if (!parsed.ok) throw new ValidationError(parsed.errors);

    const result = await this.application.processWager.execute(parsed.value, idempotencyKey, { channel: "http", correlationId: request.correlationId, causationId: idempotencyKey });
    if (result.kind !== "decided") throw new Error("resultado de inbox no canal HTTP");      // só a fila tem inbox
    response.status(statusCodeForTransaction(result.transaction));
    return presentTransactionResult(result.transaction, result.idempotentReplay);
  }

  @Get("wagering/transactions/:transactionId")
  async get(@Param("transactionId") transactionId: string) {
    const transaction = await this.application.queries.getTransaction(pathUuid(transactionId, "transactionId"));
    return presentTransaction(transaction);
  }

  @Get("providers/:providerId/wagering/transactions/:externalTransactionId")
  async getByProvider(@Param("providerId") providerId: string, @Param("externalTransactionId") externalTransactionId: string) {
    const transaction = await this.application.queries.getTransactionByProvider(providerId, externalTransactionId);
    return presentTransaction(transaction);
  }
}

// ----------------------------------------------------------------------------------------------------
//                                    HEALTH E MÉTRICAS
// ----------------------------------------------------------------------------------------------------
@Controller()
export class HealthController {
  constructor(@Inject(APPLICATION) private readonly application: Application) {}

  @PublicRoute()
  @Get("health/live")
  live() {
    return { status: "ok" };                                                                    // não olha dependências: banco fora não reinicia todas as instâncias
  }

  @PublicRoute()
  @Get("health/ready")
  async ready(@Res() response: Response) {
    if (this.application.shutdownState.shuttingDown) {
      sendProblem(response, 503, "SERVICE_UNAVAILABLE", "instância desligando");
      return;
    }
    const checks = { postgres: "ok", sqs: "ok" };
    try {
      await this.application.database.ping();                                                   // SELECT 1
    } catch {
      checks.postgres = "unavailable";
    }
    try {
      await approximateNumberOfMessages(this.application.sqsClient, this.application.queueUrls, this.application.config.sqs.inputQueueName);   // GetQueueAttributes
    } catch {
      checks.sqs = "unavailable";
    }
    const ready = checks.postgres === "ok" && checks.sqs === "ok";
    response.status(ready ? 200 : 503).json({ status: ready ? "ok" : "unavailable", checks });
  }

  @PublicRoute()
  @Get("metrics")
  async metrics(@Res() response: Response) {
    response.setHeader("Content-Type", metricsRegistry.contentType);
    response.send(await metricsRegistry.metrics());
  }
}
