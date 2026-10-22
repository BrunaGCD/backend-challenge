// ====================================================================================================
//                               CONTROLLERS HTTP (SEÇÃO 9 DO DESAFIO)
// ====================================================================================================
// Só traduzem HTTP em chamada de caso de uso e o resultado em status e corpo. Nenhuma regra mora aqui.
import { Body, Controller, Get, Headers, HttpCode, Inject, Param, Post, Query, Req, Res } from "@nestjs/common";
import type { Request, Response } from "express";
import { IdempotencyKeyMissingError, ValidationError, type FieldError } from "../../application/errors";
import { isPlainObject, isValidIdempotencyKey, parseWagerCommand, readMoney, readUuid, rejectUnknownFields, UUID_PATTERN } from "../../application/wagering/wager-command";
import type { CreateWallet } from "../../application/wallet/create-wallet";
import type { ProcessWagerTransaction } from "../../application/wagering/process-wager-transaction";
import type { WalletQueries } from "../../application/queries";
import type { Readiness } from "../../composition";
import { metricsRegistry } from "../../observability/metrics";
import { PublicRoute } from "./auth";
import { presentLedgerEntry, presentReconciliation, presentTransaction, presentTransactionResult, presentWallet, statusCodeForTransaction } from "./presenters";
import { sendProblem } from "./problem-details";

// tokens de injeção: cada controller recebe só as peças que usa, entregues pelo módulo a partir da composição
export const CREATE_WALLET = "CREATE_WALLET";
export const WALLET_QUERIES = "WALLET_QUERIES";
export const PROCESS_WAGER = "PROCESS_WAGER";                                                   // o caso de uso com o lock_timeout do HTTP
export const ACCEPTED_CURRENCIES = "ACCEPTED_CURRENCIES";
export const CHECK_READINESS = "CHECK_READINESS";

type RequestWithContext = Request & { correlationId: string };

function pathUuid(value: string, field: string): string {
  if (!UUID_PATTERN.test(value)) throw new ValidationError([{ field, message: "precisa ser um UUID" }]);
  return value.toLowerCase();
}

// ----------------------------------------------------------------------------------------------------
//                                           WALLETS
// ----------------------------------------------------------------------------------------------------
@Controller("wallets")
export class WalletsController {
  constructor(
    @Inject(CREATE_WALLET) private readonly createWallet: CreateWallet,
    @Inject(WALLET_QUERIES) private readonly queries: WalletQueries,
    @Inject(ACCEPTED_CURRENCIES) private readonly acceptedCurrencies: string[],
  ) {}

  @Post()
  @HttpCode(201)
  async create(@Body() body: unknown, @Req() request: RequestWithContext) {
    if (!isPlainObject(body)) throw new ValidationError([{ field: "body", message: "precisa ser um objeto JSON" }]);
    const errors: FieldError[] = [];
    rejectUnknownFields(body, ["playerId", "initialBalance"], "", errors);
    const playerId = readUuid(body, "playerId", errors);
    const initialBalance = readMoney(body, "initialBalance", this.acceptedCurrencies, errors);
    if (errors.length > 0 || !initialBalance) throw new ValidationError(errors);
    const wallet = await this.createWallet.execute({ playerId, initialBalance }, request.correlationId);
    return presentWallet(wallet, false);
  }

  @Get(":walletId")
  async get(@Param("walletId") walletId: string) {
    const wallet = await this.queries.getWallet(pathUuid(walletId, "walletId"));
    return presentWallet(wallet, true);
  }

  @Get(":walletId/ledger")
  async ledger(@Param("walletId") walletId: string, @Query("cursor") cursor: string | undefined, @Query("limit") limit: string | undefined) {
    const page = await this.queries.getLedgerPage(pathUuid(walletId, "walletId"), cursor, limit);
    return { entries: page.entries.map(presentLedgerEntry), nextCursor: page.nextCursor };
  }

  @Post(":walletId/reconciliation")
  @HttpCode(200)                                                                                // divergência não é erro HTTP
  async reconcile(@Param("walletId") walletId: string) {
    const result = await this.queries.reconcile(pathUuid(walletId, "walletId"));
    return presentReconciliation(result);
  }
}

// ----------------------------------------------------------------------------------------------------
//                                        TRANSAÇÕES
// ----------------------------------------------------------------------------------------------------
@Controller()
export class WageringController {
  constructor(
    @Inject(PROCESS_WAGER) private readonly processWager: ProcessWagerTransaction,
    @Inject(WALLET_QUERIES) private readonly queries: WalletQueries,
    @Inject(ACCEPTED_CURRENCIES) private readonly acceptedCurrencies: string[],
  ) {}

  @Post("wagering/transactions")
  async submit(@Body() body: unknown, @Headers("idempotency-key") idempotencyKey: string | undefined, @Req() request: RequestWithContext, @Res({ passthrough: true }) response: Response) {
    if (idempotencyKey === undefined || idempotencyKey === "") throw new IdempotencyKeyMissingError("header Idempotency-Key é obrigatório");
    if (!isValidIdempotencyKey(idempotencyKey)) throw new ValidationError([{ field: "Idempotency-Key", message: "de 1 a 255 caracteres ASCII visíveis" }]);
    const parsed = parseWagerCommand(body, this.acceptedCurrencies);
    if (!parsed.ok) throw new ValidationError(parsed.errors);

    const result = await this.processWager.execute(parsed.value, idempotencyKey, { channel: "http", correlationId: request.correlationId, causationId: idempotencyKey });
    if (result.kind !== "decided") throw new Error("resultado de inbox no canal HTTP");      // só a fila tem inbox
    response.status(statusCodeForTransaction(result.transaction));
    return presentTransactionResult(result.transaction, result.idempotentReplay);
  }

  @Get("wagering/transactions/:transactionId")
  async get(@Param("transactionId") transactionId: string) {
    const transaction = await this.queries.getTransaction(pathUuid(transactionId, "transactionId"));
    return presentTransaction(transaction);
  }

  @Get("providers/:providerId/wagering/transactions/:externalTransactionId")
  async getByProvider(@Param("providerId") providerId: string, @Param("externalTransactionId") externalTransactionId: string) {
    const transaction = await this.queries.getTransactionByProvider(providerId, externalTransactionId);
    return presentTransaction(transaction);
  }
}

// ----------------------------------------------------------------------------------------------------
//                                    HEALTH E MÉTRICAS
// ----------------------------------------------------------------------------------------------------
@Controller()
export class HealthController {
  constructor(@Inject(CHECK_READINESS) private readonly checkReadiness: () => Promise<Readiness>) {}

  @PublicRoute()
  @Get("health/live")
  live() {
    return { status: "ok" };                                                                    // não olha dependências: banco fora não reinicia todas as instâncias
  }

  @PublicRoute()
  @Get("health/ready")
  async ready(@Res() response: Response) {
    const readiness = await this.checkReadiness();
    if (readiness.shuttingDown) {
      sendProblem(response, 503, "SERVICE_UNAVAILABLE", "instância desligando");
      return;
    }
    response.status(readiness.ready ? 200 : 503).json({ status: readiness.ready ? "ok" : "unavailable", checks: readiness.checks });
  }

  @PublicRoute()
  @Get("metrics")
  async metrics(@Res() response: Response) {
    response.setHeader("Content-Type", metricsRegistry.contentType);
    response.send(await metricsRegistry.metrics());
  }
}
