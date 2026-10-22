// ====================================================================================================
//                    APLICAÇÃO NESTJS: MÓDULO, MIDDLEWARES, GUARD E DESLIGAMENTO
// ====================================================================================================
import "reflect-metadata";                                                                      // metadados que o NestJS lê dos decorators
import { Inject, Injectable, Module, type BeforeApplicationShutdown, type INestApplication, type LoggerService, type OnApplicationShutdown } from "@nestjs/common";
import { APP_GUARD, NestFactory, Reflector } from "@nestjs/core";
import express, { type NextFunction, type Request, type Response } from "express";
import { isValidCorrelationId } from "../../application/wagering/wager-command";                // o mesmo formato aceito na fila
import { stopBackgroundWork, type Application } from "../../composition";
import { runWithLogContext } from "../../observability/log-context";
import { logger } from "../../observability/logger";
import { AuthGuard, NoopProviderIdentity } from "./auth";
import { ACCEPTED_CURRENCIES, CHECK_READINESS, CREATE_WALLET, HealthController, PROCESS_WAGER, WageringController, WALLET_QUERIES, WalletsController } from "./controllers";
import { ProblemDetailsFilter, sendProblem } from "./problem-details";

const APPLICATION = "APPLICATION";                                                              // só o ciclo de vida recebe a aplicação montada inteira

// passos do SIGTERM (seção 10 do desafio): readiness em 503, consumidor e laços param, HTTP fecha, pool fecha
@Injectable()
class ShutdownCoordinator implements BeforeApplicationShutdown, OnApplicationShutdown {
  constructor(@Inject(APPLICATION) private readonly application: Application) {}

  async beforeApplicationShutdown(signal?: string): Promise<void> {
    logger.info({ event: "shutdown_started", signal }, "desligando: parando consumidor, worker e publisher");
    this.application.shutdownState.shuttingDown = true;                                         // readiness passa a responder 503
    await stopBackgroundWork(this.application);
  }

  async onApplicationShutdown(): Promise<void> {
    await this.application.database.close();                                                    // depois que o HTTP terminou as requisições
    logger.info({ event: "shutdown_finished" }, "desligado");
  }
}

// logs do próprio NestJS também saem em JSON pelo pino
class PinoNestLogger implements LoggerService {
  log(message: unknown, context?: string) { logger.info({ nestContext: context }, String(message)); }
  error(message: unknown, trace?: string, context?: string) { logger.error({ nestContext: context, trace }, String(message)); }
  warn(message: unknown, context?: string) { logger.warn({ nestContext: context }, String(message)); }
  debug(message: unknown, context?: string) { logger.debug({ nestContext: context }, String(message)); }
  verbose(message: unknown, context?: string) { logger.trace({ nestContext: context }, String(message)); }
}

function correlationMiddleware(request: Request, response: Response, next: NextFunction): void {
  const header = request.header("x-correlation-id");
  const correlationId = isValidCorrelationId(header) ? header : Bun.randomUUIDv7();
  (request as Request & { correlationId: string }).correlationId = correlationId;
  response.setHeader("X-Correlation-Id", correlationId);
  runWithLogContext({ correlationId }, () => next());                                           // tudo que vem depois herda o contexto
}

function jsonParseErrorHandler(error: unknown, _request: Request, response: Response, next: NextFunction): void {
  const parseError = error as { type?: string; status?: number };
  if (parseError?.type === "entity.parse.failed") return sendProblem(response, 400, "VALIDATION_ERROR", "corpo não é JSON válido", { errors: [{ field: "body", message: "JSON inválido" }] });
  if (parseError?.type === "entity.too.large") return sendProblem(response, 400, "VALIDATION_ERROR", "corpo grande demais");
  next(error);
}

export async function createHttpApp(application: Application): Promise<INestApplication> {
  @Module({
    controllers: [WalletsController, WageringController, HealthController],
    providers: [
      { provide: CREATE_WALLET, useValue: application.createWallet },                          // cada controller recebe só o que usa
      { provide: WALLET_QUERIES, useValue: application.queries },
      { provide: PROCESS_WAGER, useValue: application.processWager },
      { provide: ACCEPTED_CURRENCIES, useValue: application.config.acceptedCurrencies },
      { provide: CHECK_READINESS, useValue: application.checkReadiness },
      { provide: APPLICATION, useValue: application },                                         // para o ShutdownCoordinator parar os laços e fechar o pool
      { provide: APP_GUARD, useFactory: (reflector: Reflector) => new AuthGuard(reflector, new NoopProviderIdentity()), inject: [Reflector] },   // troca o no-op pelo adaptador do IdP aqui
      ShutdownCoordinator,
    ],
  })
  class AppModule {}

  const app = await NestFactory.create(AppModule, { bodyParser: false, logger: new PinoNestLogger() });
  app.use(correlationMiddleware);
  app.use(express.json({ limit: "64kb" }));
  app.use(jsonParseErrorHandler);
  app.useGlobalFilters(new ProblemDetailsFilter());
  app.enableShutdownHooks();                                                                    // SIGTERM chama os hooks (E1, item 9, no Linux)
  return app;
}
