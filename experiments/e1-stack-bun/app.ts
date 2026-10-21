// ====================================================================================================
//                E1, APLICAÇÃO MÍNIMA: NESTJS + PINO + PROM-CLIENT, PARA OS ITENS 1, 9, 10 E 11
// ====================================================================================================
// Roda com: bun app.ts <porta>. Escreve uma linha "SHUTDOWN_HOOK <sinal>" quando o hook do NestJS roda.
import "reflect-metadata";                                                                        // metadados que o NestJS lê dos decorators
import { AsyncLocalStorage } from "node:async_hooks";                                           // contexto do log
import { Controller, Get, Header, Injectable, Module, type OnApplicationShutdown } from "@nestjs/common";   // peças do NestJS
import { NestFactory } from "@nestjs/core";                                                     // sobe a aplicação
import pino from "pino";                                                                        // logger JSON
import { Counter, Histogram, Registry } from "prom-client";                                     // métricas no formato do Prometheus

const logContext = new AsyncLocalStorage<{ correlationId: string; walletId: string }>();         // campos que todo log carrega
const logger = pino({ mixin: () => logContext.getStore() ?? {} });                              // mixin lê o contexto em cada log

const registry = new Registry();
const requestsTotal = new Counter({ name: "e1_requests_total", help: "requisições recebidas", registers: [registry] });
const requestSeconds = new Histogram({ name: "e1_request_duration_seconds", help: "duração das requisições", registers: [registry] });

@Controller()
class E1Controller {
  @Get("ping")
  ping() {
    return logContext.run({ correlationId: "corr-e1", walletId: "wallet-e1" }, () => {
      const stopTimer = requestSeconds.startTimer();                                            // mede a duração
      requestsTotal.inc();
      logger.info({ event: "ping" }, "ping recebido");
      stopTimer();
      return { pong: true };
    });
  }

  @Get("metrics")
  @Header("Content-Type", "text/plain; version=0.0.4")
  metrics() {
    return registry.metrics();                                                                  // texto do Prometheus
  }
}

@Injectable()
class ShutdownWatcher implements OnApplicationShutdown {
  onApplicationShutdown(signal?: string) {
    console.log(`SHUTDOWN_HOOK ${signal}`);                                                     // prova de que o hook rodou
  }
}

@Module({ controllers: [E1Controller], providers: [ShutdownWatcher] })
class E1Module {}

const port = Number(process.argv[2] ?? 3999);
const app = await NestFactory.create(E1Module, { logger: false });
app.enableShutdownHooks();                                                                      // liga os hooks de SIGTERM/SIGINT
await app.listen(port);
console.log(`LISTENING ${port}`);
