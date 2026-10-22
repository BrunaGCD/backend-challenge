// ====================================================================================================
//                LAÇO DE FUNDO: RODA UMA ITERAÇÃO, ESPERA, REPETE; PARA NO FIM DO ITEM ATUAL
// ====================================================================================================
// Usado pelo publisher da outbox, pelo worker de pendentes e pelas métricas lidas periodicamente.
// Falhas seguidas (o banco fora, por exemplo) dobram a espera até um teto, e o primeiro sucesso volta ao
// intervalo normal. O log de erro sai na primeira falha da sequência, e o de volta no primeiro sucesso:
// numa queda, uma linha de cada por laço, em vez de uma a cada intervalo.
import { errorSummary, logger } from "../observability/logger";

export type IterationResult = "run_again_now" | "wait";                                         // lote cheio volta na hora

export function waitAfterFailuresMilliseconds(intervalMilliseconds: number, consecutiveFailures: number, capMilliseconds: number): number {
  if (consecutiveFailures === 0) return intervalMilliseconds;                                   // tudo certo: o intervalo normal
  const doubledMilliseconds = intervalMilliseconds * 2 ** consecutiveFailures;                  // dobra a cada falha seguida
  return Math.max(intervalMilliseconds, Math.min(capMilliseconds, doubledMilliseconds));        // até o teto, e nunca menos que o normal
}

export class BackgroundLoop {
  private stopping = false;
  private finished: Promise<void> = Promise.resolve();
  private wakeUp: (() => void) | undefined;
  private consecutiveFailures = 0;                                                              // falhas seguidas desde o último sucesso
  private failingSince = 0;                                                                     // quando a sequência de falhas começou

  constructor(
    private readonly name: string,
    private readonly intervalMilliseconds: number,
    private readonly iteration: () => Promise<IterationResult>,
    private readonly failureCapMilliseconds: number,                                            // a maior espera depois de falhas seguidas
  ) {}

  start(): void {
    this.finished = this.run();
  }

  async stop(): Promise<void> {
    this.stopping = true;
    this.wakeUp?.();                                                                            // acorda quem está dormindo no intervalo
    await this.finished;                                                                        // termina o item atual e para
  }

  private async run(): Promise<void> {
    while (!this.stopping) {
      let result: IterationResult = "wait";
      try {
        result = await this.iteration();
        this.recordSuccess();
      } catch (error) {
        this.recordFailure(error);
      }
      const waitMilliseconds = waitAfterFailuresMilliseconds(this.intervalMilliseconds, this.consecutiveFailures, this.failureCapMilliseconds);
      if (result === "wait" && !this.stopping) await this.sleep(waitMilliseconds);
    }
  }

  private recordSuccess(): void {
    if (this.consecutiveFailures === 0) return;                                                 // não estava falhando: nada a dizer
    logger.info({ event: "background_loop_recovered", loop: this.name, failedIterations: this.consecutiveFailures, failingForMilliseconds: Date.now() - this.failingSince }, "laço de fundo voltou a funcionar");
    this.consecutiveFailures = 0;
  }

  private recordFailure(error: unknown): void {
    this.consecutiveFailures += 1;
    if (this.consecutiveFailures === 1) {
      this.failingSince = Date.now();
      logger.error({ event: "background_loop_iteration_failed", loop: this.name, ...errorSummary(error) }, "iteração falhou; tenta de novo com espera crescente, e só avisa de novo quando voltar");
      return;
    }
    logger.debug({ event: "background_loop_iteration_failed", loop: this.name, consecutiveFailures: this.consecutiveFailures, ...errorSummary(error) }, "iteração falhou de novo");   // as seguintes só aparecem com LOG_LEVEL=debug
  }

  private sleep(milliseconds: number): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, milliseconds);
      this.wakeUp = () => {
        clearTimeout(timer);
        resolve();
      };
    });
  }
}
