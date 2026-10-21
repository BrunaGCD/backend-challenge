// ====================================================================================================
//                LAÇO DE FUNDO: RODA UMA ITERAÇÃO, ESPERA, REPETE; PARA NO FIM DO ITEM ATUAL
// ====================================================================================================
// Usado pelo publisher da outbox, pelo worker de pendentes e pelas métricas lidas periodicamente.
import { errorSummary, logger } from "../observability/logger";

export type IterationResult = "run_again_now" | "wait";                                         // lote cheio volta na hora

export class BackgroundLoop {
  private stopping = false;
  private finished: Promise<void> = Promise.resolve();
  private wakeUp: (() => void) | undefined;

  constructor(
    private readonly name: string,
    private readonly intervalMilliseconds: number,
    private readonly iteration: () => Promise<IterationResult>,
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
      } catch (error) {
        logger.error({ event: "background_loop_iteration_failed", loop: this.name, ...errorSummary(error) }, "iteração falhou; tenta de novo no próximo intervalo");
      }
      if (result === "wait" && !this.stopping) await this.sleep();
    }
  }

  private sleep(): Promise<void> {
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, this.intervalMilliseconds);
      this.wakeUp = () => {
        clearTimeout(timer);
        resolve();
      };
    });
  }
}
