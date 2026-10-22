// ====================================================================================================
//          PRAZO DE VISIBILIDADE DAS MENSAGENS DE UMA WALLET ENQUANTO ELAS ESTÃO COM O CONSUMIDOR
// ====================================================================================================
// As mensagens de um grupo (wallet) ficam com o consumidor do recebimento até a última terminar. Estender
// o prazo não faz o SQS entregar a mensagem de novo, então não gasta recebimento; devolver com
// visibilidade 0, ou deixar o prazo vencer, gasta. O prazo é estendido em dois momentos:
//   antes de começar cada mensagem, se não cabe mais uma espera inteira no que sobrou (R4-B);
//   a qualquer momento em que o grupo está em mãos, se sobra menos de um terço do prazo, porque a conta
//   do "cabe mais uma" é uma estimativa (não inclui a espera por conexão do pool, os vários comandos da
//   transação nem o DeleteMessage) e porque uma mensagem pode estar esperando a próxima tentativa.
import { errorSummary, logger } from "../../observability/logger";
import type { InputQueue, ReceivedMessage } from "./sqs";

export class GroupVisibility {
  private visibilityStartedAt: number;                                                          // de onde o prazo atual conta; cada extensão recomeça a contagem
  private readonly unfinished: ReceivedMessage[];                                               // a mensagem atual e as seguintes do grupo
  private extensionInProgress: Promise<boolean> | undefined;
  private watchTimer: ReturnType<typeof setInterval> | undefined;

  constructor(
    private readonly queue: InputQueue,
    messages: ReceivedMessage[],
    receivedAt: number,
    private readonly visibilityMilliseconds: number,
  ) {
    this.visibilityStartedAt = receivedAt;
    this.unfinished = [...messages];
  }

  hasLeft(milliseconds: number): boolean {
    const elapsedMilliseconds = performance.now() - this.visibilityStartedAt;                   // quanto do prazo já foi gasto
    return this.visibilityMilliseconds - elapsedMilliseconds >= milliseconds;
  }

  // confere a cada sexto do prazo; estende quando sobra menos de um terço
  startWatching(): void {
    const minimumLeftMilliseconds = this.visibilityMilliseconds / 3;
    this.watchTimer = setInterval(() => {
      if (!this.hasLeft(minimumLeftMilliseconds)) void this.extend();                          // a falha já sai no log; tenta de novo na próxima conferência
    }, this.visibilityMilliseconds / 6);
  }

  stopWatching(): void {
    clearInterval(this.watchTimer);
  }

  finish(message: ReceivedMessage): void {
    const index = this.unfinished.indexOf(message);
    if (index >= 0) this.unfinished.splice(index, 1);                                           // terminada: o prazo dela não interessa mais
  }

  // uma extensão por vez: quem pede enquanto outra está em andamento espera a mesma
  async extend(): Promise<boolean> {
    this.extensionInProgress ??= this.extendUnfinished().finally(() => { this.extensionInProgress = undefined; });
    return this.extensionInProgress;
  }

  // devolve com visibilidade 0 as que não terminaram (desligamento, ou extensão que falhou)
  async release(): Promise<void> {
    this.stopWatching();                                                                        // senão uma extensão chegaria depois e esconderia a mensagem de novo
    await this.extensionInProgress;
    let released = 0;
    for (const message of this.unfinished) {
      try {
        await this.queue.changeVisibility(message, 0);
        released = released + 1;
      } catch (error) {
        logger.warn({ event: "sqs_release_failed", ...errorSummary(error) }, "não deu para devolver a visibilidade; a mensagem volta quando o timeout vencer");
      }
    }
    if (released > 0) logger.info({ event: "sqs_messages_released", released }, "mensagens do grupo devolvidas para a fila com visibilidade 0 (cada uma gasta um recebimento)");
  }

  private async extendUnfinished(): Promise<boolean> {
    const messages = [...this.unfinished];
    if (messages.length === 0) return true;
    const extensionStartedAt = performance.now();                                               // medido antes das chamadas: o prazo novo nunca acaba antes do que o consumidor acha
    const fullVisibilitySeconds = Math.floor(this.visibilityMilliseconds / 1_000);              // o prazo cheio da fila, em segundos
    try {
      for (const message of messages) await this.queue.changeVisibility(message, fullVisibilitySeconds);
    } catch (error) {
      logger.warn({ event: "sqs_visibility_extension_failed", ...errorSummary(error) }, "não deu para estender o prazo de visibilidade das mensagens que faltam do grupo");
      return false;
    }
    this.visibilityStartedAt = extensionStartedAt;
    logger.info({ event: "sqs_visibility_extended", extended: messages.length, visibilitySeconds: fullVisibilitySeconds }, "prazo de visibilidade estendido para as mensagens que faltam do grupo");
    return true;
  }
}
