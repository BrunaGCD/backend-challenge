// ====================================================================================================
//      ENTRADA SÓ DE TESTE: A APLICAÇÃO NORMAL, COM UMA FILA QUE MATA O PROCESSO ANTES DO ACK
// ====================================================================================================
// O cenário "worker morto depois do commit e antes do ack" (seção 13). O commit já aconteceu quando o
// consumidor chama o delete; aqui o delete vira um SIGKILL no próprio processo. Banco e fila são os
// reais: a falha entra pela composição das portas, não por um "modo de teste" no código de produção.
import { startServer } from "../../src/main";

await startServer({
  inputQueue: (real) => ({
    receive: (maxMessages, waitTimeSeconds, abortSignal) => real.receive(maxMessages, waitTimeSeconds, abortSignal),
    changeVisibility: (message, seconds) => real.changeVisibility(message, seconds),
    sendToDeadLetter: (message, reason) => real.sendToDeadLetter(message, reason),
    delete: async () => {
      console.log(JSON.stringify({ event: "crash_before_ack", msg: "commit feito; morrendo antes do DeleteMessage" }));
      process.kill(process.pid, "SIGKILL");
      await new Promise(() => {});                                                              // nunca volta
    },
  }),
});
