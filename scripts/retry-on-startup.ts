// ====================================================================================================
//            NOVAS TENTATIVAS NA SUBIDA: O SERVIÇO PODE ESTAR "SAUDÁVEL" E AINDA NÃO RESPONDER
// ====================================================================================================
// Usado pelos scripts de uma execução do compose (migrations e criação das filas). Na primeira subida,
// o PostgreSQL e o MiniStack podem demorar alguns segundos a mais do que o healthcheck indica.

export async function retryOnStartup<T>(description: string, work: () => Promise<T>, timeoutMilliseconds = 60_000): Promise<T> {
  const deadline = Date.now() + timeoutMilliseconds;
  let attempt = 1;
  while (true) {
    try {
      return await work();
    } catch (error) {
      if (Date.now() >= deadline) throw error;
      console.log(`${description}: tentativa ${attempt} falhou (${(error as Error).message.split("\n")[0]}); tentando de novo em 2 s`);
      attempt = attempt + 1;
      await Bun.sleep(2_000);
    }
  }
}
