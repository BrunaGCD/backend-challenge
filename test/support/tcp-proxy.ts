// ====================================================================================================
//        PROXY TCP DE TESTE: FICA NA FRENTE DO POSTGRESQL OU DO MINISTACK REAIS E PODE SER CORTADO
// ====================================================================================================
// Cortar o proxy derruba as conexões abertas e recusa as novas, como uma queda de rede ou do serviço;
// restaurar volta a encaminhar. O banco e a fila por trás continuam os reais.
import net from "node:net";

export class TcpProxy {
  private server: net.Server | undefined;
  private readonly openSockets = new Set<net.Socket>();
  port = 0;

  constructor(private readonly targetHost: string, private readonly targetPort: number) {}

  async start(): Promise<void> {
    const server = net.createServer((client) => {
      const upstream = net.connect(this.targetPort, this.targetHost);
      this.openSockets.add(client);
      this.openSockets.add(upstream);
      client.pipe(upstream);
      upstream.pipe(client);
      const closeBoth = () => {
        client.destroy();
        upstream.destroy();
        this.openSockets.delete(client);
        this.openSockets.delete(upstream);
      };
      client.on("error", closeBoth);
      upstream.on("error", closeBoth);
      client.on("close", closeBoth);
      upstream.on("close", closeBoth);
    });
    await new Promise<void>((resolve) => server.listen(this.port, "127.0.0.1", resolve));
    this.port = (server.address() as net.AddressInfo).port;                                     // na primeira vez, uma porta livre; depois, a mesma
    this.server = server;
  }

  async cut(): Promise<void> {
    const server = this.server;
    this.server = undefined;
    for (const socket of this.openSockets) socket.destroy();                                    // derruba as conexões abertas
    this.openSockets.clear();
    if (server) await new Promise<void>((resolve) => server.close(() => resolve()));            // e para de aceitar novas
  }

  async restore(): Promise<void> {
    await this.start();
  }
}
