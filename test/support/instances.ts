// ====================================================================================================
//           INSTÂNCIAS DE VERDADE: PROCESSOS SEPARADOS DA APLICAÇÃO, SUBIDOS COM BUN.SPAWN
// ====================================================================================================
// Processos separados permitem matar um deles com SIGKILL no meio do trabalho (E1, item 12).
import type { Subprocess } from "bun";
import { httpRequest, waitUntil } from "./harness";

export interface Instance {
  name: string;
  baseUrl: string;
  process: Subprocess;
  output: string[];                                                                             // stdout e stderr juntos, para conferir os logs
  exited: Promise<number>;
}

async function freePort(): Promise<number> {
  const probe = Bun.serve({ port: 0, fetch: () => new Response("") });
  const port = probe.port;
  probe.stop(true);
  return port!;
}

// lê o pipe o tempo todo: se ninguém lê, o buffer enche e o processo filho trava escrevendo log
async function collect(stream: ReadableStream<Uint8Array> | number | undefined, output: string[]): Promise<void> {
  if (!stream || typeof stream === "number") return;
  const decoder = new TextDecoder();
  for await (const chunk of stream) output.push(decoder.decode(chunk));
}

export async function spawnInstance(name: string, environment: Record<string, string>, entrypoint = "src/main.ts"): Promise<Instance> {
  const port = await freePort();
  const child = Bun.spawn([process.execPath, entrypoint], {
    env: { ...process.env, ...environment, PORT: String(port), INSTANCE_NAME: name },
    stdout: "pipe",
    stderr: "pipe",
  });
  const output: string[] = [];
  void collect(child.stdout, output);
  void collect(child.stderr, output);
  const instance: Instance = { name, baseUrl: `http://localhost:${port}`, process: child, output, exited: child.exited };
  await waitUntil(`instância ${name} pronta`, async () => {
    if (child.exitCode !== null) throw new Error(`instância ${name} saiu com ${child.exitCode}: ${output.join("").slice(-2000)}`);
    const ready = await httpRequest(instance.baseUrl, "GET", "/health/ready");
    return ready.status === 200;
  }, 30_000, 200);
  return instance;
}

export async function stopInstance(instance: Instance, signal: "SIGTERM" | "SIGKILL" = "SIGKILL"): Promise<number> {
  if (instance.process.exitCode !== null) return instance.process.exitCode;
  instance.process.kill(signal);
  return instance.exited;
}

export function instanceLogLines(instance: Instance): Record<string, unknown>[] {
  return instance.output.join("").split("\n").filter((line) => line.startsWith("{")).map((line) => {
    try { return JSON.parse(line); } catch { return { unparsed: line }; }
  });
}
