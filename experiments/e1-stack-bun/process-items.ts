// ====================================================================================================
//            E1, ITENS 1, 9, 10, 11 E 12: SOBE A APP COMO PROCESSO FILHO E CONFERE CADA ITEM
// ====================================================================================================
// Roda com: bun process-items.ts
const results: { item: string; passed: boolean; detail: string }[] = [];

function record(item: string, passed: boolean, detail: string) {
  results.push({ item, passed, detail });
  console.log(`[${passed ? "PASSOU" : "FALHOU"}] ${item}: ${detail}`);
}

async function readUntil(stream: ReadableStream<Uint8Array>, collected: string[], marker: string, timeoutMilliseconds: number) {
  const reader = stream.getReader();
  const decoder = new TextDecoder();
  const deadline = Date.now() + timeoutMilliseconds;
  while (Date.now() < deadline) {
    const chunk = await Promise.race([reader.read(), Bun.sleep(deadline - Date.now()).then(() => null)]);
    if (chunk === null || chunk.done) break;
    collected.push(decoder.decode(chunk.value));
    if (collected.join("").includes(marker)) break;
  }
  reader.releaseLock();
  return collected.join("").includes(marker);
}

// ----------------------------------------------------------------------------------------------------
//                       ITENS 1, 10, 11 E 9: ROTA, LOG JSON, MÉTRICAS E SIGTERM
// ----------------------------------------------------------------------------------------------------
const firstPort = 3991;
const firstApp = Bun.spawn(["bun", "app.ts", String(firstPort)], { stdout: "pipe", stderr: "pipe" });
const firstOutput: string[] = [];
const firstStarted = await readUntil(firstApp.stdout, firstOutput, "LISTENING", 20000);

if (!firstStarted) {
  record("1 NestJS sobe e responde", false, `não subiu: ${firstOutput.join("")} ${await new Response(firstApp.stderr).text()}`);
} else {
  const pingResponse = await fetch(`http://localhost:${firstPort}/ping`);
  const pingBody = await pingResponse.json();
  record("1 NestJS sobe e responde", pingResponse.status === 200 && pingBody.pong === true, `status ${pingResponse.status}, corpo ${JSON.stringify(pingBody)}`);

  await readUntil(firstApp.stdout, firstOutput, "ping recebido", 5000);
  const logLine = firstOutput.join("").split("\n").find((line) => line.includes("ping recebido")) ?? "";
  let parsedLog: Record<string, unknown> = {};
  try { parsedLog = JSON.parse(logLine); } catch {}
  record("10 pino escreve JSON com os campos do mixin", parsedLog.correlationId === "corr-e1" && parsedLog.walletId === "wallet-e1", `linha: ${logLine.trim()}`);

  const metricsResponse = await fetch(`http://localhost:${firstPort}/metrics`);
  const metricsText = await metricsResponse.text();
  const hasCounter = /e1_requests_total 1/.test(metricsText);
  const hasHistogram = /e1_request_duration_seconds_count 1/.test(metricsText);
  record("11 prom-client: contador, histograma e /metrics em texto", hasCounter && hasHistogram, `contador: ${hasCounter}, histograma: ${hasHistogram}, content-type: ${metricsResponse.headers.get("content-type")}`);

  firstApp.kill("SIGTERM");
  const hookRan = await readUntil(firstApp.stdout, firstOutput, "SHUTDOWN_HOOK", 10000);
  const exitCode = await Promise.race([firstApp.exited, Bun.sleep(5000).then(() => "não saiu")]);
  const hookLine = firstOutput.join("").split("\n").find((line) => line.includes("SHUTDOWN_HOOK")) ?? "(nenhuma)";
  record("9 SIGTERM dispara os hooks de desligamento do NestJS", hookRan, `linha: ${hookLine.trim()}, saída do processo: ${exitCode}, signalCode: ${firstApp.signalCode}`);
}

// ----------------------------------------------------------------------------------------------------
//                          ITEM 12: BUN.SPAWN SOBE OUTRO PROCESSO E O SIGKILL MATA
// ----------------------------------------------------------------------------------------------------
const secondPort = 3992;
const secondApp = Bun.spawn(["bun", "app.ts", String(secondPort)], { stdout: "pipe", stderr: "pipe" });
const secondOutput: string[] = [];
const secondStarted = await readUntil(secondApp.stdout, secondOutput, "LISTENING", 20000);
const answeredBeforeKill = secondStarted && (await fetch(`http://localhost:${secondPort}/ping`)).status === 200;
secondApp.kill("SIGKILL");
const secondExit = await Promise.race([secondApp.exited, Bun.sleep(5000).then(() => "não saiu")]);
let answeredAfterKill = true;
try { await fetch(`http://localhost:${secondPort}/ping`); } catch { answeredAfterKill = false; }
const hookRanOnKill = secondOutput.join("").includes("SHUTDOWN_HOOK");
record("12 Bun.spawn sobe outro processo e o pai mata com SIGKILL", answeredBeforeKill && !answeredAfterKill && !hookRanOnKill, `respondia antes: ${answeredBeforeKill}, responde depois: ${answeredAfterKill}, saída: ${secondExit}, signalCode: ${secondApp.signalCode}, hook rodou: ${hookRanOnKill}`);

console.log(JSON.stringify({ runtime: `bun ${Bun.version} ${process.platform}`, results }, null, 2));
