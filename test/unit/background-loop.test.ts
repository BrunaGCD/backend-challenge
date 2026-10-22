// ====================================================================================================
//            LAÇO DE FUNDO: ESPERA CRESCENTE DEPOIS DE FALHAS SEGUIDAS E UM LOG POR QUEDA
// ====================================================================================================
import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { BackgroundLoop, waitAfterFailuresMilliseconds } from "../../src/infrastructure/background-loop";
import { logger } from "../../src/observability/logger";                                        // os logs que o laço escreve

describe("espera depois de falhas seguidas", () => {
  test("sem falha é o intervalo normal; cada falha seguida dobra, até o teto", () => {
    const waits = [0, 1, 2, 3, 4, 5, 6, 30].map((failures) => waitAfterFailuresMilliseconds(500, failures, 10_000));
    expect(waits).toEqual([500, 1000, 2000, 4000, 8000, 10_000, 10_000, 10_000]);
  });

  test("um teto menor que o intervalo nunca encurta a espera normal", () => {
    expect(waitAfterFailuresMilliseconds(5000, 1, 1000)).toBe(5000);
  });
});

describe("laço de fundo numa queda", () => {
  const spies: { mockRestore: () => void }[] = [];
  afterEach(() => {
    for (const spy of spies.splice(0)) spy.mockRestore();
  });

  test("falha 4 vezes e volta: espera cada vez mais, um log de erro na primeira falha e um de volta no sucesso", async () => {
    const errorLogs = spyOn(logger, "error").mockImplementation(() => undefined);
    const infoLogs = spyOn(logger, "info").mockImplementation(() => undefined);
    spies.push(errorLogs, infoLogs);

    const intervalMilliseconds = 10;
    const capMilliseconds = 40;
    const failuresBeforeRecovery = 4;
    const iterationStartedAt: number[] = [];                                                    // quando cada iteração começou
    let reachedSixIterations: () => void = () => undefined;
    const sixIterations = new Promise<void>((resolve) => (reachedSixIterations = resolve));

    const loop = new BackgroundLoop("test_loop", intervalMilliseconds, async () => {
      iterationStartedAt.push(performance.now());
      if (iterationStartedAt.length === 6) reachedSixIterations();
      if (iterationStartedAt.length <= failuresBeforeRecovery) throw new Error("connect ECONNREFUSED");   // o banco fora nas 4 primeiras
      return "wait";
    }, capMilliseconds);
    loop.start();
    await sixIterations;
    await loop.stop();

    const gaps = iterationStartedAt.slice(1, 6).map((startedAt, index) => startedAt - iterationStartedAt[index]!);
    const expectedGaps = [20, 40, 40, 40, 10];                                                  // 10 × 2, 10 × 4, depois o teto, e o normal depois do sucesso
    gaps.forEach((gap, index) => expect(gap).toBeGreaterThanOrEqual(expectedGaps[index]! - 2));   // o setTimeout pode adiantar 1 ms

    const failureLogs = errorLogs.mock.calls.filter(([fields]) => (fields as { event?: string }).event === "background_loop_iteration_failed");
    expect(failureLogs).toHaveLength(1);                                                        // só a primeira falha da sequência
    const recoveryLogs = infoLogs.mock.calls.filter(([fields]) => (fields as { event?: string }).event === "background_loop_recovered");
    expect(recoveryLogs).toHaveLength(1);
    expect(recoveryLogs[0]![0]).toMatchObject({ loop: "test_loop", failedIterations: failuresBeforeRecovery });
  });

  test("uma nova queda depois da volta gera um novo log de erro", async () => {
    const errorLogs = spyOn(logger, "error").mockImplementation(() => undefined);
    const infoLogs = spyOn(logger, "info").mockImplementation(() => undefined);
    spies.push(errorLogs, infoLogs);

    const outcomes = ["fail", "fail", "ok", "fail", "ok"];                                      // duas quedas separadas por um sucesso
    let iterations = 0;
    let reachedTheEnd: () => void = () => undefined;
    const allOutcomes = new Promise<void>((resolve) => (reachedTheEnd = resolve));

    const loop = new BackgroundLoop("test_loop", 1, async () => {
      const outcome = outcomes[iterations] ?? "ok";
      iterations += 1;
      if (iterations === outcomes.length) reachedTheEnd();
      if (outcome === "fail") throw new Error("connect ECONNREFUSED");
      return "wait";
    }, 4);
    loop.start();
    await allOutcomes;
    await loop.stop();

    expect(errorLogs.mock.calls.filter(([fields]) => (fields as { event?: string }).event === "background_loop_iteration_failed")).toHaveLength(2);
    expect(infoLogs.mock.calls.filter(([fields]) => (fields as { event?: string }).event === "background_loop_recovered")).toHaveLength(2);
  });
});
