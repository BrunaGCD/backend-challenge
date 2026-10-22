// ====================================================================================================
//                     MÉTRICAS: AS SÉRIES CONHECIDAS EXISTEM DESDE A SUBIDA
// ====================================================================================================
// O texto do /metrics sai de um processo novo, porque os outros testes de unidade rodam no mesmo processo
// e podem já ter incrementado algum contador.
import { describe, expect, test } from "bun:test";
import { join } from "node:path";                                                               // caminho do módulo de métricas

const metricsModulePath = join(import.meta.dir, "../../src/observability/metrics.ts");
const printMetricsScript = `const { metricsRegistry } = await import(${JSON.stringify(metricsModulePath)}); console.log(await metricsRegistry.metrics());`;

function metricsTextRightAfterStartup(): string {
  const child = Bun.spawnSync([process.execPath, "-e", printMetricsScript]);                    // só importa o módulo, sem incrementar nada
  expect(child.exitCode).toBe(0);
  return child.stdout.toString();
}

describe("contadores de poucos labels", () => {
  test("cada label conhecido aparece com zero antes do primeiro incremento", () => {
    const metricsText = metricsTextRightAfterStartup();
    const expectedSeries = [
      'wagering_duplicates_total{source="idempotency_key"}',
      'wagering_duplicates_total{source="inbox"}',
      'wagering_retries_total{component="sqs_consumer"}',
      'wagering_retries_total{component="pending_reference"}',
      'wagering_retries_total{component="outbox"}',
      'wagering_concurrency_conflicts_total{type="lock_timeout"}',
      'wagering_concurrency_conflicts_total{type="deadlock"}',
      'wagering_concurrency_conflicts_total{type="unique_violation"}',
      'wagering_concurrency_conflicts_total{type="optimistic_lock"}',
      'wagering_idempotency_conflicts_total{type="idempotency_key"}',
      'wagering_idempotency_conflicts_total{type="external_transaction_id"}',
      'wagering_consumer_pauses_total{reason="database_unavailable"}',
    ];
    for (const series of expectedSeries) expect(metricsText).toContain(`${series} 0\n`);
  });

  test("transactions_total e dlq_forwarded_total continuam sem série antes do primeiro incremento", () => {
    const metricsText = metricsTextRightAfterStartup();
    expect(metricsText).not.toMatch(/^wagering_transactions_total\{/m);
    expect(metricsText).not.toMatch(/^wagering_dlq_forwarded_total\{/m);
  });
});
