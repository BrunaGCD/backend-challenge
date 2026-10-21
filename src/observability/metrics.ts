// ====================================================================================================
//                         MÉTRICAS (PROM-CLIENT) DA SEÇÃO 12
// ====================================================================================================
// Unidade base em segundos, _total nos contadores, e nenhum label de alta cardinalidade (walletId não
// entra em label). O E1 (item 11) confirmou contador, histograma e o texto do /metrics no Bun.
import { Counter, Gauge, Histogram, Registry } from "prom-client";                             // tipos de métrica

export const metricsRegistry = new Registry();
const registers = [metricsRegistry];

export const transactionsTotal = new Counter({ name: "wagering_transactions_total", help: "Transações decididas, por tipo, status e canal de entrada", labelNames: ["kind", "status", "channel"], registers });
export const duplicatesTotal = new Counter({ name: "wagering_duplicates_total", help: "Duplicatas detectadas (replay pela idempotency key ou mensagem já vista na inbox)", labelNames: ["source"], registers });
export const idempotencyConflictsTotal = new Counter({ name: "wagering_idempotency_conflicts_total", help: "Mesma idempotency key (ou mesmo externalTransactionId) com payload diferente", labelNames: ["type"], registers });
export const retriesTotal = new Counter({ name: "wagering_retries_total", help: "Novas tentativas agendadas, por componente", labelNames: ["component"], registers });
export const dlqMessages = new Gauge({ name: "wagering_dlq_messages", help: "Mensagens na DLQ (ApproximateNumberOfMessages)", registers });
export const dlqForwardedTotal = new Counter({ name: "wagering_dlq_forwarded_total", help: "Mensagens enviadas explicitamente para a DLQ, por motivo", labelNames: ["reason"], registers });
export const walletLockWaitSeconds = new Histogram({ name: "wagering_wallet_lock_wait_seconds", help: "Tempo do SELECT ... FOR UPDATE da wallet (espera pelo lock)", buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5], registers });
export const concurrencyConflictsTotal = new Counter({ name: "wagering_concurrency_conflicts_total", help: "Conflitos de concorrência que viraram erro", labelNames: ["type"], registers });
export const outboxLagSeconds = new Gauge({ name: "wagering_outbox_lag_seconds", help: "Idade do evento não publicado mais antigo", registers });
export const outboxPending = new Gauge({ name: "wagering_outbox_pending", help: "Eventos ainda não publicados", registers });
export const outboxStuck = new Gauge({ name: "wagering_outbox_stuck", help: "Eventos não publicados com muitas tentativas", registers });
export const outboxPublishDelaySeconds = new Histogram({ name: "wagering_outbox_publish_delay_seconds", help: "published_at - occurred_at", buckets: [0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300], registers });
export const processingDurationSeconds = new Histogram({ name: "wagering_processing_duration_seconds", help: "Duração do processamento de uma transação, por canal e resultado", labelNames: ["channel", "outcome"], buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5], registers });
export const reconciliationDivergencesTotal = new Counter({ name: "wagering_reconciliation_divergences_total", help: "Reconciliações que encontraram saldo diferente do ledger", registers });
export const pendingReferenceGauge = new Gauge({ name: "wagering_pending_reference", help: "Transações em PENDING_REFERENCE", registers });
export const inboxPayloadConflictsTotal = new Counter({ name: "wagering_inbox_payload_conflicts_total", help: "Mesmo messageId com conteúdo diferente", registers });
