// ====================================================================================================
//                TRANSACTION RUNNER: UMA TRANSAÇÃO SQL POR EXECUÇÃO DE CASO DE USO
// ====================================================================================================
// em.transactional guarda o EntityManager da transação num AsyncLocalStorage (TransactionContext), e
// os repositórios o pegam dali. Dentro da transação, os repositórios só registram no Unit of Work
// (persist de registro novo, mudança em registro carregado); o transactional faz o flush no commit,
// com o FlushMode.COMMIT, e é ali que aparecem a violação de unicidade e o conflito de version.
// Fora de uma transação, cada leitura usa um fork novo.
import { OptimisticLockError, TransactionContext, type EntityName, type MikroORM, type Primary, type SqlEntityManager } from "@mikro-orm/postgresql";   // ORM e contexto da transação
import { ConcurrentUpdateError } from "../../application/errors";                               // o conflito do flush, com o nome da aplicação
import type { TransactionRunner } from "../../application/ports";                               // porta que a aplicação usa
import { logger } from "../../observability/logger";                                            // aviso quando o pool não fecha

const POSTGRES_INTERVAL_PATTERN = /^[0-9]+(ms|s|min)?$/;                                        // '2s', '5000ms': nada que dê para injetar SQL
const SCHEMA_CHECK_SQL = "select 1 from wallets, wager_transactions, wallet_ledger_entries, inbox_messages, outbox_messages limit 0";   // confere as 5 tabelas sem ler nenhuma linha
const UNDEFINED_TABLE = "42P01";                                                                // SQLSTATE de tabela que não existe
const POOL_CLOSE_TIMEOUT_MILLISECONDS = 5_000;                                                  // dentro dos 30 s de stop_grace_period do compose

export type SchemaStatus = "ok" | "unavailable" | "schema_missing";

export interface TransactionTimeouts {
  lockTimeout: string;
  statementTimeout: string;
}

export class MikroOrmDatabase implements TransactionRunner {
  constructor(
    private readonly orm: MikroORM,
    private readonly timeouts: TransactionTimeouts,
  ) {
    for (const value of [timeouts.lockTimeout, timeouts.statementTimeout]) {
      if (!POSTGRES_INTERVAL_PATTERN.test(value)) throw new Error(`timeout inválido: ${value}`);
    }
  }

  async run<T>(work: () => Promise<T>): Promise<T> {
    try {
      return await this.orm.em.fork().transactional(async (transactionEntityManager) => {
        await transactionEntityManager.execute(`set local lock_timeout = '${this.timeouts.lockTimeout}'`);           // vale só para esta transação (E1, item 7)
        await transactionEntityManager.execute(`set local statement_timeout = '${this.timeouts.statementTimeout}'`);
        return work();                                                                          // o flush do Unit of Work sai no commit, depois daqui
      });
    } catch (error) {
      if (error instanceof OptimisticLockError) throw new ConcurrentUpdateError(`o registro mudou entre a leitura e o commit: ${error.message}`);   // UPDATE ... WHERE version/status com 0 linhas
      throw error;
    }
  }

  entityManager(): SqlEntityManager {
    const insideTransaction = TransactionContext.getEntityManager(this.orm.em.name) as SqlEntityManager | undefined;
    return insideTransaction ?? (this.orm.em.fork() as SqlEntityManager);
  }

  // escrita só dentro de transação: num fork solto o persist nunca teria flush e se perderia em silêncio
  transactionEntityManager(): SqlEntityManager {
    const insideTransaction = TransactionContext.getEntityManager(this.orm.em.name) as SqlEntityManager | undefined;
    if (!insideTransaction) throw new Error("escrita fora de uma transação");
    return insideTransaction;
  }

  // o registro que esta transação já carregou, direto do Identity Map, sem SQL; mudar ele vira UPDATE no flush
  loadedRecord<T extends object>(record: EntityName<T>, id: string): T {
    const loaded = this.transactionEntityManager().getUnitOfWork().getById<T>(record, id as Primary<T>);
    if (!loaded) throw new Error(`o registro ${id} não foi carregado nesta transação: é preciso ler (com lock) antes de alterar`);
    return loaded;
  }

  isInsideTransaction(): boolean {
    return TransactionContext.getEntityManager(this.orm.em.name) !== undefined;
  }

  // banco de pé mas sem as tabelas (migrations não rodaram) não está pronto para atender
  async schemaStatus(): Promise<SchemaStatus> {
    try {
      await this.orm.em.fork().execute(SCHEMA_CHECK_SQL);
      return "ok";
    } catch (error) {
      const code = (error as { code?: unknown }).code;
      return code === UNDEFINED_TABLE ? "schema_missing" : "unavailable";
    }
  }

  // O pool.end() do pg espera as conexões emprestadas voltarem. Se o banco cai no meio de uma transação,
  // o rollback (ou o BEGIN) falha, e sem o patch do kysely (patches/) aquela conexão nunca voltava. Com o
  // patch ela volta; o limite fica como segunda proteção, para o desligamento nunca ficar preso nela.
  async close(): Promise<void> {
    const closed = this.orm.close().then(() => "closed" as const);
    const timedOut = Bun.sleep(POOL_CLOSE_TIMEOUT_MILLISECONDS).then(() => "timed_out" as const);
    if ((await Promise.race([closed, timedOut])) === "timed_out") {
      logger.warn({ event: "database_close_timeout", waitedMilliseconds: POOL_CLOSE_TIMEOUT_MILLISECONDS }, "o pool não fechou a tempo: sobrou conexão emprestada; seguindo com o desligamento");
    }
  }
}
