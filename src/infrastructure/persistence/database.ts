// ====================================================================================================
//                TRANSACTION RUNNER: UMA TRANSAÇÃO SQL POR EXECUÇÃO DE CASO DE USO
// ====================================================================================================
// em.transactional guarda o EntityManager da transação num AsyncLocalStorage (TransactionContext), e
// os repositórios o pegam dali. Fora de uma transação, cada leitura usa um fork novo.
import { TransactionContext, type MikroORM, type SqlEntityManager } from "@mikro-orm/postgresql";   // ORM e contexto da transação
import type { TransactionRunner } from "../../application/ports";                               // porta que a aplicação usa

const POSTGRES_INTERVAL_PATTERN = /^[0-9]+(ms|s|min)?$/;                                        // '2s', '5000ms': nada que dê para injetar SQL

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
    return this.orm.em.fork().transactional(async (transactionEntityManager) => {
      await transactionEntityManager.execute(`set local lock_timeout = '${this.timeouts.lockTimeout}'`);           // vale só para esta transação (E1, item 7)
      await transactionEntityManager.execute(`set local statement_timeout = '${this.timeouts.statementTimeout}'`);
      return work();
    });
  }

  entityManager(): SqlEntityManager {
    const insideTransaction = TransactionContext.getEntityManager(this.orm.em.name) as SqlEntityManager | undefined;
    return insideTransaction ?? (this.orm.em.fork() as SqlEntityManager);
  }

  isInsideTransaction(): boolean {
    return TransactionContext.getEntityManager(this.orm.em.name) !== undefined;
  }

  async ping(): Promise<void> {
    await this.orm.em.fork().execute("select 1");
  }

  async close(): Promise<void> {
    await this.orm.close();
  }
}
