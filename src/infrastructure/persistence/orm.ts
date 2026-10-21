// ====================================================================================================
//                                  CRIAÇÃO DO MIKRORM E MIGRATIONS
// ====================================================================================================
import { FlushMode, MikroORM } from "@mikro-orm/postgresql";                                   // ORM com driver do PostgreSQL
import { Migrator } from "@mikro-orm/migrations";                                              // migrations rodadas por código
import { ALL_RECORDS } from "./records";                                                        // registros das tabelas
import { CreateWallets } from "./migrations/001-create-wallets";
import { CreateWagerTransactions } from "./migrations/002-create-wager-transactions";
import { CreateWalletLedgerEntries } from "./migrations/003-create-wallet-ledger-entries";
import { CreateInboxAndOutbox } from "./migrations/004-create-inbox-and-outbox";
import { ProtectWalletsAndLedger } from "./migrations/005-protect-wallets-and-ledger";

export interface OrmOptions {
  databaseUrl: string;
  poolMax: number;
  idleInTransactionTimeoutMilliseconds: number;
  connectTimeoutMilliseconds: number;
  silentMigrations?: boolean;                                                                   // testes não precisam do log do Migrator
}

export const MIGRATIONS = [CreateWallets, CreateWagerTransactions, CreateWalletLedgerEntries, CreateInboxAndOutbox, ProtectWalletsAndLedger];   // em ordem

export async function createOrm(options: OrmOptions): Promise<MikroORM> {
  return MikroORM.init({
    clientUrl: options.databaseUrl,
    entities: ALL_RECORDS,
    extensions: [Migrator],
    migrations: { migrationsList: MIGRATIONS, tableName: "schema_migrations", transactional: true, allOrNothing: true, snapshot: false, silent: options.silentMigrations ?? false },   // schema vem só das migrations escritas à mão
    pool: { min: 0, max: options.poolMax },
    driverOptions: {
      connectionTimeoutMillis: options.connectTimeoutMilliseconds,                              // banco fora não pode travar a requisição para sempre
      options: `-c idle_in_transaction_session_timeout=${options.idleInTransactionTimeoutMilliseconds}`,   // sessão travada no meio da transação não segura o lock
    },
    allowGlobalContext: false,                                                                  // repositório fora de contexto é erro, não uso do em global
    flushMode: FlushMode.COMMIT,                                                                // o Unit of Work só escreve no commit, nunca antes de uma consulta
    debug: false,
  });
}
