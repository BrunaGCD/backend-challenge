// ====================================================================================================
//            PRELOAD DOS TESTES DE INTEGRAÇÃO E CONCORRÊNCIA: UM BANCO NOVO POR EXECUÇÃO
// ====================================================================================================
// Os triggers do ledger impedem limpar tabelas, então cada execução cria e migra um banco próprio, e
// os testes usam ids únicos. O banco fica para inspeção depois; cada um tem nome com data e pid.
import { SQL } from "bun";                                                                      // cliente PostgreSQL do Bun, só para criar o banco
import { createOrm } from "../../src/infrastructure/persistence/orm";                          // as mesmas migrations da aplicação

process.env.LOG_LEVEL ??= "warn";                                                              // a saída do teste fica legível

const adminUrl =process.env.TEST_DATABASE_ADMIN_URL ?? process.env.DATABASE_URL ?? "postgresql://wagering:wagering@localhost:5432/wagering";
const databaseName = `wagering_test_${new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14)}_${process.pid}`;

const admin = new SQL(adminUrl);
await admin.unsafe(`create database ${databaseName}`);                                          // nome gerado aqui, sem entrada externa
await admin.close();

const testDatabaseUrl = new URL(adminUrl);
testDatabaseUrl.pathname = `/${databaseName}`;
process.env.DATABASE_URL = testDatabaseUrl.toString();                                          // todo teste e toda instância usam este banco

const orm = await createOrm({ databaseUrl: process.env.DATABASE_URL, poolMax: 2, idleInTransactionTimeoutMilliseconds: 10_000, connectTimeoutMilliseconds: 3_000, silentMigrations: true });
await orm.migrator.up();
await orm.close();

console.log(`banco de teste: ${databaseName}`);
