// ====================================================================================================
//                       RODA AS MIGRATIONS: bun scripts/migrate.ts up | down
// ====================================================================================================
import { loadConfig } from "../src/config";                                                    // DATABASE_URL e pool
import { createOrm } from "../src/infrastructure/persistence/orm";                              // ORM com o Migrator
import { retryOnStartup } from "./retry-on-startup";                                            // banco recém-subido pode demorar

const direction = process.argv[2];
if (direction !== "up" && direction !== "down") {
  console.error("uso: bun scripts/migrate.ts up | down");
  process.exit(1);
}

const config = loadConfig();
const orm = await createOrm({ databaseUrl: config.database.url, poolMax: 2, idleInTransactionTimeoutMilliseconds: config.database.idleInTransactionTimeoutMilliseconds, connectTimeoutMilliseconds: config.database.connectTimeoutMilliseconds });
await retryOnStartup("conexão com o PostgreSQL", () => orm.em.fork().execute("select 1"));
const executed = direction === "up" ? await orm.migrator.up() : await orm.migrator.down();     // down desfaz só a última
console.log(`migrations ${direction}:`, executed.map((migration) => migration.name).join(", ") || "nada a fazer");
await orm.close();
