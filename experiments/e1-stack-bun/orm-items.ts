// ====================================================================================================
//                       E1, ITENS 2 A 8: MIKRORM 7 + POSTGRESQL RODANDO NO BUN
// ====================================================================================================
// Roda com: bun orm-items.ts   (precisa do PostgreSQL do docker compose em localhost:5432)
import { AsyncLocalStorage } from "node:async_hooks";                                           // contexto que atravessa os await
import { MikroORM, defineEntity, p, LockMode } from "@mikro-orm/postgresql";                    // ORM e construtor de registros
import { Migrator, Migration } from "@mikro-orm/migrations";                                    // migrations rodadas por código

const results: { item: string; passed: boolean; detail: string }[] = [];                        // passou/falhou de cada item
const executedSql: string[] = [];                                                               // SQL que o ORM mandou, para conferir o "for update"

function record(item: string, passed: boolean, detail: string) {
  results.push({ item, passed, detail });
  console.log(`[${passed ? "PASSOU" : "FALHOU"}] ${item}: ${detail}`);
}

// ----------------------------------------------------------------------------------------------------
//                                   REGISTRO E MIGRATION DE TESTE
// ----------------------------------------------------------------------------------------------------
const E1Wallet = defineEntity({
  name: "E1Wallet",
  tableName: "e1_wallets",
  properties: {
    id: p.uuid().primary(),                                                                     // id gerado na aplicação
    balance: p.decimal().precision(18).scale(2),                                                // valor exato, sem float
    currency: p.string().length(3),                                                             // moeda ISO
  },
});

class CreateE1Wallets extends Migration {
  name = "CreateE1Wallets";                                                                     // nome estável da migration
  up() {
    this.addSql(`create table e1_wallets (id uuid primary key, balance numeric(18,2) not null check (balance >= 0), currency char(3) not null check (currency ~ '^[A-Z]{3}$'))`);
    this.addSql(`create function e1_forbid_delete() returns trigger language plpgsql as $$ begin raise exception 'e1_wallets: delete proibido'; end $$`);
    this.addSql(`create trigger e1_wallets_no_delete before delete on e1_wallets for each row execute function e1_forbid_delete()`);
  }
  down() {
    this.addSql(`drop trigger e1_wallets_no_delete on e1_wallets`);
    this.addSql(`drop function e1_forbid_delete()`);
    this.addSql(`drop table e1_wallets`);
  }
}

const connectionOptions = {
  clientUrl: process.env.DATABASE_URL ?? "postgresql://wagering:wagering@localhost:5432/wagering",                        // banco do compose
  entities: [E1Wallet],
  extensions: [Migrator],                                                                       // habilita orm.migrator
  migrations: { migrationsList: [CreateE1Wallets], tableName: "e1_migrations" },               // migrations definidas em código
  debug: true,                                                                                  // liga o log de SQL
  loggerFactory: () => ({
    log: (_namespace: string, message: string) => { executedSql.push(message); },
    error: (_namespace: string, message: string) => { executedSql.push(message); },
    warn: (_namespace: string, message: string) => { executedSql.push(message); },
    logQuery: (context: { query: string }) => { executedSql.push(context.query); },
    setDebugMode: () => {},
    isEnabled: () => true,
  }),
};

const orm = await MikroORM.init(connectionOptions as any);

// ----------------------------------------------------------------------------------------------------
//                          ITEM 6: MIGRATION COM CHECK E TRIGGER, UP E DOWN
// ----------------------------------------------------------------------------------------------------
try {
  await orm.migrator.up();                                                                      // cria a tabela
  const tableAfterUp = await orm.em.getConnection().execute(`select to_regclass('e1_wallets') as name`);
  await orm.migrator.down();                                                                    // desfaz
  const tableAfterDown = await orm.em.getConnection().execute(`select to_regclass('e1_wallets') as name`);
  await orm.migrator.up();                                                                      // e sobe de novo para o resto do roteiro
  const passed = tableAfterUp[0].name === "e1_wallets" && tableAfterDown[0].name === null;
  record("6 migration up/down por código", passed, `depois do up: ${tableAfterUp[0].name}, depois do down: ${tableAfterDown[0].name}`);
} catch (error) {
  record("6 migration up/down por código", false, String(error));
}

// ----------------------------------------------------------------------------------------------------
//                          ITENS 2 E 5: PERSISTE, LÊ, E NUMERIC VOLTA COMO STRING
// ----------------------------------------------------------------------------------------------------
const walletId = Bun.randomUUIDv7();                                                            // id novo a cada execução
try {
  const em = orm.em.fork();                                                                     // em isolado para o teste
  em.create(E1Wallet, { id: walletId, balance: "25.00", currency: "BRL" });
  await em.flush();
  const readBack = await orm.em.fork().findOneOrFail(E1Wallet, { id: walletId });
  record("2 persiste e lê com defineEntity", readBack.id === walletId, `id lido: ${readBack.id}`);
  record("5 numeric(18,2) volta como string", readBack.balance === "25.00" && typeof readBack.balance === "string", `valor: ${JSON.stringify(readBack.balance)} (${typeof readBack.balance})`);
} catch (error) {
  record("2 persiste e lê com defineEntity", false, String(error));
}

// ----------------------------------------------------------------------------------------------------
//                     ITEM 3: PESSIMISTIC_WRITE GERA FOR UPDATE E A SEGUNDA ESPERA
// ----------------------------------------------------------------------------------------------------
try {
  executedSql.length = 0;
  const holdMilliseconds = 1000;                                                                // quanto tempo a primeira segura o lock
  let firstReleasedAt = 0;
  let secondGotLockAt = 0;

  const first = orm.em.fork().transactional(async (em) => {
    await em.findOneOrFail(E1Wallet, { id: walletId }, { lockMode: LockMode.PESSIMISTIC_WRITE });
    await Bun.sleep(holdMilliseconds);                                                          // segura o lock de propósito
    firstReleasedAt = performance.now();
  });
  await Bun.sleep(100);                                                                         // garante que a primeira travou antes
  const second = orm.em.fork().transactional(async (em) => {
    await em.findOneOrFail(E1Wallet, { id: walletId }, { lockMode: LockMode.PESSIMISTIC_WRITE });
    secondGotLockAt = performance.now();
  });
  await Promise.all([first, second]);

  const forUpdateQuery = executedSql.find((sql) => /for update/i.test(sql) && !/skip locked/i.test(sql)) ?? "(nenhuma)";
  const secondWaited = secondGotLockAt >= firstReleasedAt;                                      // só pegou o lock depois que a primeira soltou
  record("3 PESSIMISTIC_WRITE gera for update e a segunda espera", forUpdateQuery !== "(nenhuma)" && secondWaited, `sql: ${forUpdateQuery.slice(0, 160)} | segunda pegou o lock ${Math.round(secondGotLockAt - firstReleasedAt)} ms depois da primeira soltar`);
} catch (error) {
  record("3 PESSIMISTIC_WRITE gera for update e a segunda espera", false, String(error));
}

// ----------------------------------------------------------------------------------------------------
//                          ITEM 4: PESSIMISTIC_PARTIAL_WRITE GERA SKIP LOCKED
// ----------------------------------------------------------------------------------------------------
try {
  executedSql.length = 0;
  let rowsSeenWhileLocked = -1;
  const holder = orm.em.fork().transactional(async (em) => {
    await em.findOneOrFail(E1Wallet, { id: walletId }, { lockMode: LockMode.PESSIMISTIC_WRITE });
    await Bun.sleep(500);
  });
  await Bun.sleep(100);
  await orm.em.fork().transactional(async (em) => {
    const rows = await em.find(E1Wallet, { id: walletId }, { lockMode: LockMode.PESSIMISTIC_PARTIAL_WRITE });
    rowsSeenWhileLocked = rows.length;                                                          // linha travada deve ser pulada
  });
  await holder;
  const skipLockedQuery = executedSql.find((sql) => /for update skip locked/i.test(sql)) ?? "(nenhuma)";
  record("4 PESSIMISTIC_PARTIAL_WRITE gera for update skip locked", skipLockedQuery !== "(nenhuma)" && rowsSeenWhileLocked === 0, `sql: ${skipLockedQuery.slice(0, 160)} | linhas vistas com a linha travada: ${rowsSeenWhileLocked}`);
} catch (error) {
  record("4 PESSIMISTIC_PARTIAL_WRITE gera for update skip locked", false, String(error));
}

// ----------------------------------------------------------------------------------------------------
//                       ITEM 7: SET LOCAL LOCK_TIMEOUT VALE SÓ PARA A TRANSAÇÃO
// ----------------------------------------------------------------------------------------------------
const singleConnectionOrm = await MikroORM.init({ ...connectionOptions, pool: { min: 1, max: 1 }, debug: false } as any);   // uma conexão só: a mesma sessão antes e depois
try {
  let insideValue = "";
  await singleConnectionOrm.em.fork().transactional(async (em) => {
    await em.execute(`set local lock_timeout = '2s'`);
    const rows = await em.execute(`show lock_timeout`);
    insideValue = rows[0].lock_timeout;
  });
  const afterRows = await singleConnectionOrm.em.fork().execute(`show lock_timeout`);
  const afterValue = afterRows[0].lock_timeout;
  record("7 SET LOCAL lock_timeout só na transação", insideValue === "2s" && afterValue === "0", `dentro: ${insideValue}, depois do commit (mesma conexão): ${afterValue}`);
} catch (error) {
  record("7 SET LOCAL lock_timeout só na transação", false, String(error));
}
await singleConnectionOrm.close();

// ----------------------------------------------------------------------------------------------------
//                     ITEM 8: ASYNCLOCALSTORAGE SOBREVIVE A AWAIT E CONSULTA AO BANCO
// ----------------------------------------------------------------------------------------------------
try {
  const storage = new AsyncLocalStorage<{ correlationId: string }>();
  const seenValues = await storage.run({ correlationId: "corr-e1" }, async () => {
    const values: (string | undefined)[] = [];
    values.push(storage.getStore()?.correlationId);
    await orm.em.fork().execute(`select pg_sleep(0.05)`);                                       // consulta real ao banco
    values.push(storage.getStore()?.correlationId);
    await orm.em.fork().transactional(async (em) => {
      await em.findOneOrFail(E1Wallet, { id: walletId }, { lockMode: LockMode.PESSIMISTIC_WRITE });
      values.push(storage.getStore()?.correlationId);                                          // dentro da transação
    });
    await Bun.sleep(10);
    values.push(storage.getStore()?.correlationId);
    return values;
  });
  record("8 AsyncLocalStorage depois de await e consultas", seenValues.every((value) => value === "corr-e1"), `valores lidos: ${JSON.stringify(seenValues)}`);
} catch (error) {
  record("8 AsyncLocalStorage depois de await e consultas", false, String(error));
}

await orm.migrator.down();                                                                      // limpa o que o experimento criou
await orm.close();

console.log(JSON.stringify({ runtime: `bun ${Bun.version} ${process.platform}`, results }, null, 2));
