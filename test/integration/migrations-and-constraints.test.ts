// ====================================================================================================
//            MIGRATIONS E CONSTRAINTS: UP -> DOWN -> UP, E CADA GARANTIA CONFERIDA COM SQL DIRETO
// ====================================================================================================
// Desde a migration 005, o banco confere no commit que saldo e version da wallet são os do último
// lançamento. Por isso os fixtures montam estados coerentes (wallet, OPENING e lançamento numa
// transação só) e cada movimento muda o lançamento e a wallet juntos.
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { SQL } from "bun";
import { Money } from "../../src/domain/money";
import { createOrm } from "../../src/infrastructure/persistence/orm";
import { uniqueSuffix } from "../support/harness";

let sql: SQL;
let databaseUrl: string;

async function expectSqlError(statement: Promise<unknown>, expectedFragment: string): Promise<void> {
  let message = "nenhum erro";
  try {
    await statement;
  } catch (error) {
    message = (error as Error).message;
  }
  expect(message).toContain(expectedFragment);
}

const ids = () => Bun.randomUUIDv7();
const HASH = "a".repeat(64);
const NOW = "2026-07-29T15:00:00Z";

// ----------------------------------------------------------------------------------------------------
//                                          FIXTURES
// ----------------------------------------------------------------------------------------------------
// uma linha de wallets direto, sem lançamento: só fica de pé se for saldo zero e versão 1
function insertWalletRow(balance: string, currency = "BRL", version = 1, playerId = ids()) {
  return sql`insert into wallets (id, player_id, currency, balance, version, created_at, updated_at) values (${ids()}, ${playerId}, ${currency}, ${balance}, ${version}, ${NOW}, ${NOW})`;
}

interface TransactionColumns {
  id?: string; provider_id?: string | null; external_transaction_id?: string | null; idempotency_key?: string; payload_hash?: string;
  wallet_id: string; kind?: string; amount?: string; currency?: string; reference_external_transaction_id?: string | null;
  reference_transaction_id?: string | null; status?: string; failure_code?: string | null; processed_at?: string | null;
  round_id?: string | null; game_id?: string | null;
}

function transactionRow(columns: TransactionColumns) {
  const external = columns.external_transaction_id === undefined ? `ext-${ids()}` : columns.external_transaction_id;
  return {
    id: columns.id ?? ids(),
    provider_id: columns.provider_id === undefined ? "provider-a" : columns.provider_id,
    external_transaction_id: external,
    idempotency_key: columns.idempotency_key ?? `key-${ids()}`,
    payload_hash: columns.payload_hash ?? HASH,
    wallet_id: columns.wallet_id,
    player_id: ids(),
    round_id: columns.round_id === undefined ? "round-1" : columns.round_id,
    game_id: columns.game_id === undefined ? "game-1" : columns.game_id,
    kind: columns.kind ?? "BET",
    amount: columns.amount ?? "10.00",
    currency: columns.currency ?? "BRL",
    reference_external_transaction_id: columns.reference_external_transaction_id ?? null,
    reference_transaction_id: columns.reference_transaction_id ?? null,
    status: columns.status ?? "PROCESSED",
    failure_code: columns.failure_code ?? null,
    balance_after: "90.00",
    balance_currency: "BRL",
    correlation_id: "corr",
    attempts: 0,
    created_at: NOW,
    updated_at: NOW,
    processed_at: columns.processed_at === undefined ? ((columns.status ?? "PROCESSED") === "PROCESSED" ? NOW : null) : columns.processed_at,
  };
}

function insertTransaction(columns: TransactionColumns) {
  const row = transactionRow(columns);
  return { row, query: sql`insert into wager_transactions ${sql(row)}` };
}

const openingColumns = (walletId: string, amount: string, currency: string): TransactionColumns => ({ wallet_id: walletId, kind: "OPENING", amount, currency, provider_id: null, external_transaction_id: null, round_id: null, game_id: null });

// wallet coerente: com saldo, nasce com o OPENING e o lançamento da versão 1 na mesma transação
async function openWallet(balance = "0.00", currency = "BRL"): Promise<string> {
  const walletId = ids();
  await sql.begin(async (transaction) => {
    await transaction`insert into wallets (id, player_id, currency, balance, version, created_at, updated_at) values (${walletId}, ${ids()}, ${currency}, ${balance}, 1, ${NOW}, ${NOW})`;
    if (balance === "0.00") return;
    const opening = transactionRow(openingColumns(walletId, balance, currency));
    await transaction`insert into wager_transactions ${transaction(opening)}`;
    await transaction`insert into wallet_ledger_entries ${transaction({ id: ids(), wallet_id: walletId, transaction_id: opening.id, direction: "CREDIT", amount: balance, currency, balance_before: "0.00", balance_after: balance, wallet_version: 1, created_at: NOW })}`;
  });
  return walletId;
}

// um movimento coerente dentro de uma transação: o lançamento parte do saldo atual e a wallet sobe uma versão
async function postEntry(transaction: SQL, walletId: string, transactionId: string, direction: "DEBIT" | "CREDIT", amount: string, entryCurrency?: string): Promise<string> {
  const [wallet] = await transaction`select balance, currency, version from wallets where id = ${walletId} for update`;
  const before = Money.from({ amount: wallet.balance, currency: wallet.currency });
  const money = Money.from({ amount, currency: wallet.currency });
  const after = direction === "CREDIT" ? before.add(money) : before.subtract(money);
  const entryId = ids();
  await transaction`update wallets set balance = ${after.toString()}, version = version + 1, updated_at = now() where id = ${walletId}`;   // a wallet antes do lançamento: o banco só confere no commit
  await transaction`insert into wallet_ledger_entries ${transaction({ id: entryId, wallet_id: walletId, transaction_id: transactionId, direction, amount, currency: entryCurrency ?? wallet.currency, balance_before: before.toString(), balance_after: after.toString(), wallet_version: wallet.version + 1, created_at: NOW })}`;
  return entryId;
}

// uma aposta processada com o lançamento e a wallet atualizada, tudo junto
async function processedBet(walletId: string, amount = "10.00"): Promise<{ transactionId: string; entryId: string }> {
  const bet = transactionRow({ wallet_id: walletId, kind: "BET", amount });
  let entryId = "";
  await sql.begin(async (transaction) => {
    await transaction`insert into wager_transactions ${transaction(bet)}`;
    entryId = await postEntry(transaction, walletId, bet.id, "DEBIT", amount);
  });
  return { transactionId: bet.id, entryId };
}

async function walletState(walletId: string) {
  const [row] = await sql`select balance, version from wallets where id = ${walletId}`;
  return row;
}

beforeAll(async () => {
  const baseUrl = new URL(process.env.DATABASE_URL!);
  const databaseName = `wagering_migrations_${uniqueSuffix()}`;
  const admin = new SQL(baseUrl.toString());
  await admin.unsafe(`create database ${databaseName}`);
  await admin.close();
  baseUrl.pathname = `/${databaseName}`;
  databaseUrl = baseUrl.toString();
  sql = new SQL(databaseUrl);
});

afterAll(async () => {
  await sql.close();
});

describe("migrations", () => {
  test("up -> down -> up deixa o schema completo, e o down remove tudo", async () => {
    const orm = await createOrm({ databaseUrl, poolMax: 2, idleInTransactionTimeoutMilliseconds: 10_000, connectTimeoutMilliseconds: 3_000, silentMigrations: true });
    const tablesQuery = () => sql`select table_name from information_schema.tables where table_schema = 'public' and table_name <> 'schema_migrations' order by table_name`;
    const functionNames = ["wager_transactions_guard", "wager_transactions_forbid_truncate", "wallet_ledger_entries_forbid_change", "wallets_guard", "wallets_forbid_truncate", "wallet_ledger_entries_check_transaction", "wallets_check_matches_last_entry"];
    const functionsQuery = () => sql`select proname from pg_proc where proname in ${sql(functionNames)} order by proname`;
    const versionDefaultQuery = async () => (await sql`select column_default from information_schema.columns where table_name = 'wallets' and column_name = 'version'`)[0]?.column_default;

    await orm.migrator.up();
    const afterFirstUp = (await tablesQuery()).map((row: any) => row.table_name);
    expect(afterFirstUp).toEqual(["inbox_messages", "outbox_messages", "wager_transactions", "wallet_ledger_entries", "wallets"]);
    expect((await functionsQuery()).map((row: any) => row.proname)).toEqual([...functionNames].sort());
    expect(await versionDefaultQuery()).toBe("1");

    await orm.migrator.down();                                                                  // só a 005
    expect(await versionDefaultQuery()).toBeNull();
    expect((await functionsQuery()).map((row: any) => row.proname)).toEqual(["wager_transactions_forbid_truncate", "wager_transactions_guard", "wallet_ledger_entries_forbid_change"]);
    const [fkAfterDown] = await sql`select count(*)::int as total from pg_constraint where conname in ('wallet_ledger_entries_transaction_wallet_fk', 'wager_transactions_id_wallet_unique')`;
    expect(fkAfterDown.total).toBe(0);

    await orm.migrator.down({ to: 0 });                                                         // desfaz o resto
    expect(await tablesQuery()).toHaveLength(0);
    expect(await functionsQuery()).toHaveLength(0);

    await orm.migrator.up();
    expect((await tablesQuery()).map((row: any) => row.table_name)).toEqual(afterFirstUp);
    expect((await functionsQuery()).map((row: any) => row.proname)).toEqual([...functionNames].sort());
    await orm.close();
  });
});

describe("wallets", () => {
  test("saldo negativo é recusado pelo CHECK", async () => {
    await expectSqlError(insertWalletRow("-0.01"), "wallets_balance_non_negative");
    const walletId = await openWallet("10.00");
    await expectSqlError(sql`update wallets set balance = balance - 10.01, version = version + 1 where id = ${walletId}`, "wallets_balance_non_negative");
  });

  test("uma wallet por player e moeda", async () => {
    const playerId = ids();
    await insertWalletRow("0.00", "BRL", 1, playerId);
    await expectSqlError(insertWalletRow("0.00", "BRL", 1, playerId), "wallets_player_currency_unique");
    await insertWalletRow("0.00", "USD", 1, playerId);                                          // outra moeda pode
  });

  test("moeda fora do formato e version menor que 1 são recusadas", async () => {
    await expectSqlError(insertWalletRow("0.00", "brl"), "wallets_currency_format");
    await expectSqlError(insertWalletRow("0.00", "BRL", 0), "wallets_version_positive");
  });
});

describe("wallets: o banco protege a wallet (005)", () => {
  test("DELETE e TRUNCATE são proibidos", async () => {
    const walletId = await openWallet("10.00");
    await expectSqlError(sql`delete from wallets where id = ${walletId}`, "DELETE proibido");
    await expectSqlError(sql`truncate wallets cascade`, "TRUNCATE proibido");
    expect(await walletState(walletId)).toEqual({ balance: "10.00", version: 1 });
  });

  test("dono, moeda, id e created_at não mudam", async () => {
    const walletId = await openWallet();
    await expectSqlError(sql`update wallets set player_id = ${ids()} where id = ${walletId}`, "nao mudam");
    await expectSqlError(sql`update wallets set currency = 'USD' where id = ${walletId}`, "nao mudam");
    await expectSqlError(sql`update wallets set id = ${ids()} where id = ${walletId}`, "nao mudam");
    await expectSqlError(sql`update wallets set created_at = now() where id = ${walletId}`, "nao mudam");
    await sql`update wallets set updated_at = now() where id = ${walletId}`;                    // updated_at pode mudar
  });

  test("a version não volta, não pula, não muda sem o saldo e não fica parada quando o saldo muda", async () => {
    const walletId = await openWallet("100.00");
    await processedBet(walletId);                                                               // versão 2, saldo 90.00
    await expectSqlError(sql`update wallets set version = 1 where id = ${walletId}`, "so muda junto com o saldo");
    await expectSqlError(sql`update wallets set version = version + 1 where id = ${walletId}`, "so muda junto com o saldo");
    await expectSqlError(sql`update wallets set balance = 5000.00 where id = ${walletId}`, "precisa subir exatamente 1");
    await expectSqlError(sql`update wallets set balance = 5000.00, version = version + 2 where id = ${walletId}`, "precisa subir exatamente 1");
    expect(await walletState(walletId)).toEqual({ balance: "90.00", version: 2 });
  });
});

describe("ligação entre saldo, lançamento e transação (005)", () => {
  test("lançamento de uma transação de outra wallet é recusado (FK composta para a transação e a wallet)", async () => {
    const walletId = await openWallet("100.00");
    const otherWalletId = await openWallet("100.00");
    const otherBet = insertTransaction({ wallet_id: otherWalletId, kind: "BET" });
    await otherBet.query;
    await expectSqlError(sql.begin((transaction) => postEntry(transaction, walletId, otherBet.row.id, "DEBIT", "10.00")), "wallet_ledger_entries_transaction_wallet_fk");
    expect(await walletState(walletId)).toEqual({ balance: "100.00", version: 1 });
  });

  test("só existe lançamento de transação PROCESSED, que não seja LOSS, com o mesmo valor e moeda (conferido no commit)", async () => {
    const walletId = await openWallet("100.00");
    const cases: TransactionColumns[] = [
      { wallet_id: walletId, kind: "BET", status: "REJECTED", failure_code: "INSUFFICIENT_FUNDS" },
      { wallet_id: walletId, kind: "WIN", status: "PENDING_REFERENCE" },
      { wallet_id: walletId, kind: "LOSS" },
      { wallet_id: walletId, kind: "BET", amount: "12.00" },                                    // o lançamento abaixo é de 10.00
      { wallet_id: walletId, kind: "BET", currency: "USD" },                                    // o lançamento sai na moeda da wallet
    ];
    for (const columns of cases) {
      const transaction = insertTransaction(columns);
      await transaction.query;
      await expectSqlError(sql.begin((open) => postEntry(open, walletId, transaction.row.id, "DEBIT", "10.00")), "precisa de uma transacao PROCESSED");
    }
    expect(await walletState(walletId)).toEqual({ balance: "100.00", version: 1 });            // nada ficou

    await processedBet(walletId);                                                               // a transação certa passa
    expect(await walletState(walletId)).toEqual({ balance: "90.00", version: 2 });
  });

  test("uma transação OPENING por wallet", async () => {
    const walletId = await openWallet("100.00");
    await expectSqlError(insertTransaction(openingColumns(walletId, "5.00", "BRL")).query, "wager_transactions_single_opening");
  });

  test("no commit, saldo e version da wallet são os do último lançamento (ou zero e 1 sem lançamento)", async () => {
    await expectSqlError(insertWalletRow("50.00"), "nao batem com o ultimo lancamento");        // saldo sem lançamento
    await expectSqlError(insertWalletRow("0.00", "BRL", 2), "nao batem com o ultimo lancamento");   // versão sem lançamento

    const walletId = await openWallet("100.00");
    await expectSqlError(sql`update wallets set balance = 5000.00, version = version + 1 where id = ${walletId}`, "nao batem com o ultimo lancamento");   // saldo mudou sem lançamento

    const bet = insertTransaction({ wallet_id: walletId, kind: "BET" });
    await bet.query;
    await expectSqlError(sql`insert into wallet_ledger_entries ${sql({ id: ids(), wallet_id: walletId, transaction_id: bet.row.id, direction: "DEBIT", amount: "10.00", currency: "BRL", balance_before: "100.00", balance_after: "90.00", wallet_version: 2, created_at: NOW })}`, "nao batem com o ultimo lancamento");   // lançamento sem mudar a wallet
    expect(await walletState(walletId)).toEqual({ balance: "100.00", version: 1 });

    await sql.begin((transaction) => postEntry(transaction, walletId, bet.row.id, "DEBIT", "10.00"));   // as duas coisas juntas, na mesma transação
    expect(await walletState(walletId)).toEqual({ balance: "90.00", version: 2 });
  });
});

describe("wager_transactions", () => {
  test("idempotency_key e (provider_id, external_transaction_id) são únicos", async () => {
    const walletId = await openWallet();
    const first = insertTransaction({ wallet_id: walletId, idempotency_key: "same-key", external_transaction_id: "same-ext" });
    await first.query;
    await expectSqlError(insertTransaction({ wallet_id: walletId, idempotency_key: "same-key" }).query, "wager_transactions_idempotency_key_unique");
    await expectSqlError(insertTransaction({ wallet_id: walletId, external_transaction_id: "same-ext" }).query, "wager_transactions_provider_external_unique");
  });

  test("uma reversão processada por referência, somando REFUND e ROLLBACK", async () => {
    const walletId = await openWallet();
    const bet = insertTransaction({ wallet_id: walletId, kind: "BET" });
    await bet.query;
    await insertTransaction({ wallet_id: walletId, kind: "REFUND", reference_external_transaction_id: bet.row.external_transaction_id, reference_transaction_id: bet.row.id }).query;
    await expectSqlError(insertTransaction({ wallet_id: walletId, kind: "ROLLBACK", reference_external_transaction_id: bet.row.external_transaction_id, reference_transaction_id: bet.row.id }).query, "wager_transactions_single_reversal");
    await insertTransaction({ wallet_id: walletId, kind: "ROLLBACK", status: "REJECTED", failure_code: "REFERENCE_ALREADY_REVERSED", reference_external_transaction_id: bet.row.external_transaction_id, reference_transaction_id: bet.row.id }).query;   // rejeitada não conta
  });

  test("kind, status (PENDING não entra), valor e moeda", async () => {
    const walletId = await openWallet();
    await expectSqlError(insertTransaction({ wallet_id: walletId, kind: "DEPOSIT" }).query, "wager_transactions_kind_valid");
    await expectSqlError(insertTransaction({ wallet_id: walletId, status: "PENDING", processed_at: null }).query, "wager_transactions_status_valid");
    await expectSqlError(insertTransaction({ wallet_id: walletId, amount: "-1.00" }).query, "wager_transactions_amount_non_negative");
    await expectSqlError(insertTransaction({ wallet_id: walletId, kind: "BET", amount: "0.00" }).query, "wager_transactions_amount_positive_unless_loss");
    await insertTransaction({ wallet_id: walletId, kind: "LOSS", amount: "0.00" }).query;      // LOSS aceita zero
    await expectSqlError(insertTransaction({ wallet_id: walletId, currency: "R$1" }).query, "wager_transactions_currency_format");
  });

  test("referência por tipo: REFUND/ROLLBACK exigem, BET e OPENING não podem ter", async () => {
    const walletId = await openWallet();
    await expectSqlError(insertTransaction({ wallet_id: walletId, kind: "REFUND", status: "REJECTED", failure_code: "X", processed_at: null }).query, "wager_transactions_reference_by_kind");
    await expectSqlError(insertTransaction({ wallet_id: walletId, kind: "BET", reference_external_transaction_id: "ref" }).query, "wager_transactions_reference_by_kind");
  });

  test("failure_code se e somente se REJECTED/FAILED; processed_at se e somente se PROCESSED", async () => {
    const walletId = await openWallet();
    await expectSqlError(insertTransaction({ wallet_id: walletId, status: "REJECTED", processed_at: null }).query, "wager_transactions_failure_code_iff_failed");
    await expectSqlError(insertTransaction({ wallet_id: walletId, status: "PROCESSED", failure_code: "X" }).query, "wager_transactions_failure_code_iff_failed");
    await expectSqlError(insertTransaction({ wallet_id: walletId, status: "PROCESSED", processed_at: null }).query, "wager_transactions_processed_at_iff_processed");
  });

  test("reversão processada exige reference_transaction_id", async () => {
    const walletId = await openWallet();
    await expectSqlError(insertTransaction({ wallet_id: walletId, kind: "REFUND", reference_external_transaction_id: "ref" }).query, "wager_transactions_processed_reversal_has_reference");
  });

  test("OPENING tem os campos de provedor nulos; os outros tipos têm os quatro", async () => {
    const walletId = await openWallet();
    await insertTransaction(openingColumns(walletId, "10.00", "BRL")).query;
    await expectSqlError(insertTransaction({ wallet_id: walletId, kind: "OPENING" }).query, "wager_transactions_opening_has_no_provider_fields");
    await expectSqlError(insertTransaction({ wallet_id: walletId, kind: "BET", round_id: null }).query, "wager_transactions_opening_has_no_provider_fields");
  });

  test("payload_hash precisa ser SHA-256 em hexadecimal minúsculo", async () => {
    const walletId = await openWallet();
    await expectSqlError(insertTransaction({ wallet_id: walletId, payload_hash: "A".repeat(64) }).query, "wager_transactions_payload_hash_format");
  });

  test("chave estrangeira para a wallet", async () => {
    await expectSqlError(insertTransaction({ wallet_id: ids() }).query, "foreign key");
  });

  test("trigger: DELETE proibido, UPDATE de terminal proibido, identidade não muda nem pendente, TRUNCATE proibido", async () => {
    const walletId = await openWallet();
    const processed = insertTransaction({ wallet_id: walletId });
    await processed.query;
    await expectSqlError(sql`delete from wager_transactions where id = ${processed.row.id}`, "DELETE proibido");
    await expectSqlError(sql`update wager_transactions set status = 'REJECTED', failure_code = 'X', processed_at = null where id = ${processed.row.id}`, "terminal");

    const pending = insertTransaction({ wallet_id: walletId, kind: "REFUND", status: "PENDING_REFERENCE", reference_external_transaction_id: "ref", processed_at: null });
    await pending.query;
    await expectSqlError(sql`update wager_transactions set amount = 99.00 where id = ${pending.row.id}`, "nao mudam");
    await sql`update wager_transactions set attempts = 1 where id = ${pending.row.id}`;        // pendente pode ser reagendada
    await expectSqlError(sql`truncate wager_transactions cascade`, "TRUNCATE proibido");
  });

  test("índice parcial do worker existe e só cobre PENDING_REFERENCE", async () => {
    const [index] = await sql`select indexdef from pg_indexes where indexname = 'wager_transactions_pending_due'`;
    expect(index.indexdef).toContain("WHERE ((status)::text = 'PENDING_REFERENCE'::text)");
  });
});

describe("wallet_ledger_entries", () => {
  // wallet com 100.00 (OPENING na versão 1) e uma aposta de 10.00 processada, ainda sem lançamento
  async function ledgerFixture() {
    const walletId = await openWallet("100.00");
    const transaction = insertTransaction({ wallet_id: walletId });
    await transaction.query;
    return { walletId, transactionId: transaction.row.id };
  }

  function insertEntry(values: Record<string, unknown>) {
    const row = { id: ids(), direction: "DEBIT", amount: "10.00", currency: "BRL", balance_before: "100.00", balance_after: "90.00", wallet_version: 2, created_at: NOW, ...values };
    return sql`insert into wallet_ledger_entries ${sql(row)}`;
  }

  test("a aritmética do lançamento é conferida no schema", async () => {
    const { walletId, transactionId } = await ledgerFixture();
    await expectSqlError(insertEntry({ wallet_id: walletId, transaction_id: transactionId, balance_after: "91.00" }), "wallet_ledger_entries_arithmetic");
    await expectSqlError(insertEntry({ wallet_id: walletId, transaction_id: transactionId, direction: "CREDIT" }), "wallet_ledger_entries_arithmetic");
  });

  test("valor positivo, saldos não negativos, direção válida, versão >= 1", async () => {
    const { walletId, transactionId } = await ledgerFixture();
    await expectSqlError(insertEntry({ wallet_id: walletId, transaction_id: transactionId, amount: "0.00", balance_after: "100.00" }), "wallet_ledger_entries_amount_positive");
    await expectSqlError(insertEntry({ wallet_id: walletId, transaction_id: transactionId, amount: "110.00", balance_after: "-10.00" }), "wallet_ledger_entries_balance_after_non_negative");
    await expectSqlError(insertEntry({ wallet_id: walletId, transaction_id: transactionId, direction: "SIDE" }), "violates check constraint");   // a aritmética também quebra e vem antes na ordem do PostgreSQL
    const [directionCheck] = await sql`select pg_get_constraintdef(oid) as definition from pg_constraint where conname = 'wallet_ledger_entries_direction_valid'`;
    expect(directionCheck.definition).toContain("'DEBIT'");
    expect(directionCheck.definition).toContain("'CREDIT'");
    await expectSqlError(insertEntry({ wallet_id: walletId, transaction_id: transactionId, wallet_version: 0 }), "wallet_ledger_entries_wallet_version_positive");
  });

  test("um lançamento por transação por wallet, e uma versão por wallet (guarda contra lost update)", async () => {
    const { walletId, transactionId } = await ledgerFixture();
    await sql.begin((transaction) => postEntry(transaction, walletId, transactionId, "DEBIT", "10.00"));   // versão 2
    await expectSqlError(insertEntry({ wallet_id: walletId, transaction_id: transactionId, balance_before: "90.00", balance_after: "80.00", wallet_version: 3 }), "wallet_ledger_entries_one_per_transaction");
    const other = insertTransaction({ wallet_id: walletId });
    await other.query;
    await expectSqlError(insertEntry({ wallet_id: walletId, transaction_id: other.row.id, wallet_version: 2 }), "wallet_ledger_entries_wallet_version_unique");
  });

  test("a moeda do lançamento é a da wallet (FK composta)", async () => {
    const { walletId, transactionId } = await ledgerFixture();
    await expectSqlError(insertEntry({ wallet_id: walletId, transaction_id: transactionId, currency: "USD" }), "wallet_ledger_entries_wallet_currency_fk");
  });

  test("UPDATE, DELETE e TRUNCATE são proibidos", async () => {
    const { walletId, transactionId } = await ledgerFixture();
    const entryId = await sql.begin((transaction) => postEntry(transaction, walletId, transactionId, "DEBIT", "10.00"));
    await expectSqlError(sql`update wallet_ledger_entries set amount = 1.00, balance_after = 99.00 where id = ${entryId}`, "UPDATE proibido");
    await expectSqlError(sql`delete from wallet_ledger_entries where id = ${entryId}`, "DELETE proibido");
    await expectSqlError(sql`truncate wallet_ledger_entries`, "TRUNCATE proibido");
    const [stillThere] = await sql`select count(*)::int as total from wallet_ledger_entries where id = ${entryId}`;
    expect(stillThere.total).toBe(1);
  });
});

describe("inbox e outbox", () => {
  test("inbox: (consumer_name, message_id) é a chave", async () => {
    await sql`insert into inbox_messages (consumer_name, message_id, payload_hash, received_at) values ('c', 'm-1', ${HASH}, ${NOW})`;
    await expectSqlError(sql`insert into inbox_messages (consumer_name, message_id, payload_hash, received_at) values ('c', 'm-1', ${HASH}, ${NOW})`, "inbox_messages_pkey");
    await sql`insert into inbox_messages (consumer_name, message_id, payload_hash, received_at) values ('outro', 'm-1', ${HASH}, ${NOW})`;
  });

  test("outbox: attempts não negativo e índice parcial dos publishers", async () => {
    await expectSqlError(sql`insert into outbox_messages (id, aggregate_id, event_type, payload, occurred_at, attempts, next_attempt_at) values (${ids()}, ${ids()}, 'X', '{}', ${NOW}, -1, ${NOW})`, "outbox_messages_attempts_non_negative");
    const [index] = await sql`select indexdef from pg_indexes where indexname = 'outbox_messages_due'`;
    expect(index.indexdef).toContain("WHERE (published_at IS NULL)");
  });
});
