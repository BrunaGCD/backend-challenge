// ====================================================================================================
//        VERIFICADOR DO FIM DE TODO TESTE DE INTEGRAÇÃO E DE CONCORRÊNCIA (SEÇÃO 13)
// ====================================================================================================
// Para cada wallet tocada, lendo o banco com SQL direto (não pelo ORM da aplicação):
//   balance == Σ créditos − Σ débitos; balanceBefore de cada lançamento == balanceAfter do anterior;
//   wallet_version sem buracos; version da wallet == versão do último lançamento;
//   cada lançamento tem um WalletBalanceChanged na outbox, e vice-versa;
//   transação PROCESSED que move saldo tem exatamente um lançamento; as outras, nenhum.
import { expect } from "bun:test";
import type { SQL } from "bun";
import { Money } from "../../src/domain/money";
import { LedgerDirection, WalletLedgerEntry } from "../../src/domain/wallet-ledger-entry";
import { testSql } from "./harness";
import { ledgerChainProblems } from "./ledger-chain";

// todas as consultas num snapshot só: saldo, ledger e outbox coerentes entre si mesmo com trabalho em andamento
export async function walletInvariantProblems(walletId: string): Promise<string[]> {
  return testSql().begin("isolation level repeatable read read only", (snapshot) => problemsInSnapshot(snapshot, walletId));
}

async function problemsInSnapshot(sql: SQL, walletId: string): Promise<string[]> {
  const [wallet] = await sql`select balance, currency, version from wallets where id = ${walletId}`;
  if (!wallet) return [`wallet ${walletId} não existe`];

  const rows = await sql`select * from wallet_ledger_entries where wallet_id = ${walletId} order by wallet_version`;
  const entries = rows.map((row: any) => WalletLedgerEntry.rehydrate({
    id: row.id, walletId: row.wallet_id, transactionId: row.transaction_id, direction: row.direction as LedgerDirection,
    money: Money.from({ amount: row.amount, currency: row.currency }),
    balanceBefore: Money.from({ amount: row.balance_before, currency: row.currency }),
    balanceAfter: Money.from({ amount: row.balance_after, currency: row.currency }),
    walletVersion: row.wallet_version, createdAt: row.created_at,
  }));
  const balance = Money.from({ amount: wallet.balance, currency: wallet.currency });
  const problems = ledgerChainProblems(balance, entries).map((problem) => `wallet ${walletId}: ${problem}`);

  const lastVersion = entries.at(-1)?.walletVersion ?? 1;
  if (wallet.version !== lastVersion) problems.push(`wallet ${walletId}: version ${wallet.version} diferente da versão do último lançamento ${lastVersion}`);

  const [eventCounts] = await sql`
    select count(*)::int as events,
           count(*) filter (where exists (
             select 1 from wallet_ledger_entries e
              where e.wallet_id = ${walletId}
                and e.transaction_id::text = o.payload->'data'->>'transactionId'
                and e.wallet_version = (o.payload->'data'->>'walletVersion')::int))::int as matched
      from outbox_messages o
     where o.event_type = 'WalletBalanceChanged' and o.aggregate_id = ${walletId}`;
  if (eventCounts.events !== entries.length || eventCounts.matched !== entries.length) {
    problems.push(`wallet ${walletId}: ${entries.length} lançamentos, ${eventCounts.events} WalletBalanceChanged, ${eventCounts.matched} casando lançamento e versão`);
  }

  const wrongEntryCounts = await sql`
    select t.id, t.kind, t.status, count(e.id)::int as entries
      from wager_transactions t
      left join wallet_ledger_entries e on e.transaction_id = t.id
     where t.wallet_id = ${walletId}
     group by t.id, t.kind, t.status
    having count(e.id) <> case when t.status = 'PROCESSED' and t.kind <> 'LOSS' then 1 else 0 end`;
  for (const row of wrongEntryCounts) problems.push(`transação ${row.id} (${row.kind} ${row.status}) tem ${row.entries} lançamentos`);
  return problems;
}

export async function expectWalletInvariants(walletIds: string[]): Promise<void> {
  const problems: string[] = [];
  for (const walletId of new Set(walletIds)) problems.push(...(await walletInvariantProblems(walletId)));
  expect(problems).toEqual([]);
}
