// ====================================================================================================
//                                    REPOSITÓRIO DO LEDGER
// ====================================================================================================
// Só INSERT (pelo Unit of Work, no flush do commit) e leitura: o ledger não tem update nem delete,
// nem aqui nem no banco (trigger). A reconciliação é SQL escrito à mão, de propósito (abaixo).
import type { InferEntity } from "@mikro-orm/postgresql";                                    // tipo da linha a partir do registro
import type { LedgerRepository, ReconciliationRow } from "../../application/ports";
import { Money } from "../../domain/money";
import { LedgerDirection, WalletLedgerEntry } from "../../domain/wallet-ledger-entry";
import type { MikroOrmDatabase } from "./database";
import { WalletLedgerEntryRecord } from "./records";

type LedgerRow = InferEntity<typeof WalletLedgerEntryRecord>;

function toDomain(row: LedgerRow): WalletLedgerEntry {
  return WalletLedgerEntry.rehydrate({
    id: row.id,
    walletId: row.walletId,
    transactionId: row.transactionId,
    direction: row.direction as LedgerDirection,
    money: Money.from({ amount: row.amount, currency: row.currency }),
    balanceBefore: Money.from({ amount: row.balanceBefore, currency: row.currency }),
    balanceAfter: Money.from({ amount: row.balanceAfter, currency: row.currency }),
    walletVersion: row.walletVersion,
    createdAt: row.createdAt,
  });
}

// Uma consulta só: um comando enxerga um snapshot só, então saldo e soma do ledger são coerentes
// mesmo com transações sendo aplicadas ao mesmo tempo, sem travar a wallet. Em SQL direto porque
// pelo ORM seriam duas consultas (a wallet e a soma), cada uma com o seu snapshot em READ COMMITTED.
const RECONCILIATION_SQL = `
  select w.balance::text as stored_balance,
         w.currency as currency,
         coalesce(sum(e.amount) filter (where e.direction = 'CREDIT'), 0)::numeric(18,2)::text as total_credits,
         coalesce(sum(e.amount) filter (where e.direction = 'DEBIT'), 0)::numeric(18,2)::text as total_debits,
         count(e.id)::int as checked_entries
    from wallets w
    left join wallet_ledger_entries e on e.wallet_id = w.id
   where w.id = ?
   group by w.id`;

export class MikroOrmLedgerRepository implements LedgerRepository {
  constructor(private readonly database: MikroOrmDatabase) {}

  async insert(entry: WalletLedgerEntry): Promise<void> {
    const entityManager = this.database.transactionEntityManager();
    const row = entityManager.create(WalletLedgerEntryRecord, {
      id: entry.id,
      walletId: entry.walletId,
      transactionId: entry.transactionId,
      direction: entry.direction,
      amount: entry.money.toString(),
      currency: entry.money.currency,
      balanceBefore: entry.balanceBefore.toString(),
      balanceAfter: entry.balanceAfter.toString(),
      walletVersion: entry.walletVersion,
      createdAt: entry.createdAt,
    });
    entityManager.persist(row);                                                                 // depois da transação no flush: a relação define a ordem
  }

  async listByWallet(walletId: string, afterWalletVersion: number, limit: number): Promise<WalletLedgerEntry[]> {
    const rows = await this.database.entityManager().find(
      WalletLedgerEntryRecord,
      { walletId, walletVersion: { $gt: afterWalletVersion } },                                 // faixa no índice UNIQUE (wallet_id, wallet_version)
      { orderBy: { walletVersion: "asc" }, limit },
    );
    return rows.map(toDomain);
  }

  async reconcile(walletId: string): Promise<ReconciliationRow | undefined> {
    const rows = await this.database.entityManager().execute<{ stored_balance: string; currency: string; total_credits: string; total_debits: string; checked_entries: number }[]>(RECONCILIATION_SQL, [walletId]);
    const row = rows[0];
    if (!row) return undefined;
    return {
      storedBalance: Money.from({ amount: row.stored_balance, currency: row.currency }),
      totalCredits: Money.from({ amount: row.total_credits, currency: row.currency }),
      totalDebits: Money.from({ amount: row.total_debits, currency: row.currency }),
      checkedEntries: row.checked_entries,
    };
  }
}
