// ====================================================================================================
//                                   REPOSITÓRIO DE WALLETS
// ====================================================================================================
// Grava pelo Unit of Work: insert faz persist de um registro novo, e updateBalance muda o registro que
// a transação carregou com o lock (vem do Identity Map). O SQL sai no flush do commit, e o UPDATE leva
// WHERE version = <a lida>: é o optimistic lock do MikroORM (version: true) somado ao FOR UPDATE.
import type { InferEntity } from "@mikro-orm/postgresql";                                    // tipo da linha a partir do registro
import { LockMode } from "@mikro-orm/postgresql";                                              // FOR UPDATE
import { ConcurrentUpdateError } from "../../application/errors";                               // versão diferente da que o domínio viu é bug
import type { WalletRepository } from "../../application/ports";
import { Money } from "../../domain/money";
import { Wallet } from "../../domain/wallet";
import { walletLockWaitSeconds } from "../../observability/metrics";                            // tempo esperando o lock
import type { MikroOrmDatabase } from "./database";
import { WalletRecord } from "./records";

type WalletRow = InferEntity<typeof WalletRecord>;

function toDomain(row: WalletRow): Wallet {
  return Wallet.rehydrate({
    id: row.id,
    playerId: row.playerId,
    currency: row.currency,
    balance: Money.from({ amount: row.balance, currency: row.currency }),                       // numeric volta como string "975.00"
    version: row.version,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  });
}

export class MikroOrmWalletRepository implements WalletRepository {
  constructor(private readonly database: MikroOrmDatabase) {}

  async findById(walletId: string): Promise<Wallet | undefined> {
    const row = await this.database.entityManager().findOne(WalletRecord, { id: walletId });
    return row ? toDomain(row) : undefined;
  }

  async findByIdForUpdate(walletId: string): Promise<Wallet | undefined> {
    const stopTimer = walletLockWaitSeconds.startTimer();
    const row = await this.database.entityManager().findOne(WalletRecord, { id: walletId }, { lockMode: LockMode.PESSIMISTIC_WRITE, refresh: true });   // trava só esta wallet e lê o valor atual do banco
    stopTimer();
    return row ? toDomain(row) : undefined;
  }

  async findByPlayerAndCurrency(playerId: string, currency: string): Promise<Wallet | undefined> {
    const row = await this.database.entityManager().findOne(WalletRecord, { playerId, currency });
    return row ? toDomain(row) : undefined;
  }

  async insert(wallet: Wallet): Promise<void> {
    const entityManager = this.database.transactionEntityManager();
    const row = entityManager.create(WalletRecord, {
      id: wallet.id,
      playerId: wallet.playerId,
      currency: wallet.currency,
      balance: wallet.balance.toString(),
      createdAt: wallet.createdAt,
      updatedAt: wallet.updatedAt,
    });                                                                                         // sem version: o banco grava 1 (default) e o ORM lê de volta
    entityManager.persist(row);
  }

  async updateBalance(wallet: Wallet, expectedVersion: number): Promise<void> {
    const row = this.database.loadedRecord(WalletRecord, wallet.id);                            // o mesmo registro do FOR UPDATE, sem SQL
    if (row.version !== expectedVersion) throw new ConcurrentUpdateError(`a wallet ${wallet.id} foi lida na versão ${row.version}, o domínio esperava ${expectedVersion}`);
    if (wallet.version !== expectedVersion + 1) throw new ConcurrentUpdateError(`a wallet ${wallet.id} precisa subir uma versão por movimento, veio ${wallet.version}`);
    row.balance = wallet.balance.toString();                                                    // só saldo e updatedAt mudam: a version o ORM sobe sozinho
    row.updatedAt = wallet.updatedAt;
  }
}
