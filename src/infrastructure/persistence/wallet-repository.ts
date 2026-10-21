// ====================================================================================================
//                                   REPOSITÓRIO DE WALLETS
// ====================================================================================================
import type { InferEntity } from "@mikro-orm/postgresql";                                    // tipo da linha a partir do registro
import { LockMode } from "@mikro-orm/postgresql";                                              // FOR UPDATE
import { WalletVersionConflictError } from "../../application/errors";                          // UPDATE com 0 linhas é bug
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
    const row = await this.database.entityManager().findOne(WalletRecord, { id: walletId }, { lockMode: LockMode.PESSIMISTIC_WRITE });   // trava só esta wallet
    stopTimer();
    return row ? toDomain(row) : undefined;
  }

  async findByPlayerAndCurrency(playerId: string, currency: string): Promise<Wallet | undefined> {
    const row = await this.database.entityManager().findOne(WalletRecord, { playerId, currency });
    return row ? toDomain(row) : undefined;
  }

  async insert(wallet: Wallet): Promise<void> {
    await this.database.entityManager().insert(WalletRecord, {
      id: wallet.id,
      playerId: wallet.playerId,
      currency: wallet.currency,
      balance: wallet.balance.toString(),
      version: wallet.version,
      createdAt: wallet.createdAt,
      updatedAt: wallet.updatedAt,
    });
  }

  async updateBalance(wallet: Wallet, expectedVersion: number): Promise<void> {
    const affectedRows = await this.database.entityManager().nativeUpdate(
      WalletRecord,
      { id: wallet.id, version: expectedVersion },                                              // guarda extra contra lost update
      { balance: wallet.balance.toString(), version: wallet.version, updatedAt: wallet.updatedAt },
    );
    if (affectedRows !== 1) throw new WalletVersionConflictError(`UPDATE da wallet ${wallet.id} com version ${expectedVersion} afetou ${affectedRows} linhas`);
  }
}
