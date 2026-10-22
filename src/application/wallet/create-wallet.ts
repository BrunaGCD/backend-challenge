// ====================================================================================================
//                    CASO DE USO: CRIAR WALLET (COM O OPENING NA MESMA TRANSAÇÃO SQL)
// ====================================================================================================
// Saldo inicial maior que zero gera a transação interna OPENING, o lançamento CREDIT e os eventos
// WagerTransactionProcessed e WalletBalanceChanged, tudo no mesmo commit.
import { WagerTransactionProcessed, WalletBalanceChanged } from "../../domain/events";          // eventos do OPENING
import type { Money } from "../../domain/money";
import { OutboxMessage } from "../../domain/messaging/outbox-message";                          // eventos no mesmo commit
import { Wallet } from "../../domain/wallet/wallet";                                            // aggregate
import { WagerTransaction } from "../../domain/wagering/wager-transaction";                     // OPENING
import { addToLogContext } from "../../observability/log-context";
import { logger } from "../../observability/logger";
import { transactionsTotal } from "../../observability/metrics";
import { WalletAlreadyExistsError } from "../errors";                                           // 409
import { uniqueViolationConstraint } from "../infrastructure-errors";
import { computeOpeningPayloadHash } from "../wagering/payload-hash";                           // hash do OPENING
import type { Clock, IdGenerator, Repositories, TransactionRunner } from "../ports";

export interface CreateWalletCommand {
  playerId: string;
  initialBalance: Money;
}

export class CreateWallet {
  constructor(
    private readonly runner: TransactionRunner,
    private readonly repositories: Repositories,
    private readonly clock: Clock,
    private readonly ids: IdGenerator,
  ) {}

  async execute(command: CreateWalletCommand, correlationId: string): Promise<Wallet> {
    const existing = await this.repositories.wallets.findByPlayerAndCurrency(command.playerId, command.initialBalance.currency);
    if (existing) throw new WalletAlreadyExistsError(existing.id);

    try {
      const wallet = await this.runner.run(() => this.openWallet(command, correlationId));
      logger.info({ event: "wallet_created", walletId: wallet.id }, "wallet criada");
      return wallet;
    } catch (error) {
      if (uniqueViolationConstraint(error) !== "wallets_player_currency_unique") throw error;
      const winner = await this.repositories.wallets.findByPlayerAndCurrency(command.playerId, command.initialBalance.currency);   // criada ao mesmo tempo por outra requisição
      if (!winner) throw error;
      throw new WalletAlreadyExistsError(winner.id);
    }
  }

  private async openWallet(command: CreateWalletCommand, correlationId: string): Promise<Wallet> {
    const now = this.clock.now();
    const walletId = this.ids.newId();
    const openingTransactionId = this.ids.newId();
    addToLogContext({ walletId });
    const { wallet, openingEntry } = Wallet.open({ id: walletId, playerId: command.playerId, initialBalance: command.initialBalance, openingTransactionId, openingLedgerEntryId: this.ids.newId(), openedAt: now });
    await this.repositories.wallets.insert(wallet);
    if (!openingEntry) return wallet;                                                           // saldo zero: sem OPENING, sem lançamento, sem evento

    const opening = WagerTransaction.createOpening({ id: openingTransactionId, walletId, playerId: command.playerId, money: command.initialBalance, payloadHash: computeOpeningPayloadHash(walletId, command.initialBalance), correlationId, createdAt: now });
    opening.markProcessed(undefined, wallet.balance, now);
    await this.repositories.transactions.insert(opening);
    await this.repositories.ledger.insert(openingEntry);

    const eventContext = () => ({ eventId: this.ids.newId(), correlationId, causationId: `create-wallet:${walletId}`, occurredAt: now });
    const events = [WagerTransactionProcessed.from(opening, eventContext()), WalletBalanceChanged.from(wallet, openingEntry, eventContext())];
    await this.repositories.outbox.insert(events.map((event) => OutboxMessage.enqueue(event)));
    transactionsTotal.inc({ kind: opening.kind, status: opening.status, channel: "http" });
    return wallet;
  }
}
