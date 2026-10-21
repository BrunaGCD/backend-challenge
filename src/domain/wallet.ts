// ====================================================================================================
//                                 WALLET: AGGREGATE ROOT DO SALDO
// ====================================================================================================
// debit e credit devolvem o lançamento junto com a mudança de saldo, então não existe caminho para
// mexer num sem o outro. version começa em 1 e só sobe quando o saldo muda.
import { CurrencyMismatchError, InsufficientFundsError, InvalidWalletError } from "./errors";   // violações da wallet
import { Money } from "./money";                                                                // saldo exato
import { LedgerDirection, WalletLedgerEntry } from "./wallet-ledger-entry";                      // lançamento que nasce junto

export interface WalletState {
  id: string;
  playerId: string;
  currency: string;
  balance: Money;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface OpenWalletProps {
  id: string;
  playerId: string;
  initialBalance: Money;
  openingTransactionId: string;                                                                 // id da transação OPENING, se houver saldo
  openingLedgerEntryId: string;
  openedAt: Date;
}

export interface MovementContext {
  transactionId: string;
  ledgerEntryId: string;
  at: Date;
}

export class Wallet {
  private constructor(
    public readonly id: string,
    public readonly playerId: string,
    public readonly currency: string,
    private _balance: Money,
    private _version: number,
    public readonly createdAt: Date,
    private _updatedAt: Date,
  ) {}

  static open(props: OpenWalletProps): { wallet: Wallet; openingEntry?: WalletLedgerEntry } {
    if (props.initialBalance.isNegative()) throw new InvalidWalletError("saldo inicial não pode ser negativo");
    const wallet = new Wallet(props.id, props.playerId, props.initialBalance.currency, props.initialBalance, 1, props.openedAt, props.openedAt);
    if (props.initialBalance.isZero()) return { wallet };                                       // saldo zero: sem OPENING e sem lançamento

    const openingEntry = WalletLedgerEntry.create({
      id: props.openingLedgerEntryId,
      walletId: props.id,
      transactionId: props.openingTransactionId,
      direction: LedgerDirection.Credit,
      money: props.initialBalance,
      balanceBefore: Money.zero(props.initialBalance.currency),                                 // antes da abertura não havia nada
      balanceAfter: props.initialBalance,
      walletVersion: 1,                                                                         // a wallet já nasce na versão 1 com o saldo
      createdAt: props.openedAt,
    });
    return { wallet, openingEntry };
  }

  static rehydrate(state: WalletState): Wallet {
    return new Wallet(state.id, state.playerId, state.currency, state.balance, state.version, state.createdAt, state.updatedAt);
  }

  get balance(): Money {
    return this._balance;
  }

  get version(): number {
    return this._version;
  }

  get updatedAt(): Date {
    return this._updatedAt;
  }

  debit(money: Money, context: MovementContext): WalletLedgerEntry {
    this.assertSameCurrency(money);
    if (this._balance.isLessThan(money)) {
      throw new InsufficientFundsError(`saldo insuficiente na wallet ${this.id}`);              // sem valores na mensagem: ela pode ir para log
    }
    return this.applyMovement(LedgerDirection.Debit, money, context);
  }

  credit(money: Money, context: MovementContext): WalletLedgerEntry {
    this.assertSameCurrency(money);
    return this.applyMovement(LedgerDirection.Credit, money, context);
  }

  private applyMovement(direction: LedgerDirection, money: Money, context: MovementContext): WalletLedgerEntry {
    const balanceBefore = this._balance;
    const balanceAfter = direction === LedgerDirection.Credit ? balanceBefore.add(money) : balanceBefore.subtract(money);
    const nextVersion = this._version + 1;
    const entry = WalletLedgerEntry.create({                                                    // valida antes de mudar o estado
      id: context.ledgerEntryId,
      walletId: this.id,
      transactionId: context.transactionId,
      direction,
      money,
      balanceBefore,
      balanceAfter,
      walletVersion: nextVersion,
      createdAt: context.at,
    });
    this._balance = balanceAfter;
    this._version = nextVersion;
    this._updatedAt = context.at;
    return entry;
  }

  private assertSameCurrency(money: Money): void {
    if (money.currency !== this.currency) {
      throw new CurrencyMismatchError(`moeda ${money.currency} diferente da moeda da wallet ${this.currency}`);
    }
  }
}
