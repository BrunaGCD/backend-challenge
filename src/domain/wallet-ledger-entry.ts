// ====================================================================================================
//                         LANÇAMENTO DO LEDGER: IMUTÁVEL POR CONSTRUÇÃO
// ====================================================================================================
// Sem campo mutável e sem método de transição. A factory confere a aritmética antes de deixar nascer.
import { InvalidLedgerEntryError } from "./errors";                                             // aritmética ou campos inválidos
import { Money } from "./money";                                                                // valores exatos

export enum LedgerDirection {
  Debit = "DEBIT",
  Credit = "CREDIT",
}

export interface LedgerEntryState {
  id: string;
  walletId: string;
  transactionId: string;
  direction: LedgerDirection;
  money: Money;
  balanceBefore: Money;
  balanceAfter: Money;
  walletVersion: number;                                                                        // versão da wallet depois do movimento
  createdAt: Date;
}

export type CreateLedgerEntryProps = LedgerEntryState;

export class WalletLedgerEntry {
  private constructor(
    public readonly id: string,
    public readonly walletId: string,
    public readonly transactionId: string,
    public readonly direction: LedgerDirection,
    public readonly money: Money,
    public readonly balanceBefore: Money,
    public readonly balanceAfter: Money,
    public readonly walletVersion: number,
    public readonly createdAt: Date,
  ) {
    Object.freeze(this);
  }

  static create(props: CreateLedgerEntryProps): WalletLedgerEntry {
    if (!props.money.isPositive()) throw new InvalidLedgerEntryError("lançamento precisa de valor maior que zero");
    if (props.balanceBefore.isNegative() || props.balanceAfter.isNegative()) throw new InvalidLedgerEntryError("saldo do lançamento não pode ser negativo");
    if (!Number.isInteger(props.walletVersion) || props.walletVersion < 1) throw new InvalidLedgerEntryError(`wallet_version inválido: ${props.walletVersion}`);
    const entry = WalletLedgerEntry.rehydrate(props);                                           // monta e só depois confere a conta
    if (!entry.isBalanced()) {
      throw new InvalidLedgerEntryError(`lançamento não fecha: ${props.balanceBefore} ${props.direction} ${props.money} != ${props.balanceAfter}`);
    }
    return entry;
  }

  static rehydrate(state: LedgerEntryState): WalletLedgerEntry {
    return new WalletLedgerEntry(state.id, state.walletId, state.transactionId, state.direction, state.money, state.balanceBefore, state.balanceAfter, state.walletVersion, state.createdAt);
  }

  isBalanced(): boolean {
    const sameCurrency = this.money.currency === this.balanceBefore.currency && this.money.currency === this.balanceAfter.currency;
    if (!sameCurrency) return false;
    const expectedAfter = this.direction === LedgerDirection.Credit ? this.balanceBefore.add(this.money) : this.balanceBefore.subtract(this.money);
    return expectedAfter.equals(this.balanceAfter);
  }
}
