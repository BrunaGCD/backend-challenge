// ====================================================================================================
//     INVARIANTE FINAL: SALDO DA WALLET == SALDO RECONSTRUÍDO PELO LEDGER (SEÇÃO 13 DO DESAFIO)
// ====================================================================================================
// Usado nos testes de unidade da Wallet (com os lançamentos devolvidos por debit/credit) e, pelo
// verificador do banco, nos testes de integração e concorrência.
import { Money } from "../../src/domain/money";                                                 // soma exata
import { LedgerDirection, type WalletLedgerEntry } from "../../src/domain/wallet-ledger-entry";  // lançamentos

export function ledgerChainProblems(currentBalance: Money, entries: WalletLedgerEntry[]): string[] {
  const problems: string[] = [];
  const ordered = [...entries].sort((first, second) => first.walletVersion - second.walletVersion);

  let reconstructed = Money.zero(currentBalance.currency);
  for (const entry of ordered) {
    reconstructed = entry.direction === LedgerDirection.Credit ? reconstructed.add(entry.money) : reconstructed.subtract(entry.money);
  }
  if (!reconstructed.equals(currentBalance)) problems.push(`saldo ${currentBalance} diferente do reconstruído pelo ledger ${reconstructed}`);

  for (let index = 1; index < ordered.length; index++) {
    const previous = ordered[index - 1]!;
    const current = ordered[index]!;
    if (!current.balanceBefore.equals(previous.balanceAfter)) problems.push(`versão ${current.walletVersion}: balanceBefore ${current.balanceBefore} != balanceAfter anterior ${previous.balanceAfter}`);
    if (current.walletVersion !== previous.walletVersion + 1) problems.push(`buraco nas versões: ${previous.walletVersion} -> ${current.walletVersion}`);
  }
  if (ordered.length > 0 && ordered[0]!.walletVersion === 1 && !ordered[0]!.balanceBefore.isZero()) problems.push("o lançamento da versão 1 precisa partir de zero");
  for (const entry of ordered) {
    if (!entry.isBalanced()) problems.push(`lançamento ${entry.id} não fecha a conta`);
  }
  return problems;
}
