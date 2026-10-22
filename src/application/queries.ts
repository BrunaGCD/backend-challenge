// ====================================================================================================
//                     CONSULTAS: WALLET, LEDGER COM CURSOR, TRANSAÇÕES E RECONCILIAÇÃO
// ====================================================================================================
import type { Money } from "../domain/money";
import type { Wallet } from "../domain/wallet/wallet";
import type { WalletLedgerEntry } from "../domain/wallet/wallet-ledger-entry";
import type { WagerTransaction } from "../domain/wagering/wager-transaction";
import { logger } from "../observability/logger";                                               // divergência vai para log
import { reconciliationDivergencesTotal } from "../observability/metrics";                      // e para métrica
import { InvalidCursorError, NotFoundError, ValidationError } from "./errors";
import type { Repositories } from "./ports";

export interface LedgerPage {
  entries: WalletLedgerEntry[];
  nextCursor: string | null;
}

export interface ReconciliationResult {
  walletId: string;
  storedBalance: Money;
  calculatedBalance: Money;
  difference: Money;
  consistent: boolean;
  checkedEntries: number;
}

// ----------------------------------------------------------------------------------------------------
//            CURSOR OPACO: {"after": <wallet_version>} EM BASE64URL (SEÇÃO 9 DO DESAFIO)
// ----------------------------------------------------------------------------------------------------
export function encodeLedgerCursor(afterWalletVersion: number): string {
  return Buffer.from(JSON.stringify({ after: afterWalletVersion }), "utf8").toString("base64url");
}

export function decodeLedgerCursor(cursor: string): number {
  try {
    if (!/^[A-Za-z0-9_-]+$/.test(cursor)) throw new Error("fora do alfabeto base64url");
    const decoded = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8"));
    const keys = Object.keys(decoded ?? {});
    if (keys.length !== 1 || keys[0] !== "after" || !Number.isSafeInteger(decoded.after) || decoded.after < 0) throw new Error("formato inesperado");
    return decoded.after;
  } catch {
    throw new InvalidCursorError("cursor inválido");
  }
}

export class WalletQueries {
  constructor(
    private readonly repositories: Repositories,
    private readonly ledgerPageLimits: { defaultLimit: number; maxLimit: number },
  ) {}

  async getWallet(walletId: string): Promise<Wallet> {
    const wallet = await this.repositories.wallets.findById(walletId);
    if (!wallet) throw new NotFoundError(`wallet ${walletId} não encontrada`);
    return wallet;
  }

  async getLedgerPage(walletId: string, cursor: string | undefined, rawLimit: string | undefined): Promise<LedgerPage> {
    const limit = this.parseLimit(rawLimit);
    const afterWalletVersion = cursor === undefined ? 0 : decodeLedgerCursor(cursor);
    await this.getWallet(walletId);                                                             // wallet inexistente é 404, não página vazia
    const rows = await this.repositories.ledger.listByWallet(walletId, afterWalletVersion, limit + 1);   // uma a mais diz se existe próxima página
    const entries = rows.slice(0, limit);
    const hasNextPage = rows.length > limit;
    const lastEntry = entries.at(-1);
    return { entries, nextCursor: hasNextPage && lastEntry ? encodeLedgerCursor(lastEntry.walletVersion) : null };
  }

  async getTransaction(transactionId: string): Promise<WagerTransaction> {
    const transaction = await this.repositories.transactions.findById(transactionId);
    if (!transaction) throw new NotFoundError(`transação ${transactionId} não encontrada`);
    return transaction;
  }

  async getTransactionByProvider(providerId: string, externalTransactionId: string): Promise<WagerTransaction> {
    const transaction = await this.repositories.transactions.findByProviderAndExternalId(providerId, externalTransactionId);
    if (!transaction) throw new NotFoundError(`transação ${externalTransactionId} do provedor ${providerId} não encontrada`);
    return transaction;
  }

  // O POST executa uma verificação: nada é gravado e nada é corrigido.
  async reconcile(walletId: string): Promise<ReconciliationResult> {
    const row = await this.repositories.ledger.reconcile(walletId);
    if (!row) throw new NotFoundError(`wallet ${walletId} não encontrada`);
    const calculatedBalance = row.totalCredits.subtract(row.totalDebits);                       // Σ créditos − Σ débitos
    const difference = row.storedBalance.subtract(calculatedBalance);
    const consistent = difference.isZero();
    if (!consistent) {
      reconciliationDivergencesTotal.inc();
      logger.error({ event: "reconciliation_divergence", walletId, checkedEntries: row.checkedEntries }, "saldo da wallet diferente do ledger; nada foi corrigido");   // sem valores no log
    }
    return { walletId, storedBalance: row.storedBalance, calculatedBalance, difference, consistent, checkedEntries: row.checkedEntries };
  }

  private parseLimit(rawLimit: string | undefined): number {
    if (rawLimit === undefined) return this.ledgerPageLimits.defaultLimit;
    const limit = Number(rawLimit);
    if (!/^[0-9]+$/.test(rawLimit) || !Number.isSafeInteger(limit) || limit < 1 || limit > this.ledgerPageLimits.maxLimit) {
      throw new ValidationError([{ field: "limit", message: `precisa ser um inteiro de 1 a ${this.ledgerPageLimits.maxLimit}` }]);
    }
    return limit;
  }
}
