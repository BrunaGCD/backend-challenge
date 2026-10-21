// ====================================================================================================
//                                       ERROS DE DOMÍNIO
// ====================================================================================================
// Cada violação de regra tem o seu tipo; o caso de uso traduz o tipo no failureCode da seção 7.2.

export abstract class DomainError extends Error {
  constructor(message: string) {
    super(message);
    this.name = new.target.name;                                                                // nome da subclasse aparece no erro
  }
}

export class InvalidMoneyError extends DomainError {}                                           // formato de valor ou moeda inválido
export class CurrencyMismatchError extends DomainError {}                                       // conta entre moedas diferentes
export class InsufficientFundsError extends DomainError {}                                      // débito maior que o saldo
export class InvalidLedgerEntryError extends DomainError {}                                     // aritmética do lançamento não fecha
export class InvalidWalletError extends DomainError {}                                          // dados de abertura da wallet inválidos
export class InvalidWagerTransactionError extends DomainError {}                                // transação que não pode nem nascer
export class InvalidTransactionStateError extends DomainError {}                                // transição a partir de estado terminal (bug)
