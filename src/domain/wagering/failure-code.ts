// ====================================================================================================
//                   CÓDIGOS DE FALHA GRAVADOS NA TRANSAÇÃO (SEÇÃO 7.2 DO DESAFIO)
// ====================================================================================================
// Estáveis: código publicado não muda de significado; código novo é acrescentado, nunca renomeado.

export enum FailureCode {
  InsufficientFunds = "INSUFFICIENT_FUNDS",                                                     // aposta maior que o saldo
  ReversalInsufficientFunds = "REVERSAL_INSUFFICIENT_FUNDS",                                    // reversão deixaria o saldo negativo
  CurrencyMismatch = "CURRENCY_MISMATCH",                                                       // moeda diferente da wallet
  PlayerWalletMismatch = "PLAYER_WALLET_MISMATCH",                                              // playerId não é o dono da wallet
  ReferenceMismatch = "REFERENCE_MISMATCH",                                                     // referência de outro player, wallet, moeda ou rodada
  ReferenceKindNotAllowed = "REFERENCE_KIND_NOT_ALLOWED",                                       // tipo de referência que a regra 3 não permite
  ReferenceAmountMismatch = "REFERENCE_AMOUNT_MISMATCH",                                        // reversão com valor diferente da referência
  ReferenceNotProcessed = "REFERENCE_NOT_PROCESSED",                                            // referência REJECTED ou FAILED
  ReferenceAlreadyReversed = "REFERENCE_ALREADY_REVERSED",                                      // referência já tem uma reversão processada
  ReferenceNotFound = "REFERENCE_NOT_FOUND",                                                    // referência não chegou dentro do TTL
  ProcessingFailed = "PROCESSING_FAILED",                                                       // erro permanente ao reprocessar uma pendente
}
