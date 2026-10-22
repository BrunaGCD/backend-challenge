# Regras de negócio

Como uma transação de aposta é decidida: os estados que ela atravessa, a ordem em que as regras da seção 7 do desafio são conferidas, as interpretações que adotei onde o enunciado deixava espaço, o tratamento das referências que chegam fora de ordem e os códigos de falha que o provedor recebe. O resumo das decisões está no [ARCHITECTURE.md](../ARCHITECTURE.md#regras-de-negócio).

## Estados da transação

`PENDING` existe só em memória: a transação nasce assim na factory e sai dele antes do commit, e o CHECK do banco nem aceita esse valor. A classe segue a forma do esqueleto da seção 6.3 do desafio: os campos de identidade e de negócio são `public readonly` no construtor privado, e só o estado da decisão (`_status`, `_referenceTransactionId`, `_failureCode`, `_processedAt`, mais o saldo observado e o agendamento do worker) é privado e mutável, lido por getters e mudado só pelas transições. As transições válidas, em [wager-transaction.ts](../src/domain/wagering/wager-transaction.ts):

| De | Para | Quando |
|---|---|---|
| `PENDING` | `PROCESSED` | aplicada |
| `PENDING` | `REJECTED` | violou regra de negócio |
| `PENDING` | `PENDING_REFERENCE` | a referência ainda não existe (ou também está pendente) |
| `PENDING_REFERENCE` | `PROCESSED` | a referência chegou e a transação foi aplicada |
| `PENDING_REFERENCE` | `REJECTED` | a referência chegou e a regra foi violada, ou o TTL acabou (`REFERENCE_NOT_FOUND`) |
| `PENDING_REFERENCE` | `FAILED` | erro permanente no reprocessamento pelo worker |

Sair de um estado terminal lança `InvalidTransactionStateError`, e no banco o trigger recusa o `UPDATE`. `PENDING → FAILED` não existe: no fluxo síncrono, um erro de infraestrutura desfaz a transação SQL inteira e nada é gravado. Uma nova tentativa do worker sobre uma pendente que continua sem referência não é transição: `scheduleReferenceRetry` só conta a tentativa e reagenda.

## As regras e a ordem das checagens

As regras da seção 7 do desafio estão em [wager-settlement.ts](../src/domain/wagering/wager-settlement.ts), que recebe a wallet já travada e a referência já buscada e decide processar, rejeitar ou esperar. A ordem das checagens: dono da wallet e moeda; depois, para quem tem referência, a identidade da referência (mesmo provedor, player, wallet, moeda e rodada), o tipo permitido e o valor (nas reversões); só então o estado da referência (pendente faz esperar, `REJECTED`/`FAILED` rejeita) e a reversão única. Coloquei identidade, tipo e valor antes do estado porque esses dados da referência nunca mudam, então uma referência errada é rejeitada na hora, mesmo que ainda esteja pendente, em vez de ficar esperando à toa.

## Interpretações

Onde o enunciado deixava espaço, adotei as interpretações abaixo. Os códigos I1 a I8 são citados nos comentários do código e nos outros documentos.

- **I1. Uma reversão por referência, somando `REFUND` e `ROLLBACK`.** Pela letra da regra 4, uma `BET` poderia receber um `REFUND` e depois um `ROLLBACK`, e os dois creditariam o mesmo valor, o que viola "o sistema não pode duplicar créditos". A segunda volta `REFERENCE_ALREADY_REVERSED`, e no banco é o índice único parcial. O `ROLLBACK` de um `REFUND` continua permitido, porque a referência dele é o `REFUND`.
- **I2. Referência que existe mas não está `PROCESSED`.** Pendente: a dependente também espera. `REJECTED` ou `FAILED`: `REFERENCE_NOT_PROCESSED`, porque uma transação que nunca foi aplicada não tem efeito para reverter.
- **I3. Referência por tipo.** Obrigatória em `REFUND`/`ROLLBACK`, opcional em `WIN`/`LOSS`, proibida em `BET` (400, para o campo não ser ignorado em silêncio). `WIN`/`LOSS` com referência só apontam para `BET`, com qualquer valor; vários `WIN` podem apontar para a mesma `BET`, e um `WIN` não conta como reversão. Uma transação que referencia o próprio `externalTransactionId` apontaria para uma do próprio tipo, o que a regra 3 nunca permite (`REFUND` só aponta para `BET`, `ROLLBACK` não aponta para `ROLLBACK`, `WIN`/`LOSS` só para `BET`), então a liquidação a rejeita na hora com `REFERENCE_KIND_NOT_ALLOWED` (422, gravada, com o evento `WagerTransactionRejected`), antes de procurar a referência, em vez de deixá-la em `PENDING_REFERENCE` (202) esperando até o TTL uma referência que nunca chega.
- **I4. `gameId` não é comparado** na validação da referência; a regra 2 lista provider, player, wallet, moeda e rodada.
- **I5. Não existe estado de rodada.** São permitidos várias `BET` na mesma rodada, `WIN`/`LOSS` sem `BET` antes, `WIN` depois de uma `BET` revertida, `ROLLBACK` de uma `BET` que já teve `WIN`. As invariantes financeiras continuam protegidas pelas regras de referência, de reversão única e de saldo não negativo.
- **I6. Valor zero.** `BET`, `WIN`, `REFUND` e `ROLLBACK` exigem valor maior que zero (um movimento de zero geraria um lançamento sem mudança de saldo, o que contradiz a regra do `version`); `LOSS` aceita zero; saldo inicial zero é aceito, sem `OPENING`.
- **I7. Dono da wallet e identidade do provedor.** Em toda operação, o `playerId` precisa ser o dono da `walletId` (`PLAYER_WALLET_MISMATCH`). Não há cadastro de provedores; as validações são o formato do `providerId`, o valor reservado `internal` proibido, e a referência buscada sempre dentro do mesmo `providerId`. Valem também para mensagens da fila.
- **I8. Transação `OPENING`.** Criar wallet com saldo maior que zero gera, na mesma transação SQL, uma transação interna `OPENING` com `providerId`, `externalTransactionId`, `roundId` e `gameId` nulos (nenhum valor inventado numa tabela de auditoria), `idempotencyKey = "opening:{walletId}"`, o lançamento `CREDIT` com `wallet_version = 1` e os eventos `WagerTransactionProcessed` e `WalletBalanceChanged`.

## Referências fora de ordem

Uma transação cuja referência ainda não existe é gravada em `PENDING_REFERENCE` (HTTP responde 202) e o **worker** a reprocessa ([process-wager-transaction.ts](../src/application/wagering/process-wager-transaction.ts), `processNextDuePending`). O worker roda a cada 1 s em todas as instâncias e pega uma pendente por transação SQL com `FOR UPDATE SKIP LOCKED`, então duas instâncias nunca pegam a mesma. A ordem de locks é linha da pendente e depois wallet, o que não forma ciclo com o HTTP, que nunca trava uma linha existente de `wager_transactions`.

**Só o worker reprocessa**, e a chegada da referência não acorda as pendentes, de propósito. Acordar seria um `UPDATE` na linha da pendente dentro da transação do HTTP ou da fila, que já travou a wallet, enquanto o worker trava a pendente e depois a wallet; com as duas ordens opostas, as duas transações podem esperar uma pela outra (deadlock), e esse ciclo hoje não existe.

O intervalo entre tentativas usa o backoff comum aos três pontos que reagendam trabalho ([backoff.ts](../src/domain/backoff.ts)): exponencial com **full jitter**, `espera = aleatório(0, min(teto, base × 2^tentativa))`, base 1 s e teto 10 s no worker (`PENDING_RETRY_CAP_MS`). O aleatório entra por porta, então nos testes a espera é determinística. É o teto que limita a demora depois que a referência chega: a pendente é decidida na tentativa seguinte, que estava agendada para no máximo 10 s depois da anterior, e o worker a pega em até 1 s depois disso, então ela espera até uns 11 s depois da chegada da referência. Escolhi um teto curto para quem testa à mão, mandando o `REFUND` antes da `BET`, não ver a pendente parada por quase um minuto ([o teto de antes](medicoes-e-historico.md#o-teto-do-backoff-das-pendentes)). O custo é mais tentativas: uma pendente que espera os 10 minutos do TTL inteiros é tentada umas 110 vezes, cada uma uma transação curta que trava a linha dela e busca a referência pelo índice.

**TTL de 10 minutos**, contado do `created_at` da pendente. Um prazo em tempo responde à pergunta "por quanto tempo a referência ainda pode estar a caminho?": uma mensagem da referência com falha transitória fica tentando de novo no consumidor por até 5 minutos, contados da primeira falha ([o orçamento da fila](fila-e-outbox.md#o-limite-de-tentativas-é-um-orçamento-de-tempo)), mais a duração da última tentativa (até os 15 s de `statement_timeout` da fila), e só então vai para a DLQ. O TTL precisa ser maior que isso, e 10 minutos é o dobro, com folga para os retries do próprio provedor no HTTP. A conta não é um limite absoluto: se a mensagem estiver atrás de outra da mesma wallet que também falha, ela espera o orçamento da da frente antes de começar o seu, e uma instância que morre com ela em mãos faz a conta recomeçar na próxima. Esgotado o prazo, a transação vira `REJECTED` com `REFERENCE_NOT_FOUND` e sai o evento `WagerTransactionRejected`.

Erro transitório no worker desfaz a transação SQL e a pendente volta na próxima varredura. Erro permanente (violação de constraint, invariante quebrada, qualquer erro fora da lista de transitórios) faz uma transação SQL separada marcar `FAILED` com `PROCESSING_FAILED` e emitir `WagerTransactionFailed`, para o provedor que recebeu 202 saber que ela terminou.

## Códigos de falha

Códigos em `UPPER_SNAKE_CASE`, estáveis (código publicado não muda de significado; código novo é acrescentado). Cada um diz ao provedor uma de três ações: reenviar (a mesma requisição, a mesma key), corrigir (se a rejeição foi gravada, a correção precisa de um novo `externalTransactionId` e uma nova key) ou desistir.

| Código | Onde | Quando | Ação |
|---|---|---|---|
| `INSUFFICIENT_FUNDS` | transação `REJECTED`, 422 | `BET` maior que o saldo | desistir |
| `REVERSAL_INSUFFICIENT_FUNDS` | `REJECTED`, 422 | `ROLLBACK` de `WIN`/`REFUND` deixaria o saldo negativo | desistir e escalar: o dinheiro já saiu da wallet |
| `CURRENCY_MISMATCH` | `REJECTED`, 422 | moeda diferente da wallet | corrigir |
| `PLAYER_WALLET_MISMATCH` | `REJECTED`, 422 | `playerId` não é o dono da wallet | corrigir |
| `REFERENCE_MISMATCH` | `REJECTED`, 422 | referência de outro player, wallet, moeda ou rodada | corrigir |
| `REFERENCE_KIND_NOT_ALLOWED` | `REJECTED`, 422 | tipo de referência que a regra 3 não permite, inclusive a transação que referencia a si mesma (I3) | corrigir |
| `REFERENCE_AMOUNT_MISMATCH` | `REJECTED`, 422 | reversão com valor diferente da referência | corrigir |
| `REFERENCE_NOT_PROCESSED` | `REJECTED`, 422 | referência `REJECTED` ou `FAILED` | desistir |
| `REFERENCE_ALREADY_REVERSED` | `REJECTED`, 422 | referência já tem uma reversão processada | desistir |
| `REFERENCE_NOT_FOUND` | `REJECTED` | a referência não chegou dentro do TTL | desistir e investigar |
| `PROCESSING_FAILED` | transação `FAILED` | erro permanente no worker | desistir e escalar |
| `WALLET_NOT_FOUND` | erro 422, não gravado | a wallet não existe (a chave estrangeira impede gravar) | corrigir |
| `VALIDATION_ERROR`, `IDEMPOTENCY_KEY_MISSING`, `INVALID_CURSOR` | erro 400 | payload, header ou cursor inválido; nada gravado | corrigir |
| `IDEMPOTENCY_KEY_CONFLICT`, `EXTERNAL_TRANSACTION_ID_CONFLICT`, `WALLET_ALREADY_EXISTS` | erro 409 | outra operação com a mesma key, o mesmo id externo, ou wallet repetida | desistir e investigar |
| `NOT_FOUND` | erro 404 | consulta de recurso inexistente | corrigir |
| `SERVICE_UNAVAILABLE` | erro 503 com `Retry-After` | falha transitória; nada gravado | reenviar com a mesma key |
| `INTERNAL_ERROR` | erro 500 | erro inesperado; nada gravado | reenviar é seguro; se repetir, escalar |

`INSUFFICIENT_FUNDS` e `REVERSAL_INSUFFICIENT_FUNDS` são separados porque são situações diferentes: o primeiro é rotina (o jogador não tem saldo para apostar), o segundo é um problema operacional (o provedor quer desfazer um crédito que o jogador já gastou).
