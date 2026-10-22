# API HTTP e idempotência

O contrato da API visto por quem a chama: o status de cada situação, o formato dos corpos, como a idempotência funciona (inclusive o algoritmo do `payloadHash`), a reconciliação e o cursor do ledger. As rotas e exemplos de `curl` estão no [README](../README.md#usar-a-api); o resumo das decisões, no [ARCHITECTURE.md](../ARCHITECTURE.md#api-http-e-idempotência).

## Status e corpos

| Situação | Status | Corpo |
|---|---|---|
| processada (inclusive `LOSS`) e o replay dela | 200 | transação |
| aceite pendente (`PENDING_REFERENCE`) | 202 | transação |
| rejeição de negócio | 422 | transação com `failureCode` (ou erro, em `WALLET_NOT_FOUND`) |
| payload inválido ou header ausente | 400 | erro |
| conflito de idempotência | 409 | erro |
| falha transitória | 503 com `Retry-After` | erro |
| erro inesperado | 500 | erro |
| wallet criada / duplicada | 201 / 409 | wallet / erro |
| recurso não encontrado | 404 | erro |
| reconciliação, consistente ou não | 200 | resultado |

A regra dos corpos: se existe uma `WagerTransaction`, o corpo é a transação; se não existe, é um erro em Problem Details (RFC 9457, `application/problem+json`, com `type`, `title`, `status`, `detail`), mais a extensão `code` e, no 400, `errors` com os campos. JSON malformado também sai nesse formato: o parser do Express é registrado à mão ([http-app.ts](../src/infrastructure/http/http-app.ts)) para isso. A lista completa de códigos, com a ação esperada do provedor em cada um, está em [regras de negócio](regras-de-negocio.md#códigos-de-falha).

**Divergência consciente do draft da IETF do header `Idempotency-Key`.** O draft recomenda 422 para key reutilizada com outro payload; aqui é 409. O enunciado chama esse caso de conflito, as cinco situações precisam de cinco códigos diferentes, e o 422 ficou para a rejeição de negócio, que é o caso que mais se encaixa na definição de 422 da RFC 9110. O draft é rascunho, não RFC.

Duas escolhas minhas, em casos que o enunciado não cobre: id malformado no caminho (`/wallets/abc`) responde 400 `VALIDATION_ERROR`, em vez de deixar o PostgreSQL recusar o UUID e virar 500; e o replay de uma transação `FAILED` (uma pendente que falhou no worker) responde **500 com o corpo da transação**, porque não é regra de negócio nem algo que o provedor corrija, e pela regra dos corpos, se a transação existe, o corpo é ela.

O header `X-Correlation-Id` é aceito na requisição (até 128 caracteres seguros; se não vier, nasce um UUIDv7) e devolvido na resposta.

## Idempotência

O header `Idempotency-Key` é obrigatório e é a fonte da verdade (1 a 255 caracteres ASCII visíveis; o formato `"{providerId}:{externalTransactionId}"` é o recomendado ao provedor, sem validação). No banco, `UNIQUE (idempotency_key)` global e, separado, `UNIQUE (provider_id, external_transaction_id)`. A key nunca expira, porque o registro é a própria transação financeira. Nada disso fica em memória: o teste "a idempotência é persistente" reinicia a aplicação e o replay continua batendo.

O fluxo ([process-wager-transaction.ts](../src/application/wagering/process-wager-transaction.ts)) segue a seção 8 do desafio: consulta rápida pela key, sem lock (replay não entra na fila do lock); abre a transação; na fila, `INSERT` na inbox; `SELECT ... FOR UPDATE` da wallet; consulta a key de novo, agora serializado pela wallet, o que fecha a corrida das cópias simultâneas; só então confere se o `(providerId, externalTransactionId)` já foi usado com outra key; decide e grava tudo; commit. Se o commit falhar com violação de unicidade na key ou no par (duas cópias com a mesma key e `walletId` diferentes travam wallets diferentes), a transação é desfeita e o caso de uso consulta de novo: mesmo hash é replay, hash diferente é conflito.

Repetir uma operação já decidida devolve o resultado original, inclusive o saldo observado na época (coluna `balance_after`) e inclusive rejeições (422 de novo, com `idempotentReplay: true`). Repetição enquanto a original está pendente devolve o estado atual (202 enquanto pendente, o resultado final depois). Erros 400, 409, 500 e 503 não são gravados, então reenviar depois de um 503 tenta de novo de verdade.

## O payloadHash

O **payloadHash** ([payload-hash.ts](../src/application/wagering/payload-hash.ts)) é o SHA-256 (hexadecimal minúsculo, de `node:crypto`) dos bytes UTF-8 do JSON canônico pela **RFC 8785 (JCS)** de um objeto montado a partir do comando **já validado e normalizado**, nunca do corpo bruto. A canonicalização usa o pacote `canonicalize`, que rodou no Bun sem ajuste.

Os campos: `providerId`, `externalTransactionId`, `playerId` e `walletId` (UUID em minúsculas), `roundId`, `gameId`, `kind`, `money` (`{ "amount": "25.00", "currency": "BRL" }`, como `Money.toJSON()` serializa) e `referenceExternalTransactionId` só quando presente (ausente e `null` são a mesma coisa). Não entram o header, o `data.idempotencyKey` da mensagem, `messageId`, `type`, `occurredAt` nem metadado de transporte, então a mesma operação chegando por HTTP e depois pela fila é replay.

Os vetores fixos em [payload-hash.test.ts](../test/unit/payload-hash.test.ts) tiveram o hash esperado calculado fora do projeto, com `sha256sum`, sobre o JSON canônico escrito à mão. O custo de fixar a lista de campos é que mudá-la quebraria os replays antigos; se um dia for preciso, o hash ganha um prefixo de versão.

## Reconciliação

`POST /wallets/:walletId/reconciliation` ([queries.ts](../src/application/queries.ts), [ledger-repository.ts](../src/infrastructure/persistence/ledger-repository.ts)): uma única consulta SQL lê o saldo gravado, a soma dos créditos, a soma dos débitos e a contagem de lançamentos, então a leitura usa um snapshot só e é coerente mesmo com transações sendo aplicadas, sem travar a wallet. Nada é gravado nem corrigido: divergência vira log de nível `error` (com `walletId` e `checkedEntries`, sem valores), incremento de `wagering_reconciliation_divergences_total` e `consistent: false` na resposta.

## Cursor do ledger

`GET /wallets/:walletId/ledger` pagina por chave pela coluna `wallet_version`, em ordem crescente (a de auditoria), com o cursor `{"after": <wallet_version>}` em base64url, opaco para o cliente. É estável porque o ledger só recebe lançamentos novos no fim, e a consulta é uma faixa no índice `UNIQUE (wallet_id, wallet_version)`.
