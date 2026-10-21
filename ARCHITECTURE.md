# Arquitetura

Este documento explica as decisões do projeto, o porquê de cada uma, o que elas custam e o que ficou de fora. O README diz como subir e testar; aqui está o raciocínio.

**Sumário:** 1. Visão geral · 2. Experimentos antes da implementação · 3. Dinheiro · 4. ORM e estratégia transacional · 5. Concorrência · 6. Schema, constraints e índices · 7. Estados da transação · 8. Regras de negócio e interpretações · 9. Referências fora de ordem · 10. Códigos de falha · 11. Idempotência e payloadHash · 12. API HTTP e status · 13. Fila de entrada · 14. Outbox e eventos · 15. Observabilidade · 16. Autenticação · 17. Testes · 18. Limitações e o que ficou de fora · 19. Desenvolvido com Claude Code

---

## 1. Visão geral

O serviço tem uma operação central, processar uma transação de aposta, que chega por três caminhos: `POST /wagering/transactions`, a fila `wager-transactions.fifo` e o worker que reprocessa transações à espera da referência. Os três chamam **o mesmo caso de uso** ([process-wager-transaction.ts](src/application/process-wager-transaction.ts)), então as regras e as garantias são as mesmas em qualquer canal.

As camadas:

- `src/domain`: `Money`, `Wallet`, `WalletLedgerEntry`, `WagerTransaction`, eventos, `InboxMessage`, `OutboxMessage` e a decisão de liquidação ([wager-settlement.ts](src/domain/wager-settlement.ts)), que concentra as regras da seção 7 do desafio. Não importa nada de NestJS, MikroORM nem de outras camadas; uma regra de lint (`no-restricted-imports` em [eslint.config.js](eslint.config.js)) impede. Ids e datas entram por parâmetro, então o domínio é determinístico em teste.
- `src/application`: casos de uso, as portas que eles usam ([ports.ts](src/application/ports.ts)), a validação do comando (a mesma para HTTP e fila), o `payloadHash` e o publisher da outbox. Também não depende de NestJS nem de MikroORM.
- `src/infrastructure`: as implementações das portas (repositórios MikroORM, `TransactionRunner`, SQS), o consumidor da fila, os laços de fundo e a borda HTTP em NestJS.
- [composition.ts](src/composition.ts): monta tudo com chamadas explícitas de construtor. O NestJS só recebe o que sai daqui, e os testes que injetam falha trocam uma porta por aqui, sem "modo de teste" no código de produção.

A garantia final de cada invariante está no banco, porque com várias instâncias ele é o único ponto que todas compartilham: lock de linha da wallet, `UNIQUE` da `idempotency_key` e de `(provider_id, external_transaction_id)`, inbox e outbox na mesma transação SQL do efeito, CHECKs e triggers. A ordem e a deduplicação do SQS FIFO são tratadas como otimização.

---

## 2. Experimentos antes da implementação

A especificação deixou duas escolhas condicionadas a experimento. Os roteiros estão em [experiments/](experiments/) e rodaram antes de qualquer código do projeto.

### E1: a stack no Bun (decide o ORM, o desligamento, logs, métricas e os testes com várias instâncias)

Projeto descartável com NestJS 11.2.7, MikroORM 7.2.5, pino 10.4.0 e prom-client 15.1.3 contra o `postgres:17-alpine` do compose, rodado duas vezes: no Windows (Bun 1.4.3) e num container `oven/bun:1.4.3` (Linux).

| Item | Windows | Linux | Evidência |
|---|---|---|---|
| 1 NestJS sobe e responde | passou | passou | `GET /ping` 200 |
| 2 persiste e lê com `defineEntity` | passou | passou | registro lido pelo id |
| 3 `PESSIMISTIC_WRITE` gera `for update` e a segunda espera | passou | passou | SQL `... limit 1 for update`; com a primeira segurando o lock por 1 s, a segunda só pegou o lock 14 ms (Windows) e 5 ms (Linux) depois de ela soltar |
| 4 `PESSIMISTIC_PARTIAL_WRITE` gera `for update skip locked` | passou | passou | 0 linhas vistas com a linha travada |
| 5 `numeric(18,2)` volta como string | passou | passou | `"25.00"` |
| 6 migration com CHECK e trigger, `up` e `down` por código | passou | passou | `orm.migrator.up/down` com `migrationsList` |
| 7 `SET LOCAL lock_timeout` vale só na transação | passou | passou | com pool de 1 conexão: `2s` dentro, `0` depois do commit |
| 8 `AsyncLocalStorage` sobrevive a `await` e consultas | passou | passou | o mesmo valor em 4 pontos, inclusive dentro de `em.transactional` |
| 9 `SIGTERM` dispara os hooks do NestJS | **falhou** | passou | no Windows o processo morre na hora (código 143) sem rodar o hook |
| 10 pino com os campos do `mixin` | passou | passou | linha JSON com `correlationId` e `walletId` |
| 11 prom-client e `/metrics` | passou | passou | contador e histograma no texto |
| 12 `Bun.spawn` + `SIGKILL` | passou | passou | saída 137, a porta para de responder |

Pelo critério da especificação, os itens 1 a 7 passaram, então o ORM é o **MikroORM 7**; os itens 8, 10 e 11 confirmam pino com `mixin` lendo o `AsyncLocalStorage` e prom-client; o item 12 confirma as 3 instâncias via `Bun.spawn` nos testes. O item 9 passou no Linux, que é onde a aplicação roda (containers do compose), então o desligamento ficou nos hooks do NestJS. A falha no Windows é do sistema, que não entrega `SIGTERM`, e por isso o teste de desligamento gracioso é pulado no Windows e roda no serviço `tests` do compose.

### E2: o MiniStack se comporta como o SQS FIFO? (decide o emulador)

Script com `@aws-sdk/client-sqs` contra `ministackorg/ministack:1.5.30`.

| Item | Resultado |
|---|---|
| 1 FIFO e DLQ FIFO com `RedrivePolicy` e `maxReceiveCount = 2` | passou |
| 2 um por vez no mesmo grupo (a segunda não vem com a primeira em voo) | passou |
| 3 grupos diferentes recebidos ao mesmo tempo por dois consumidores | passou |
| 4 deduplicação por `MessageDeduplicationId` | passou, com uma diferença (abaixo) |
| 5 `ChangeMessageVisibility` para 0 e para N segundos | passou |
| 6 `ApproximateReceiveCount` sobe e o redrive leva para a DLQ | passou (contagens 1 e 2, depois a mensagem aparece na DLQ) |
| 7 long polling de 20 s volta assim que a mensagem chega | passou (voltou em 2,22 s com a mensagem enviada aos 2 s) |
| 8 `ApproximateNumberOfMessages` da DLQ | passou |
| 9 `SendMessageBatch` com 10 | passou |

Tudo passou, então o emulador é o **MiniStack**, que sobe sem conta (desde março de 2026 a imagem `latest` do LocalStack exige token, o que obrigaria o avaliador a criar uma conta). A diferença do item 4: no envio duplicado, o MiniStack devolve o `MessageId` e o MD5 do corpo da mensagem **original**; se a cópia tiver corpo diferente, o SDK compara o MD5 com o corpo novo e lança `InvalidChecksumError`. Não confirmei o que a AWS devolve nesse caso. Isso não afeta o projeto, porque nos dois lugares em que ele reenvia com o mesmo `MessageDeduplicationId` (republicação de evento da outbox e reenvio explícito para a DLQ) o corpo é idêntico.

---

## 3. Dinheiro

O [`Money`](src/domain/money.ts) guarda o valor como **inteiro em centavos num `BigInt`**, mais a moeda, sem biblioteca de decimal. Escolhi assim porque nenhuma conta do sistema divide ou multiplica dinheiro (a escala é fixa em 2 e a reversão parcial está fora de escopo), então somar, subtrair, negar e comparar centavos é exato. O `BigInt` tem uma proteção a mais: o JavaScript lança `TypeError` quando alguém mistura `BigInt` com `number` numa conta, então um `number` que escape para dentro do `Money` quebra na hora em vez de perder precisão em silêncio. O `Money` é congelado (`Object.freeze`) e toda operação devolve outra instância.

Na entrada, o formato aceito é exatamente `^(0|[1-9][0-9]*)\.[0-9]{2}$` com até 16 dígitos inteiros. Nada é normalizado: `"25.5"` é rejeitado em vez de virar `"25.50"`, e `"10.005"` é rejeitado em vez de virar `"10.01"`, então cada valor tem uma grafia só, o que também resolve a questão do hash. A moeda segue `^[A-Z]{3}$` mais uma lista de moedas aceitas por configuração (`ACCEPTED_CURRENCIES`, padrão `BRL`); não validei contra a ISO 4217 inteira porque ela tem moedas com outro número de casas (o iene não tem casas), e a escala fixa de 2 só serve para moedas de 2 casas. O modelo é multi-moeda: cada wallet tem a sua, e operação entre moedas diferentes é erro de domínio.

No banco, o valor é `NUMERIC(18,2)` e a moeda `CHAR(3)`, em colunas separadas. O MikroORM devolve `numeric` como string (`"25.00"`, item 5 do E1), que vai direto para `Money.from`. O parser e o formatador de centavos são código próprio, com testes próprios em [money.test.ts](test/unit/money.test.ts), inclusive o de "arredondamento", que prova que nada é arredondado em silêncio.

---

## 4. ORM e estratégia transacional

**MikroORM 7** (resultado do E1), usado pelo caminho que o enunciado marca como preferencial: Unit of Work e Identity Map explícitos, `EntityManager.transactional()` e `LockMode`. Os registros de persistência ficam em [records.ts](src/infrastructure/persistence/records.ts), definidos com `defineEntity` sem decorators, e os repositórios convertem registro em objeto de domínio pela factory `rehydrate`. Não mapeei as classes de domínio direto no ORM porque elas têm construtor privado, estado encapsulado e o value object `Money`, e o domínio não pode depender de tipos do ORM. O custo é código de mapeamento nos repositórios; o ganho é um domínio testável sem banco. Cada um dos quatro termos tem um lugar no código:

- **`EntityManager.transactional()`.** Uma transação SQL por execução de caso de uso, aberta pela aplicação através da porta `TransactionRunner`, cuja implementação ([database.ts](src/infrastructure/persistence/database.ts)) faz um fork do `EntityManager` e chama `transactional()` nele. Não é uma transação por requisição, porque o mesmo caso de uso roda a partir do HTTP, do consumidor e do worker, e só um deles é requisição. Dentro da transação, os repositórios pegam o `EntityManager` dela pelo contexto (`TransactionContext` do MikroORM, que é um `AsyncLocalStorage`). O isolamento é o `READ COMMITTED` padrão, mais o lock de linha da wallet.
- **Unit of Work.** Os repositórios não escrevem SQL na hora: `insert` faz `em.persist()` de um registro novo, e `updateBalance`, `updateDecision` e `saveAttempt` mudam o registro que a transação carregou. O `transactional()` faz um flush só, no commit, e o `FlushMode.COMMIT` ([orm.ts](src/infrastructure/persistence/orm.ts)) garante que nenhuma consulta no meio do caminho dispare um flush antes da hora. A ordem dos INSERTs vem das relações declaradas nos registros (`manyToOne(...).mapToPk()`, que mantém o campo como o id, uma string): primeiro a wallet, depois a transação, depois o lançamento, qualquer que seja a ordem em que o caso de uso registrou. Como tudo sai no flush, a violação de unicidade e o conflito de versão aparecem ali, de dentro do `transactional()`, e a transação inteira é desfeita. O caso de uso já tratava o erro vindo do `run` inteiro, então a recuperação das cópias simultâneas continua igual: depois do rollback, consulta de novo pela key (§11). O runner traduz o `OptimisticLockError` do ORM para `ConcurrentUpdateError`, um erro da aplicação, que vira 500 no HTTP, nova tentativa na fila e `FAILED` no worker, como qualquer erro inesperado, e conta em `wagering_concurrency_conflicts_total{type="optimistic_lock"}`.
- **Identity Map.** Cada transação tem o seu, o do fork. Alterar uma linha exige que ela tenha sido lida nessa mesma transação: o repositório pega o registro direto do Identity Map (`getUnitOfWork().getById`), sem SQL, e recusa a alteração se ele não estiver lá, porque mudar uma linha que a transação não leu (nem travou) seria gravar sem controle de concorrência. As leituras que decidem alguma coisa dentro da transação (a wallet travada, a key consultada de novo depois do lock, o id externo, a referência, as linhas pegas com `SKIP LOCKED`) usam `refresh: true`, para valer o que está no banco mesmo que o registro já esteja no Identity Map.
- **`LockMode`.** `PESSIMISTIC_WRITE` (`SELECT ... FOR UPDATE`) na wallet e na pendente que vai virar `FAILED`; `PESSIMISTIC_PARTIAL_WRITE` (`FOR UPDATE SKIP LOCKED`) no worker de pendentes e nos publishers da outbox. Junto com o lock pessimista, a `version` da wallet é o optimistic lock do próprio MikroORM (`version: true`), explicado no §5.

No acesso a dados, ficou SQL escrito à mão em três lugares, cada um porque o ORM não resolve do mesmo jeito. Os `SET LOCAL lock_timeout` e `statement_timeout` no começo de cada transação são configuração da sessão, não dado. O `INSERT ... ON CONFLICT DO NOTHING RETURNING` da inbox precisa rodar na hora e dizer se a linha já existia, porque é essa resposta que decide se a mensagem é nova; pelo Unit of Work, a duplicata só apareceria no commit, como violação de unicidade, desfazendo a transação inteira em vez de devolver "já processada". E a soma da reconciliação é uma consulta só, então saldo e ledger saem do mesmo snapshot; pelo ORM seriam duas consultas, cada uma com o seu snapshot em `READ COMMITTED`. Fora do acesso a dados, há SQL nas migrations (o schema é escrito à mão, de propósito) e na readiness (§15).

O teste [unit-of-work.test.ts](test/integration/unit-of-work.test.ts) prova cada uma dessas peças no banco real: dentro da transação nada foi escrito ainda e no commit sai tudo, na ordem das chaves estrangeiras, mesmo registrado ao contrário; a segunda leitura da wallet é o mesmo registro do Identity Map, e alterar sem ter lido é recusado; o conflito de versão desfaz a transação; a `version` só sobe quando o saldo muda; e as duas corridas de unicidade (mesma key em wallets diferentes, mesmo id externo com outra key) terminam no 409 certo, com o erro vindo do flush. O teste de atomicidade força um flush no meio da transação e só então lança o erro, para provar que o rollback desfaz linhas que já tinham sido escritas.

**Migrations** versionadas e reversíveis pelo Migrator do MikroORM, rodadas por código ([orm.ts](src/infrastructure/persistence/orm.ts), [scripts/migrate.ts](scripts/migrate.ts)), com `up` e `down` escritos à mão e o CHECK, os índices parciais e os triggers em SQL explícito. O gerador de schema do ORM não é usado como fonte (o snapshot está desligado).

---

## 5. Concorrência

**Lock pessimista na linha da wallet** (`SELECT ... FOR UPDATE`, `LockMode.PESSIMISTIC_WRITE`), numa transação curta em `READ COMMITTED`, com duas defesas no schema: `CHECK (balance >= 0)` e `UNIQUE (wallet_id, wallet_version)` no ledger. Sem retry automático no caso feliz.

No PostgreSQL em `READ COMMITTED`, quem tenta travar uma linha já travada espera e, se o outro fez commit, trabalha com a versão atualizada da linha. No cenário obrigatório (saldo 100, duas apostas de 80), a segunda espera, lê o saldo já em 20 e é rejeitada com `INSUFFICIENT_FUNDS`, sem retry nenhum. Considerei as outras três opções: o lock otimista com `version` vira uma nova tentativa a cada conflito, e o desafio pede justamente conflito alto (50 cópias em paralelo, hot wallet), então com limite de tentativas uma operação válida pode falhar só por concorrência; o `UPDATE ... WHERE balance >= x` resolve o débito isolado, mas a transação também precisa ler a referência, conferir se ela já foi revertida e gravar `balanceBefore`, tudo vendo o mesmo estado da wallet, e o lock serializa essas leituras de graça; `SERIALIZABLE` exigiria retry em toda falha de serialização.

O lock trava exatamente uma wallet, então wallets diferentes andam em paralelo e não existe lock global (o teste "outra wallet não espera o lock de uma wallet travada" em [retry-and-dlq.test.ts](test/integration/retry-and-dlq.test.ts) segura o lock de uma wallet por fora e mostra outra wallet respondendo em menos de 500 ms). Como a referência de um `REFUND`/`ROLLBACK`/`WIN` precisa ser da mesma wallet, o lock serializa também tudo que envolve a referência, inclusive duas reversões concorrentes da mesma aposta. Cada transação trava uma wallet só, então duas transações financeiras não formam ciclo de espera entre wallets.

**O `version`** começa em 1 (o default da coluna, porque o MikroORM não manda a versão no INSERT e lê de volta a que o banco gravou) e sobe só quando o saldo muda. É o optimistic lock do próprio MikroORM (`version: true` no registro da wallet): o UPDATE que sai no flush é `SET balance = ?, updated_at = ?, version = <lida + 1> WHERE id = ? AND version = <lida>`, e zero linhas afetadas vira `OptimisticLockError` e desfaz a transação. É a combinação que a seção 8 do desafio permite: o `FOR UPDATE` é o mecanismo principal, e com ele ninguém muda a wallet entre a leitura e o commit, então a `version` é a segunda defesa, que pegaria um lost update se um bug pulasse o lock. A terceira é o `UNIQUE (wallet_id, wallet_version)` no ledger, porque cada lançamento grava a versão da wallet depois do movimento. A regra "só sobe quando o saldo muda" vem do próprio Unit of Work: ele só gera UPDATE quando algum campo do registro mudou, e os únicos que o repositório muda são o saldo e o `updatedAt`, então `LOSS`, rejeição ou uma leitura com lock sem movimento não geram UPDATE. O repositório ainda confere que a versão que o domínio calculou é a lida mais 1, para domínio e banco contarem igual.

**Timeouts.** Em cada transação, `SET LOCAL lock_timeout = '2s'` e `statement_timeout = '5s'`; na conexão, `idle_in_transaction_session_timeout = 10s`, para uma sessão travada no meio da transação não segurar o lock. Estourar o timeout é falha transitória: 503 no HTTP, retry na fila.

**Hot wallet: o custo medido.** No teste de hot wallet ([hot-wallet.test.ts](test/concurrency/hot-wallet.test.ts)), 300 operações misturadas numa wallet só, metade por HTTP nas 3 instâncias e metade pela fila, disparadas ao mesmo tempo, a métrica `wagering_wallet_lock_wait_seconds` registrou, numa execução no Windows, 475 travas com média de 665 ms de espera e, numa execução dentro do container Linux do compose, 367 travas com média de 249 ms. Parte das respostas HTTP passou dos 2 s de `lock_timeout` e voltou 503 (entre 92 e 146 nas execuções no Windows, 29 na do Linux), e o teste reenviou com a mesma key, como um provedor faria. Ou seja, a correção se manteve, mas a vazão de uma wallet é limitada pelo tempo de cada transação, que é o trade-off esperado do lock pessimista.

---

## 6. Schema, constraints e índices

As garantias de unicidade, imutabilidade e não-negatividade estão no schema ([migrations/](src/infrastructure/persistence/migrations/)), e o teste [migrations-and-constraints.test.ts](test/integration/migrations-and-constraints.test.ts) roda `up → down → up` num banco próprio e confere cada constraint com SQL direto.

- `wallets`: `CHECK (balance >= 0)`, `CHECK (version >= 1)`, formato da moeda, `UNIQUE (player_id, currency)` e `UNIQUE (id, currency)` (alvo da chave estrangeira composta do ledger).
- `wager_transactions`: `UNIQUE (idempotency_key)`; `UNIQUE (provider_id, external_transaction_id)`; índice único parcial em `reference_transaction_id` onde `kind IN ('REFUND','ROLLBACK') AND status = 'PROCESSED'` (uma reversão por referência, interpretação I1); CHECKs de tipo, de status (sem `PENDING`), de valor (zero só em `LOSS`), de referência por tipo, de `failure_code` se e somente se `REJECTED`/`FAILED`, de `processed_at` se e somente se `PROCESSED`, de reversão processada com referência interna, dos campos nulos só no `OPENING`, e do formato do `payload_hash`; índice parcial `(next_attempt_at) WHERE status = 'PENDING_REFERENCE'` para o worker. Um trigger proíbe `DELETE`, proíbe `UPDATE` de transação em estado terminal e proíbe mudar as colunas de identidade e de negócio mesmo enquanto pendente; outro proíbe `TRUNCATE`.
- `wallet_ledger_entries`: CHECK da aritmética (`balance_after = balance_before ± amount`, conforme a direção), valor positivo, saldos não negativos, `UNIQUE (wallet_id, transaction_id)`, `UNIQUE (wallet_id, wallet_version)`, chave estrangeira composta `(wallet_id, currency)` para a wallet, e triggers que proíbem `UPDATE`, `DELETE` e `TRUNCATE`.
- `inbox_messages` com chave primária `(consumer_name, message_id)`, e `outbox_messages` com índice parcial `(next_attempt_at) WHERE published_at IS NULL`.

**Uma coluna a mais do que a lista da especificação:** `wager_transactions.balance_currency`, ao lado de `balance_after`. Numa rejeição `CURRENCY_MISMATCH`, o saldo observado está na moeda da wallet, que é diferente da moeda da transação, então sem essa coluna não haveria como reidratar o saldo observado como `Money`. Segue a mesma regra do resto do schema (valor e moeda em colunas separadas), com um CHECK que exige as duas juntas.

---

## 7. Estados da transação

`PENDING` existe só em memória: a transação nasce assim na factory e sai dele antes do commit, e o CHECK do banco nem aceita esse valor. As transições válidas, em [wager-transaction.ts](src/domain/wager-transaction.ts):

| De | Para | Quando |
|---|---|---|
| `PENDING` | `PROCESSED` | aplicada |
| `PENDING` | `REJECTED` | violou regra de negócio |
| `PENDING` | `PENDING_REFERENCE` | a referência ainda não existe (ou também está pendente) |
| `PENDING_REFERENCE` | `PROCESSED` | a referência chegou e a transação foi aplicada |
| `PENDING_REFERENCE` | `REJECTED` | a referência chegou e a regra foi violada, ou o TTL acabou (`REFERENCE_NOT_FOUND`) |
| `PENDING_REFERENCE` | `FAILED` | erro permanente no reprocessamento pelo worker |

Sair de um estado terminal lança `InvalidTransactionStateError`, e no banco o trigger recusa o `UPDATE`. `PENDING → FAILED` não existe: no fluxo síncrono, um erro de infraestrutura desfaz a transação SQL inteira e nada é gravado. Uma nova tentativa do worker sobre uma pendente que continua sem referência não é transição: `scheduleReferenceRetry` só conta a tentativa e reagenda.

---

## 8. Regras de negócio e interpretações

As regras da seção 7 do desafio estão em [wager-settlement.ts](src/domain/wager-settlement.ts), que recebe a wallet já travada e a referência já buscada e decide processar, rejeitar ou esperar. A ordem das checagens: dono da wallet e moeda; depois, para quem tem referência, a identidade da referência (mesmo provedor, player, wallet, moeda e rodada), o tipo permitido e o valor (nas reversões); só então o estado da referência (pendente faz esperar, `REJECTED`/`FAILED` rejeita) e a reversão única. Coloquei identidade, tipo e valor antes do estado porque esses dados da referência nunca mudam, então uma referência errada é rejeitada na hora, mesmo que ainda esteja pendente, em vez de ficar esperando à toa.

Interpretações adotadas onde o enunciado deixava espaço:

- **I1. Uma reversão por referência, somando `REFUND` e `ROLLBACK`.** Pela letra da regra 4, uma `BET` poderia receber um `REFUND` e depois um `ROLLBACK`, e os dois creditariam o mesmo valor, o que viola "o sistema não pode duplicar créditos". A segunda volta `REFERENCE_ALREADY_REVERSED`, e no banco é o índice único parcial. O `ROLLBACK` de um `REFUND` continua permitido, porque a referência dele é o `REFUND`.
- **I2. Referência que existe mas não está `PROCESSED`.** Pendente: a dependente também espera. `REJECTED` ou `FAILED`: `REFERENCE_NOT_PROCESSED`, porque uma transação que nunca foi aplicada não tem efeito para reverter.
- **I3. Referência por tipo.** Obrigatória em `REFUND`/`ROLLBACK`, opcional em `WIN`/`LOSS`, proibida em `BET` (400, para o campo não ser ignorado em silêncio). `WIN`/`LOSS` com referência só apontam para `BET`, com qualquer valor; vários `WIN` podem apontar para a mesma `BET`, e um `WIN` não conta como reversão.
- **I4. `gameId` não é comparado** na validação da referência; a regra 2 lista provider, player, wallet, moeda e rodada.
- **I5. Não existe estado de rodada.** São permitidos várias `BET` na mesma rodada, `WIN`/`LOSS` sem `BET` antes, `WIN` depois de uma `BET` revertida, `ROLLBACK` de uma `BET` que já teve `WIN`. As invariantes financeiras continuam protegidas pelas regras de referência, de reversão única e de saldo não negativo.
- **I6. Valor zero.** `BET`, `WIN`, `REFUND` e `ROLLBACK` exigem valor maior que zero (um movimento de zero geraria um lançamento sem mudança de saldo, o que contradiz a regra do `version`); `LOSS` aceita zero; saldo inicial zero é aceito, sem `OPENING`.
- **I7. Dono da wallet e identidade do provedor.** Em toda operação, o `playerId` precisa ser o dono da `walletId` (`PLAYER_WALLET_MISMATCH`). Não há cadastro de provedores; as validações são o formato do `providerId`, o valor reservado `internal` proibido, e a referência buscada sempre dentro do mesmo `providerId`. Valem também para mensagens da fila.
- **I8. Transação `OPENING`.** Criar wallet com saldo maior que zero gera, na mesma transação SQL, uma transação interna `OPENING` com `providerId`, `externalTransactionId`, `roundId` e `gameId` nulos (nenhum valor inventado numa tabela de auditoria), `idempotencyKey = "opening:{walletId}"`, o lançamento `CREDIT` com `wallet_version = 1` e os eventos `WagerTransactionProcessed` e `WalletBalanceChanged`.

---

## 9. Referências fora de ordem

Uma transação cuja referência ainda não existe é gravada em `PENDING_REFERENCE` (HTTP responde 202) e o **worker** a reprocessa ([process-wager-transaction.ts](src/application/process-wager-transaction.ts), `processNextDuePending`). O worker roda a cada 1 s em todas as instâncias e pega uma pendente por transação SQL com `FOR UPDATE SKIP LOCKED`, então duas instâncias nunca pegam a mesma. A ordem de locks é linha da pendente e depois wallet, o que não forma ciclo com o HTTP, que nunca trava uma linha existente de `wager_transactions`. Só o worker reprocessa: a chegada da referência não acorda as pendentes.

O intervalo entre tentativas usa o backoff comum aos três pontos que reagendam trabalho ([backoff.ts](src/domain/backoff.ts)): exponencial com **full jitter**, `espera = aleatório(0, min(teto, base × 2^tentativa))`, base 1 s e teto 60 s no worker. O aleatório entra por porta, então nos testes a espera é determinística.

**TTL de 10 minutos**, contado do `created_at` da pendente. Um prazo em tempo responde à pergunta "por quanto tempo a referência ainda pode estar a caminho?": uma mensagem da referência com falha transitória pode ficar até `maxReceiveCount` × espera máxima na fila antes da DLQ, com a configuração da fila 5 recebimentos com até 60 s cada, cerca de 5 minutos. O TTL precisa ser maior que isso, e 10 minutos é o dobro, com folga para os retries do próprio provedor no HTTP. Esgotado o prazo, a transação vira `REJECTED` com `REFERENCE_NOT_FOUND` e sai o evento `WagerTransactionRejected`.

Erro transitório no worker desfaz a transação SQL e a pendente volta na próxima varredura. Erro permanente (violação de constraint, invariante quebrada, qualquer erro fora da lista de transitórios) faz uma transação SQL separada marcar `FAILED` com `PROCESSING_FAILED` e emitir `WagerTransactionFailed`, para o provedor que recebeu 202 saber que ela terminou.

---

## 10. Códigos de falha

Códigos em `UPPER_SNAKE_CASE`, estáveis (código publicado não muda de significado; código novo é acrescentado). Cada um diz ao provedor uma de três ações: reenviar (a mesma requisição, a mesma key), corrigir (se a rejeição foi gravada, a correção precisa de um novo `externalTransactionId` e uma nova key) ou desistir.

| Código | Onde | Quando | Ação |
|---|---|---|---|
| `INSUFFICIENT_FUNDS` | transação `REJECTED`, 422 | `BET` maior que o saldo | desistir |
| `REVERSAL_INSUFFICIENT_FUNDS` | `REJECTED`, 422 | `ROLLBACK` de `WIN`/`REFUND` deixaria o saldo negativo | desistir e escalar: o dinheiro já saiu da wallet |
| `CURRENCY_MISMATCH` | `REJECTED`, 422 | moeda diferente da wallet | corrigir |
| `PLAYER_WALLET_MISMATCH` | `REJECTED`, 422 | `playerId` não é o dono da wallet | corrigir |
| `REFERENCE_MISMATCH` | `REJECTED`, 422 | referência de outro player, wallet, moeda ou rodada | corrigir |
| `REFERENCE_KIND_NOT_ALLOWED` | `REJECTED`, 422 | tipo de referência que a regra 3 não permite | corrigir |
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

---

## 11. Idempotência e payloadHash

O header `Idempotency-Key` é obrigatório e é a fonte da verdade (1 a 255 caracteres ASCII visíveis; o formato `"{providerId}:{externalTransactionId}"` é o recomendado ao provedor, sem validação). No banco, `UNIQUE (idempotency_key)` global e, separado, `UNIQUE (provider_id, external_transaction_id)`. A key nunca expira, porque o registro é a própria transação financeira. Nada disso fica em memória: o teste "a idempotência é persistente" reinicia a aplicação e o replay continua batendo.

O fluxo ([process-wager-transaction.ts](src/application/process-wager-transaction.ts)) segue a seção 8 do desafio: consulta rápida pela key, sem lock (replay não entra na fila do lock); abre a transação; na fila, `INSERT` na inbox; `SELECT ... FOR UPDATE` da wallet; consulta a key de novo, agora serializado pela wallet, o que fecha a corrida das cópias simultâneas; só então confere se o `(providerId, externalTransactionId)` já foi usado com outra key; decide e grava tudo; commit. Se o commit falhar com violação de unicidade na key ou no par (duas cópias com a mesma key e `walletId` diferentes travam wallets diferentes), a transação é desfeita e o caso de uso consulta de novo: mesmo hash é replay, hash diferente é conflito.

**payloadHash** ([payload-hash.ts](src/application/payload-hash.ts)) é o SHA-256 (hexadecimal minúsculo, de `node:crypto`) dos bytes UTF-8 do JSON canônico pela **RFC 8785 (JCS)** de um objeto montado a partir do comando **já validado e normalizado**, nunca do corpo bruto. A canonicalização usa o pacote `canonicalize`, que rodou no Bun sem ajuste. Os campos: `providerId`, `externalTransactionId`, `playerId` e `walletId` (UUID em minúsculas), `roundId`, `gameId`, `kind`, `money` (`{ "amount": "25.00", "currency": "BRL" }`, como `Money.toJSON()` serializa) e `referenceExternalTransactionId` só quando presente (ausente e `null` são a mesma coisa). Não entram o header, o `data.idempotencyKey` da mensagem, `messageId`, `type`, `occurredAt` nem metadado de transporte, então a mesma operação chegando por HTTP e depois pela fila é replay. Os vetores fixos em [payload-hash.test.ts](test/unit/payload-hash.test.ts) tiveram o hash esperado calculado fora do projeto, com `sha256sum`, sobre o JSON canônico escrito à mão. Consequência: mudar a lista de campos quebraria os replays antigos; se um dia for preciso, o hash ganha um prefixo de versão.

Repetir uma operação já decidida devolve o resultado original, inclusive o saldo observado na época (coluna `balance_after`) e inclusive rejeições (422 de novo, com `idempotentReplay: true`). Repetição enquanto a original está pendente devolve o estado atual (202 enquanto pendente, o resultado final depois). Erros 400, 409, 500 e 503 não são gravados, então reenviar depois de um 503 tenta de novo de verdade.

---

## 12. API HTTP e status

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

A regra dos corpos: se existe uma `WagerTransaction`, o corpo é a transação; se não existe, é um erro em Problem Details (RFC 9457, `application/problem+json`, com `type`, `title`, `status`, `detail`), mais a extensão `code` e, no 400, `errors` com os campos. JSON malformado também sai nesse formato: o parser do Express é registrado à mão ([http-app.ts](src/infrastructure/http/http-app.ts)) para isso.

**Divergência consciente do draft da IETF do header `Idempotency-Key`.** O draft recomenda 422 para key reutilizada com outro payload; aqui é 409. O enunciado chama esse caso de conflito, as cinco situações precisam de cinco códigos diferentes, e o 422 ficou para a rejeição de negócio, que é o caso que mais se encaixa na definição de 422 da RFC 9110. O draft é rascunho, não RFC.

Duas escolhas que a especificação não cobria: id malformado no caminho (`/wallets/abc`) responde 400 `VALIDATION_ERROR`, em vez de deixar o PostgreSQL recusar o UUID e virar 500; e o replay de uma transação `FAILED` (uma pendente que falhou no worker) responde **500 com o corpo da transação**, porque não é regra de negócio nem algo que o provedor corrija, e pela regra dos corpos, se a transação existe, o corpo é ela.

O header `X-Correlation-Id` é aceito na requisição (até 128 caracteres seguros; se não vier, nasce um UUIDv7) e devolvido na resposta.

**Reconciliação** ([queries.ts](src/application/queries.ts), [ledger-repository.ts](src/infrastructure/persistence/ledger-repository.ts)): uma única consulta SQL lê o saldo gravado, a soma dos créditos, a soma dos débitos e a contagem de lançamentos, então a leitura usa um snapshot só e é coerente mesmo com transações sendo aplicadas, sem travar a wallet. Nada é gravado nem corrigido: divergência vira log de nível `error` (com `walletId` e `checkedEntries`, sem valores), incremento de `wagering_reconciliation_divergences_total` e `consistent: false` na resposta.

**Cursor do ledger:** paginação por chave pela coluna `wallet_version`, em ordem crescente (a de auditoria), com o cursor `{"after": <wallet_version>}` em base64url, opaco para o cliente. É estável porque o ledger só recebe lançamentos novos no fim, e a consulta é uma faixa no índice `UNIQUE (wallet_id, wallet_version)`.

---

## 13. Fila de entrada

Filas `wager-transactions.fifo` e `wager-transactions-dlq.fifo`, criadas por [create-queues.ts](src/infrastructure/messaging/create-queues.ts): FIFO, deduplicação por conteúdo desligada, visibilidade de 30 s, long polling de 20 s, `maxReceiveCount = 5` e retenção de 14 dias na DLQ.

**Contrato da fila:** `MessageGroupId = walletId` e `MessageDeduplicationId = messageId`, com o atributo de mensagem `correlationId` opcional. Quem define esses atributos é o produtor; o desafio não tem produtor, então esse é o contrato documentado, e os testes o seguem. O grupo por wallet dá ordem por wallet e paralelismo entre wallets, mas é otimização: a ordem não vale entre HTTP e fila, a janela de deduplicação é de 5 minutos e redelivery acontece. A correção vem do lock da wallet, da inbox e da `idempotency_key`.

O consumidor ([sqs-consumer.ts](src/infrastructure/messaging/sqs-consumer.ts)) usa o mesmo caso de uso do HTTP e grava a inbox `(consumer_name, message_id)` no mesmo commit do efeito; `DeleteMessage` só depois do commit. A classificação:

| Resultado | Ação |
|---|---|
| sucesso, replay, mensagem já na inbox com o mesmo hash, rejeição de negócio | `DeleteMessage` depois do commit |
| transitório ou inesperado (nada foi gravado) | `ChangeMessageVisibility` com o backoff comum (base 2 s, teto 60 s, a partir do `ApproximateReceiveCount`); depois do `maxReceiveCount` o próprio SQS move para a DLQ |
| permanente: JSON inválido, `type` desconhecido, `kind` `OPENING`, payload inválido, `WALLET_NOT_FOUND`, conflito de idempotência, mesmo `messageId` com outro hash | envio explícito para a DLQ (com o atributo `reason`) e depois `DeleteMessage` |

Mensagem inválida é permanente, não negócio, porque sem um comando válido não existe transação para marcar `REJECTED`; o envio explícito evita esperar 5 recebimentos e atrasar as seguintes da mesma wallet. Esse envio não é atômico com o delete: se o processo morrer entre os dois, a mensagem é reclassificada e reenviada, e a deduplicação da DLQ FIFO (mesmo `MessageDeduplicationId`) segura a cópia. Mensagens do mesmo grupo no mesmo lote são processadas em ordem; se uma falha de forma transitória, as seguintes do grupo voltam com visibilidade 0. Grupos diferentes vão em paralelo.

Usei dois clientes SQS: um com timeout de 5 s para as chamadas comuns e outro com timeout maior que os 20 s do long polling. Os dois têm `throwOnRequestTimeout` ligado, porque sem ele o SDK só escreve um aviso e a chamada continua pendurada, o que no publisher da outbox seguraria a transação aberta além do `idle_in_transaction_session_timeout`.

**SIGTERM** ([http-app.ts](src/infrastructure/http/http-app.ts), `ShutdownCoordinator`): a readiness passa a 503; o consumidor para de receber (o long polling em andamento é cancelado), devolve com visibilidade 0 as mensagens recebidas e não iniciadas e conclui as que estão em andamento, com limite de 20 s; o worker e o publisher terminam o item atual e param; o NestJS fecha o HTTP; o pool do banco fecha por último. No compose, `stop_grace_period: 30s` e `init: true`. Com `docker compose stop`, uma instância saiu do primeiro ao último passo em 123 ms. Se o limite estourar ou o processo morrer de vez, nada se perde nem duplica: a transação SQL sem commit é desfeita, a mensagem sem `DeleteMessage` volta depois da visibilidade e a idempotência segura a duplicata.

---

## 14. Outbox e eventos

Transação, saldo, lançamento, inbox (na fila) e eventos de integração são gravados na mesma transação SQL. O publisher ([outbox-publisher.ts](src/application/outbox-publisher.ts)) roda em todas as instâncias: abre uma transação, pega até 10 linhas vencidas com `FOR UPDATE SKIP LOCKED`, publica com `SendMessageBatch`, marca as aceitas e reagenda as recusadas (backoff comum, base 1 s, teto 5 min, `last_error` gravado), e faz commit. Varre a cada 500 ms ocioso e volta na hora quando o lote veio cheio. **Sem limite de tentativas**, porque descartar um evento quebraria "não perder eventos confirmados"; depois de 10 tentativas, cada falha vira log de erro e conta em `wagering_outbox_stuck`.

O destino é `wagering-events.fifo`, com `MessageGroupId = aggregateId` (`walletId` em todos os eventos) e `MessageDeduplicationId = eventId`. A única duplicação possível é publicar e morrer antes do commit que marca `published_at`; a cópia tem o mesmo `eventId`, então o SQS descarta dentro de 5 minutos (otimização) e o consumidor de eventos deduplica por `eventId` (a garantia). O teste "publicou e morreu antes do commit" em [outbox-publishers.test.ts](test/integration/outbox-publishers.test.ts) reproduz exatamente isso.

**Contrato dos eventos:** envelope com `eventId`, `eventType`, `aggregateId`, `correlationId`, `causationId`, `occurredAt` e `version`, mais `data` com `MoneyProps`. O envelope é uma classe abstrata ([events.ts](src/domain/events.ts)) com uma subclasse por evento: `WagerTransactionProcessed`, `WagerTransactionRejected`, `WagerTransactionPendingReference` (sai uma vez, quando entra em espera), `WagerTransactionFailed` e `WalletBalanceChanged` (só quando o saldo muda; todo lançamento tem um). Não há ordem garantida de publicação entre eventos da mesma wallet com vários publishers; o `WalletBalanceChanged` carrega `walletVersion`, que cresce de 1 em 1, e o consumidor que precisa de ordem usa esse número. O consumidor deduplica por `eventId` e tolera ordem trocada.

`correlationId` e `causationId`: no HTTP, o `X-Correlation-Id` (ou um novo) e a `Idempotency-Key`; na fila, o atributo `correlationId` (ou o `messageId`) e o `messageId`; no worker, o `correlation_id` gravado na pendente e o id dela. O `correlationId` é gravado na transação para amarrar o que o worker faz minutos depois à operação original.

---

## 15. Observabilidade

**Logs** em JSON pelo pino ([logger.ts](src/observability/logger.ts)), com um `mixin` lendo um `AsyncLocalStorage` ([log-context.ts](src/observability/log-context.ts)) que guarda `correlationId`, `messageId`, `transactionId`, `walletId` e `providerId` do trabalho em andamento, e `redact` como segunda barreira. Nunca vão para log: `playerId`, qualquer valor monetário, corpo de requisição ou de mensagem e o header `Authorization`. O teste de várias instâncias lê a saída dos 3 processos e confere que nenhum `playerId` usado e nenhum campo `"amount"` aparecem, e que todo log de decisão tem os ids. Consequência: diagnosticar um problema de valor exige consultar o banco ou chamar a reconciliação.

**Métricas** pelo prom-client em `GET /metrics` ([metrics.ts](src/observability/metrics.ts)), sem label de alta cardinalidade:

| Exigência | Métrica |
|---|---|
| transações por status | `wagering_transactions_total{kind, status, channel}` |
| duplicatas | `wagering_duplicates_total{source}` e `wagering_idempotency_conflicts_total{type}` |
| retries | `wagering_retries_total{component}` |
| DLQ | `wagering_dlq_messages` (lido da fila a cada 30 s) e `wagering_dlq_forwarded_total{reason}` |
| conflitos de lock | `wagering_wallet_lock_wait_seconds` (histograma) e `wagering_concurrency_conflicts_total{type}` |
| outbox lag | `wagering_outbox_lag_seconds`, `wagering_outbox_pending`, `wagering_outbox_publish_delay_seconds` |
| latência | `wagering_processing_duration_seconds{channel, outcome}` |
| reconciliação | `wagering_reconciliation_divergences_total` |
| complementares | `wagering_pending_reference`, `wagering_outbox_stuck`, `inbox_payload_conflicts_total` |

Com lock pessimista, conflito de lock é espera, não exceção, por isso a métrica principal é o tempo de espera (medido em volta do `SELECT ... FOR UPDATE`, então inclui também o tempo da própria consulta).

**Health:** `GET /health/live` responde 200 sem olhar dependências (se olhasse o banco, uma queda dele reiniciaria todas as instâncias à toa); `GET /health/ready` chama a `checkReadiness()` montada na [composição](src/composition.ts), que faz um `select 1 from <as 5 tabelas> limit 0` (conecta e confere que as tabelas existem, sem ler nenhuma linha) e um `GetQueueAttributes` na fila de entrada, e responde 503 se algum falhar ou se a instância estiver desligando. O banco aparece como `unavailable` quando não responde e como `schema_missing` quando responde mas as tabelas não existem: um `SELECT 1` sozinho deixaria uma instância sem schema aparecer como pronta e responder 500 em toda rota. Os dois e o `/metrics` são públicos.

---

## 16. Autenticação

**Decisão: não integrei um Identity Provider.** A autenticação não vale pontos na avaliação e não podia competir com correção financeira, concorrência e idempotência. O que existe é o **ponto de extensão explícito**: a porta `ProviderIdentityPort` e um `AuthGuard` global que a chama em toda rota não marcada como pública ([auth.ts](src/infrastructure/http/auth.ts)); health e `/metrics` são marcados com `@PublicRoute()`. Sem IdP, o adaptador é um no-op (`NoopProviderIdentity`) que não restringe nada, e ele é ligado em um lugar só ([http-app.ts](src/infrastructure/http/http-app.ts), provider `APP_GUARD`).

**O desenho que seria adotado:** um IdP externo, nunca autenticação artesanal (nada de tabela de usuários com hash de senha), o Keycloak com o realm importado na subida do compose. Cada provedor seria um cliente confidencial obtendo token pelo client credentials grant (RFC 6749, seção 4.4), o fluxo do OAuth 2.0 para chamada entre máquinas. A API validaria o JWT localmente com a biblioteca `jose`, buscando as chaves públicas no JWKS do Keycloak e conferindo emissor, audiência e expiração; o provedor do token seria comparado com o `providerId` do corpo (e do caminho, na consulta por provedor), com 403 `PROVIDER_MISMATCH` quando diferente e 401 sem token ou com token inválido. Mensagens da fila não têm token: são canal interno confiável, mas a identidade do provedor contida nelas passa pelas mesmas validações de domínio (I7).

---

## 17. Testes

| Pasta | O que roda | Infraestrutura |
|---|---|---|
| `test/unit` | `Money` (inclusive arredondamento), `Wallet`, `WalletLedgerEntry`, `WagerTransaction`, regras de BET/WIN/LOSS/REFUND/ROLLBACK, conflito de moeda, `payloadHash` com vetores fixos, backoff com aleatório injetado, classificação de erros, cursor, eventos, inbox e outbox | nenhuma |
| `test/integration` | migrations e constraints; Unit of Work, Identity Map e optimistic lock do MikroORM; API HTTP (idempotência persistente, replay, conflitos, status); atomicidade entre wallet, ledger, inbox e outbox; evento só depois do commit; inbox e redelivery; retry e DLQ (com erro injetado e com `lock_timeout` real); publishers concorrentes; recuperação após reinicialização; PostgreSQL e SQS fora do ar (um proxy TCP na frente dos serviços reais, cortado e religado pelo teste) | PostgreSQL e MiniStack do compose |
| `test/concurrency` | os 8 cenários da seção 13 e a hot wallet, com 3 processos da aplicação via `Bun.spawn` | idem, mais os processos |

**Nenhum mock de banco ou de fila.** As falhas entram pela composição das portas: uma outbox que lança erro entre o lançamento e a outbox (atomicidade), uma outbox que segura a transação antes do commit (evento só depois do commit), um repositório que lança `55P03` (transitório), e uma entrada só de teste, [crash-after-commit-main.ts](test/support/crash-after-commit-main.ts), que sobe a aplicação normal com uma fila cujo `DeleteMessage` vira um `SIGKILL` no próprio processo (worker morto depois do commit e antes do ack).

**Invariante final.** Todo teste de integração e de concorrência termina chamando um verificador ([invariants.ts](test/support/invariants.ts)) que, para cada wallet tocada, lê o banco com SQL direto num snapshot só e confere `balance = Σ créditos − Σ débitos`, que o `balanceBefore` de cada lançamento é o `balanceAfter` do anterior, que os `wallet_version` não têm buracos, que cada lançamento tem um `WalletBalanceChanged` na outbox e que cada transação `PROCESSED` que move saldo tem exatamente um lançamento.

**Determinismo:** relógio e aleatório entram por porta; as esperas são por condição com prazo (`waitUntil`), não por `sleep` fixo; TTL, visibilidade e backoff ficam curtos por configuração nos testes; sorteios com semente 42. **Isolamento:** um banco novo por execução, criado e migrado no início ([fresh-database.ts](test/support/fresh-database.ts)), porque os triggers do ledger impedem limpar tabelas; filas com nome único por arquivo; ids únicos por teste.

Nos testes de concorrência, o cliente HTTP faz o papel do provedor: quando recebe 503 (lock_timeout numa wallet disputada) ou quando a conexão cai antes da resposta, reenvia a mesma operação com a mesma key e conta quantas vezes fez isso. As asserções de correção (um débito só, saldo esperado, invariantes) continuam as mesmas.

---

## 18. Limitações e o que ficou de fora

- **O dono do schema consegue remover os triggers.** A imutabilidade do ledger e das transações terminais vale para qualquer papel enquanto os triggers existirem; não usei `REVOKE` nem dois papéis no banco.
- **A outbox não tem rotina de retenção.** As linhas publicadas ficam para auditoria e a tabela cresce sem limpeza.
- **Não existe estado de rodada** (I5): `WIN` depois de uma `BET` revertida, por exemplo, é aceito.
- **Reversão rejeitada por TTL não é refeita sozinha.** Se a referência chegar depois dos 10 minutos, ela é processada normalmente e a reversão já está rejeitada; num `ROLLBACK` de `BET`, a aposta fica debitada até o provedor mandar uma nova reversão com outro `externalTransactionId`.
- **Uma falha transitória na fila atrasa as mensagens seguintes da mesma wallet** por até alguns minutos (só na fila; o HTTP continua livre).
- **Disputa forte ou instância morta podem levar mensagens da fila à DLQ.** Uma mensagem que falha de forma transitória (por exemplo, `lock_timeout`) em todos os `maxReceiveCount` recebimentos é movida para a DLQ pelo SQS, sem nada gravado, e fica auditável lá; uma instância morta com mensagens em mãos também conta um recebimento para cada uma delas. Vi isso duas vezes com o `maxReceiveCount = 3` que os testes usavam: na hot wallet (2 de 273 operações, confirmadas na DLQ) e numa execução do cenário 8, com SIGKILL no meio da carga (1 de 150 operações). Neste segundo caso a DLQ não foi lida naquela execução e não consegui reproduzir em 14 execuções seguidas, então a causa é deduzida pelo código (a fila de entrada estava vazia, e a única saída sem transação para uma mensagem válida é o redrive), não observada. Os dois testes passaram a usar o valor de produção (5) e a contabilizar cada operação como transação ou como mensagem na DLQ movida pelo SQS, imprimindo a contagem e o histórico de logs de cada mensagem que for para lá.
- **Logs sem valores:** diagnosticar um problema de valor exige o banco ou a reconciliação.
- **Desligamento gracioso só em Linux/macOS.** No Windows o `SIGTERM` não chega ao processo (E1, item 9); a aplicação é feita para rodar em container.
- **Um `ECONNRESET` esporádico no cliente de teste**, numa das execuções dos testes de concorrência no Windows, com a instância continuando viva. Tentei reproduzir isolado (rajadas de 100 requisições com ociosidade maior que o keep-alive do servidor) e não consegui, então a causa não está diagnosticada; o cliente de teste reenvia com a mesma key, como o provedor faria.
- **A imagem da aplicação inclui os testes e as dependências de desenvolvimento**, para o serviço `tests` do compose usar a mesma imagem. Numa imagem de produção eu separaria em dois estágios.
- **As métricas padrão de processo do prom-client** não foram conferidas no Bun; as métricas do desafio são todas definidas no código e não dependem delas.
- **Ficaram de fora, por serem opcionais:** integração com um Identity Provider (há o desenho e o ponto de extensão, seção 16), ledger de partidas dobradas, OpenTelemetry e dashboard, e teste de carga.

---

## 19. Desenvolvido com Claude Code

Este projeto foi desenvolvido com o Claude Code, o assistente de programação da Anthropic. Antes da implementação, eu preparei a especificação adaptada do desafio e as decisões de arquitetura (com os experimentos E1 e E2 definidos como pré-condição de cada escolha); a partir delas, o Claude Code rodou os experimentos, escreveu o código, os testes e esta documentação, registrando cada experimento e cada decisão tomada durante a implementação. As escolhas que a especificação não cobria estão sinalizadas aqui como tal (a coluna `balance_currency`, o 500 no replay de `FAILED`, o 400 para id malformado no caminho, os dois clientes SQS e a ordem das checagens de referência), para eu revisar e poder justificar cada uma na apresentação.
