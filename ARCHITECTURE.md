# Arquitetura

Este documento resume as decisões do projeto: o que eu decidi, por quê e o que cada decisão custa. Cada seção termina apontando para o documento de [`docs/`](docs/) que tem o detalhe daquele assunto, com o raciocínio completo, os testes que o provam e as medições. O [README](README.md) diz como subir, usar e testar.

**As 8 decisões principais**, uma linha cada, para quem tem pouco tempo:

| Assunto | Decisão | Onde |
|---|---|---|
| Dinheiro | `Money` com centavos em `bigint` e escala fixa de 2; `NUMERIC(18,2)` e moeda em colunas separadas no banco | [Dinheiro](#dinheiro) |
| Concorrência | `SELECT ... FOR UPDATE` só na linha da wallet, sem retry no caso feliz; a `version` é o optimistic lock do MikroORM, como segunda defesa | [Concorrência](#concorrência) |
| Persistência | MikroORM com Unit of Work e Identity Map, `transactional()` e `LockMode`; uma transação SQL por execução de caso de uso | [Persistência e transação](#persistência-e-transação) |
| Garantias no banco | `UNIQUE`, `CHECK`s e triggers para unicidade, imutabilidade e saldo não negativo, e a conferência no commit de que saldo e ledger batem | [Garantias no banco](#garantias-no-banco) |
| Idempotência | `Idempotency-Key` com `UNIQUE` no banco e `payloadHash` SHA-256 do JSON canônico (RFC 8785) do comando validado | [API HTTP e idempotência](#api-http-e-idempotência) |
| Fila de entrada | o mesmo caso de uso do HTTP, inbox no mesmo commit, ack só depois dele; falha transitória tenta de novo no próprio consumidor por até 5 minutos, sem gastar recebimento, e o banco fora pausa o recebimento | [Fila de entrada](#fila-de-entrada) |
| Outbox | eventos no mesmo commit do efeito, publishers com `SKIP LOCKED` em todas as instâncias, e o consumidor deduplica pelo `eventId` | [Outbox e eventos](#outbox-e-eventos) |
| Referências fora de ordem | `PENDING_REFERENCE` reprocessada por um worker com backoff exponencial e full jitter, até um TTL de 10 minutos | [Regras de negócio](#regras-de-negócio) |

**Sumário:** [Visão geral](#visão-geral) · [Experimentos antes do código](#experimentos-antes-do-código) · [Dinheiro](#dinheiro) · [Persistência e transação](#persistência-e-transação) · [Concorrência](#concorrência) · [Garantias no banco](#garantias-no-banco) · [Regras de negócio](#regras-de-negócio) · [API HTTP e idempotência](#api-http-e-idempotência) · [Fila de entrada](#fila-de-entrada) · [Outbox e eventos](#outbox-e-eventos) · [Observabilidade](#observabilidade) · [Autenticação](#autenticação) · [Testes](#testes) · [Limitações e o que ficou de fora](#limitações-e-o-que-ficou-de-fora) · [Desenvolvido com Claude Code](#desenvolvido-com-claude-code)

Nos comentários do código, "seção N do desafio" é a seção do enunciado, e os códigos I1 a I8 são as interpretações listadas em [Regras de negócio](#regras-de-negócio).

---

## Visão geral

O serviço tem uma operação central, processar uma transação de aposta, que chega por três caminhos: `POST /wagering/transactions`, a fila `wager-transactions.fifo` e o worker que reprocessa transações à espera da referência. Os três chamam **o mesmo caso de uso** ([process-wager-transaction.ts](src/application/wagering/process-wager-transaction.ts)), então as regras e as garantias são as mesmas em qualquer canal.

O código segue a arquitetura em camadas do DDD: domínio, aplicação e infraestrutura. Dentro do domínio e da aplicação, os arquivos ficam agrupados nos três módulos do serviço, `wallet` (a wallet e os lançamentos), `wagering` (as transações de aposta e as regras da seção 7) e `messaging` (inbox e outbox), e o que é comum a eles (`Money`, erros, eventos, portas) fica na raiz da camada. São módulos de um bounded context só, e não contextos separados, porque saldo, lançamento e transação de aposta mudam juntos, na mesma transação SQL.

- `src/domain`: `Money`, `Wallet`, `WalletLedgerEntry`, `WagerTransaction`, eventos, `InboxMessage`, `OutboxMessage` e a decisão de liquidação ([wager-settlement.ts](src/domain/wagering/wager-settlement.ts)), que concentra as regras da seção 7 do desafio. Não importa nada de NestJS, MikroORM nem de outras camadas; uma regra de lint (`no-restricted-imports` em [eslint.config.js](eslint.config.js)) impede. Ids e datas entram por parâmetro, então o domínio é determinístico em teste.
- `src/application`: casos de uso, as portas que eles usam ([ports.ts](src/application/ports.ts)), a validação do comando (a mesma para HTTP e fila), o `payloadHash` e o publisher da outbox. Também não depende de NestJS, MikroORM, do SDK do SQS nem da infraestrutura, e a mesma regra de lint garante isso.
- `src/infrastructure`: as implementações das portas (repositórios MikroORM, `TransactionRunner`, SQS), o consumidor da fila, os laços de fundo e a borda HTTP em NestJS.
- [composition.ts](src/composition.ts): monta tudo com chamadas explícitas de construtor, e o NestJS só recebe o que sai daqui. O módulo ([http-app.ts](src/infrastructure/http/http-app.ts)) registra cada peça com `useValue`, e cada controller recebe só o que usa (o de wallets, `CreateWallet` e `WalletQueries`; o de transações, o `ProcessWagerTransaction` do HTTP e `WalletQueries`; o de health, só a `checkReadiness()`). As classes de `src/domain` e `src/application` não têm nenhum decorator do NestJS. Os testes que injetam falha trocam uma porta por aqui, sem "modo de teste" no código de produção.

A garantia final de cada invariante está no banco, porque com várias instâncias ele é o único ponto que todas compartilham: lock de linha da wallet, `UNIQUE` da `idempotency_key` e de `(provider_id, external_transaction_id)`, inbox e outbox na mesma transação SQL do efeito, CHECKs e triggers. A ordem e a deduplicação do SQS FIFO são tratadas como otimização.

## Experimentos antes do código

Antes de escrever o código, deixei duas escolhas condicionadas a experimento, com o critério definido antes de rodar. O **E1** confirmou que NestJS, MikroORM 7, pino e prom-client funcionam no Bun, inclusive o lock de linha, o `SKIP LOCKED` e 3 instâncias via `Bun.spawn`; o único item que falhou foi o `SIGTERM` no Windows, que não entrega o sinal ao processo, e por isso o desligamento é testado no container. O **E2** confirmou que o MiniStack se comporta como o SQS FIFO e sobe sem conta, ao contrário da imagem atual do LocalStack, que exige token desde março de 2026. Os roteiros estão em [experiments/](experiments/), e o resultado item por item, em [medições e histórico](docs/medicoes-e-historico.md#os-experimentos-antes-da-implementação).

## Dinheiro

O [`Money`](src/domain/money.ts) guarda o valor como **inteiro em centavos num `BigInt`**, mais a moeda, sem biblioteca de decimal. Escolhi assim porque nenhuma conta do sistema divide ou multiplica dinheiro (a escala é fixa em 2 e a reversão parcial está fora de escopo), então somar, subtrair, negar e comparar centavos é exato. O `BigInt` tem uma proteção a mais: o JavaScript lança `TypeError` quando alguém mistura `BigInt` com `number` numa conta, então um `number` que escape para dentro do `Money` quebra na hora em vez de perder precisão em silêncio. O `Money` é congelado (`Object.freeze`) e toda operação devolve outra instância.

Na entrada, o formato aceito é exatamente `^(0|[1-9][0-9]*)\.[0-9]{2}$` com até 16 dígitos inteiros. Nada é normalizado: `"25.5"` é rejeitado em vez de virar `"25.50"`, e `"10.005"` é rejeitado em vez de virar `"10.01"`, então cada valor tem uma grafia só, o que também resolve a questão do hash. A moeda segue `^[A-Z]{3}$` mais uma lista de moedas aceitas por configuração (`ACCEPTED_CURRENCIES`, padrão `BRL`). Não validei contra a ISO 4217 inteira porque ela tem moedas com outro número de casas (o iene não tem casas), e a escala fixa de 2 só serve para moedas de 2 casas. O modelo é multi-moeda: cada wallet tem a sua, e operação entre moedas diferentes é erro de domínio.

No banco, o valor é `NUMERIC(18,2)` e a moeda `CHAR(3)`, em colunas separadas. O MikroORM devolve `numeric` como string (`"25.00"`, item 5 do E1), que vai direto para `Money.from`. O custo de não usar biblioteca é que o parser e o formatador de centavos são código próprio, com testes próprios em [money.test.ts](test/unit/money.test.ts), inclusive o de "arredondamento", que prova que nada é arredondado em silêncio.

## Persistência e transação

**MikroORM 7**, usado pelo caminho que o enunciado marca como preferencial. Cada execução de caso de uso é uma transação SQL, aberta pela porta `TransactionRunner` com `transactional()` num fork do `EntityManager`; não é uma transação por requisição, porque o mesmo caso de uso roda a partir do HTTP, da fila e do worker. O **Unit of Work** faz um flush só, no commit (`FlushMode.COMMIT`), então a violação de unicidade e o conflito de versão aparecem ali e desfazem tudo. O **Identity Map** é o de cada transação, e alterar uma linha exige tê-la lido nela, para nenhuma linha ser gravada sem controle de concorrência. O **`LockMode`** é `PESSIMISTIC_WRITE` na wallet e `PESSIMISTIC_PARTIAL_WRITE` (`SKIP LOCKED`) nos workers.

As classes de domínio não são mapeadas no ORM, porque têm construtor privado, estado encapsulado e o value object `Money`; os repositórios convertem os registros de [records.ts](src/infrastructure/persistence/records.ts) com a factory `rehydrate`. O custo é o código de mapeamento; o ganho é um domínio testável sem banco. Ficou SQL à mão em três lugares, cada um porque o ORM não resolve do mesmo jeito: os `SET LOCAL` dos timeouts, o `INSERT ... ON CONFLICT DO NOTHING RETURNING` da inbox e a soma da reconciliação. As migrations são escritas à mão e reversíveis, com o gerador de schema do ORM desligado.

O detalhe de cada peça, e o teste que a prova no banco real, estão em [banco de dados](docs/banco-de-dados.md#mikroorm-e-a-estratégia-transacional).

## Concorrência

**Lock pessimista na linha da wallet** (`SELECT ... FOR UPDATE`), numa transação curta em `READ COMMITTED`, sem retry automático no caso feliz. No cenário obrigatório (saldo 100, duas apostas de 80), a segunda espera o lock, lê o saldo já em 20 e é rejeitada com `INSUFFICIENT_FUNDS`, sem retry nenhum. Considerei as outras opções: o lock otimista vira uma nova tentativa a cada conflito, e o desafio pede justamente conflito alto (50 cópias em paralelo, hot wallet); o `UPDATE ... WHERE balance >= x` resolve o débito isolado, mas a transação também precisa ler a referência e conferir reversões vendo o mesmo estado da wallet, o que o lock serializa de graça; `SERIALIZABLE` exigiria retry em toda falha de serialização.

O lock trava exatamente uma wallet, então wallets diferentes andam em paralelo e não existe lock global. A **`version`** começa em 1, sobe só quando o saldo muda e é o optimistic lock do próprio MikroORM: é a segunda defesa, que pegaria um lost update se um bug pulasse o lock. A terceira é o `UNIQUE (wallet_id, wallet_version)` no ledger.

Os **timeouts** são dois conjuntos, porque a pressa de cada canal é diferente: no HTTP, `lock_timeout` de 2 s, porque tem um provedor esperando e um 503 com `Retry-After` faz ele reenviar com a mesma key; na fila, no worker e no publisher, 10 s, porque ali ninguém espera resposta e cada desistência gastaria uma tentativa. O custo do lock pessimista aparece na **hot wallet**: a vazão de uma wallet é limitada pelo tempo de cada transação, e sob disputa forte parte das respostas HTTP passa dos 2 s e volta 503, sem nenhum erro de correção.

Detalhe, números e testes em [banco de dados](docs/banco-de-dados.md#concorrência).

## Garantias no banco

As garantias de unicidade, imutabilidade e não-negatividade que a restrição 9 pede estão no schema, e mais algumas, para qualquer cliente que fale com o banco:

- **saldo nunca negativo:** `CHECK` no saldo da wallet, nos saldos do ledger e no saldo observado de cada transação;
- **unicidade:** uma wallet por player e moeda, a `idempotency_key`, o par provedor e id externo, um lançamento por transação, uma versão por lançamento, uma reversão por referência e um `OPENING` por wallet;
- **imutabilidade:** triggers barram `UPDATE`, `DELETE` e `TRUNCATE` no ledger, qualquer mudança em transação terminal, o `DELETE` de wallets e transações, e o `DELETE` ou a "despublicação" de eventos da outbox;
- **coerência conferida no commit:** constraint triggers adiados conferem que o saldo e a `version` da wallet são os do último lançamento, que todo lançamento é de uma transação `PROCESSED` com o mesmo valor, a mesma moeda e a direção do tipo dela, e que toda referência processada é do mesmo provedor, player, rodada e moeda, do tipo permitido e com o mesmo valor nas reversões.

Ficaram só no código três coisas: a referência estar `PROCESSED` no momento da decisão, o saldo suficiente para o `ROLLBACK` de um crédito (que o `CHECK` da wallet já protege de outro jeito) e a contagem "transação `PROCESSED` que move saldo tem exatamente um lançamento", que o verificador dos testes confere. A lista completa, constraint por constraint, está em [banco de dados](docs/banco-de-dados.md#schema-constraints-e-índices).

## Regras de negócio

As regras da seção 7 do desafio moram num lugar só, [wager-settlement.ts](src/domain/wagering/wager-settlement.ts), que recebe a wallet já travada e a referência já buscada e decide processar, rejeitar ou esperar. A identidade, o tipo e o valor da referência são conferidos antes do estado dela, porque esses dados nunca mudam, então uma referência errada é rejeitada na hora em vez de esperar à toa. `PENDING` existe só em memória, e `PROCESSED`, `REJECTED` e `FAILED` são terminais no código e no banco.

Onde o enunciado deixava espaço, adotei estas interpretações:

- **I1.** Uma reversão por referência, somando `REFUND` e `ROLLBACK`, porque um `REFUND` seguido de um `ROLLBACK` da mesma `BET` creditaria o valor duas vezes.
- **I2.** Referência pendente faz a dependente esperar; referência `REJECTED` ou `FAILED` rejeita, porque não tem efeito para reverter.
- **I3.** Referência obrigatória em `REFUND`/`ROLLBACK`, opcional em `WIN`/`LOSS` e proibida em `BET`; a transação que referencia a si mesma é rejeitada na hora.
- **I4.** O `gameId` não é comparado na referência, porque a regra 2 não o lista.
- **I5.** Não existe estado de rodada; as invariantes financeiras continuam protegidas pelas regras de referência e de saldo.
- **I6.** Valor zero só em `LOSS` e no saldo inicial.
- **I7.** O `playerId` precisa ser o dono da wallet, também pela fila.
- **I8.** A transação interna `OPENING` nasce com a wallet, na mesma transação SQL, sem inventar valores de provedor.

**Referências fora de ordem.** A transação cuja referência ainda não chegou é gravada em `PENDING_REFERENCE` (202 no HTTP) e reprocessada por um worker em todas as instâncias, com `SKIP LOCKED`, backoff exponencial com full jitter e teto de 10 s, então ela é decidida em até uns 11 s depois de a referência chegar. O TTL é de 10 minutos, o dobro do tempo que uma referência pode passar tentando de novo na fila antes de ir para a DLQ; esgotado, ela vira `REJECTED` com `REFERENCE_NOT_FOUND` e o evento sai.

**Códigos de falha** estáveis, em `UPPER_SNAKE_CASE`, cada um dizendo ao provedor se deve reenviar, corrigir ou desistir. `INSUFFICIENT_FUNDS` (o jogador sem saldo para apostar, rotina) e `REVERSAL_INSUFFICIENT_FUNDS` (o provedor querendo desfazer um crédito já gasto, problema operacional) são separados de propósito.

O texto completo de cada interpretação, a tabela de transições, o worker e a tabela de códigos com a ação de cada um estão em [regras de negócio](docs/regras-de-negocio.md).

## API HTTP e idempotência

| Situação | Status |
|---|---|
| processada (inclusive `LOSS`) e o replay dela | 200, com a transação |
| aceite pendente (`PENDING_REFERENCE`) | 202, com a transação |
| rejeição de negócio | 422, com a transação e o `failureCode` |
| payload inválido ou header ausente | 400 |
| conflito de idempotência | 409 |
| falha transitória | 503 com `Retry-After` |
| erro inesperado | 500 |

Se existe uma transação, o corpo é ela; se não existe, é um erro em Problem Details (RFC 9457) com a extensão `code`, e no 400 a lista dos campos inválidos. A key reutilizada com outro payload é 409, e não o 422 que o draft da IETF recomenda, porque o enunciado chama esse caso de conflito e o 422 ficou para a rejeição de negócio.

**Idempotência.** O header `Idempotency-Key` é obrigatório e é a fonte da verdade, com `UNIQUE` no banco e, separado, `UNIQUE (provider_id, external_transaction_id)`; nada fica em memória. A key é consultada antes do lock (replay não entra na fila do lock) e de novo depois dele, o que fecha a corrida das cópias simultâneas. O replay devolve o resultado original, inclusive o saldo observado na época e inclusive rejeições. O **`payloadHash`** é o SHA-256 do JSON canônico pela RFC 8785 do comando já validado e normalizado, com uma lista fixa de campos de negócio e nada de transporte, então a mesma operação chegando por HTTP e depois pela fila é replay.

Os campos do hash, os vetores fixos, a reconciliação e o cursor do ledger estão em [API HTTP](docs/api-http.md).

## Fila de entrada

O consumidor usa o mesmo caso de uso do HTTP, grava a inbox `(consumer_name, message_id)` no mesmo commit do efeito e só faz `DeleteMessage` depois do commit. O contrato da fila é `MessageGroupId = walletId` e `MessageDeduplicationId = messageId`, e a ordem e a deduplicação do FIFO são otimização: a correção vem do lock, da inbox e da key. Sucesso, replay e rejeição de negócio são confirmados; erro permanente (mensagem inválida, wallet inexistente, conflito de idempotência) vai direto para a DLQ com o motivo; falha transitória tenta de novo no próprio consumidor.

**O limite de tentativas é um orçamento de 5 minutos, e não a contagem de recebimentos do SQS.** O SQS conta um recebimento a cada entrega, inclusive quando a mensagem só volta para a fila sem culpa dela, e quando essa contagem era o orçamento, uma queda do banco de 64 s mandou operações válidas para a DLQ. Com o prazo, a mensagem fica com o consumidor, com o prazo de visibilidade estendido, tentando de novo com backoff, e uma queda menor que 5 minutos não manda nada para a DLQ, desde que a instância não morra no meio. Com o banco fora, o consumidor também para de buscar mensagens novas até ele voltar. O `maxReceiveCount = 10` continua como rede de segurança para os casos em que a mensagem sai das mãos do consumidor (desligamento, processo morto).

O custo é a complexidade: o consumidor controla o prazo de visibilidade de cada grupo, um limite de mensagens em mãos por instância e um teto de 6 minutos por mensagem, para uma chamada que nunca termina não prender a mensagem. No `SIGTERM`, a readiness passa a 503, as mensagens não iniciadas e as que esperavam nova tentativa voltam para a fila com visibilidade 0, e as em andamento terminam.

A vida de uma mensagem desenhada, cada um desses mecanismos com o porquê e os testes que conferem o número de recebimentos estão em [fila e outbox](docs/fila-e-outbox.md).

## Outbox e eventos

Transação, saldo, lançamento, inbox e eventos de integração são gravados na mesma transação SQL. O publisher roda em todas as instâncias, pega até 10 eventos vencidos com `FOR UPDATE SKIP LOCKED`, publica em `wagering-events.fifo` e marca as aceitas, **sem limite de tentativas**, porque descartar um evento quebraria "não perder eventos confirmados". A única duplicação possível é publicar e morrer antes do commit; por isso o contrato com quem consome é deduplicar pelo `eventId` e usar o `walletVersion` quando precisar de ordem, e um teste prova esse contrato com um consumidor de exemplo recebendo cada evento duas vezes de verdade.

Os eventos seguem o envelope da seção 11 do desafio: uma classe abstrata com uma subclasse por evento (`WagerTransactionProcessed`, `WagerTransactionRejected`, `WagerTransactionPendingReference`, `WagerTransactionFailed` e `WalletBalanceChanged`), com `MoneyProps` no `data`. Detalhe em [fila e outbox](docs/fila-e-outbox.md#outbox).

## Observabilidade

**Logs** em JSON pelo pino ([logger.ts](src/observability/logger.ts)), com um `mixin` lendo um `AsyncLocalStorage` ([log-context.ts](src/observability/log-context.ts)) que guarda `correlationId`, `messageId`, `transactionId`, `walletId` e `providerId` do trabalho em andamento, e `redact` como segunda barreira. Nunca vão para log: `playerId`, qualquer valor monetário, corpo de requisição ou de mensagem e o header `Authorization`. O teste de várias instâncias lê a saída dos 3 processos e confere que nenhum `playerId` usado e nenhum campo `"amount"` aparecem, e que todo log de decisão tem os ids. Consequência: diagnosticar um problema de valor exige consultar o banco ou chamar a reconciliação.

Os laços de fundo (publisher da outbox, worker de pendentes e os medidores) não enchem o log numa queda: a primeira falha de uma sequência sai em `background_loop_iteration_failed` (nível `error`) e a volta em `background_loop_recovered`, com quantas iterações falharam e por quanto tempo; entre as duas, a espera dobra a partir do intervalo de cada laço até o teto de 10 s (`BACKGROUND_LOOP_FAILURE_CAP_MS`) e volta ao normal no primeiro sucesso ([o volume de log que isso evita](docs/medicoes-e-historico.md#os-laços-de-fundo-numa-queda)). O custo é que, depois que o banco volta, um laço pode levar até esses 10 s para perceber.

**Métricas** pelo prom-client em `GET /metrics` ([metrics.ts](src/observability/metrics.ts)), sem label de alta cardinalidade:

| Exigência | Métrica |
|---|---|
| transações por status | `wagering_transactions_total{kind, status, channel}` |
| duplicatas | `wagering_duplicates_total{source}` e `wagering_idempotency_conflicts_total{type}` |
| retries | `wagering_retries_total{component}` (no consumidor, cada nova tentativa feita ali mesmo) |
| DLQ | `wagering_dlq_messages` (lido da fila a cada 30 s) e `wagering_dlq_forwarded_total{reason}` |
| conflitos de lock | `wagering_wallet_lock_wait_seconds` (histograma) e `wagering_concurrency_conflicts_total{type}` |
| outbox lag | `wagering_outbox_lag_seconds`, `wagering_outbox_pending`, `wagering_outbox_publish_delay_seconds` |
| latência | `wagering_processing_duration_seconds{channel, outcome}` |
| reconciliação | `wagering_reconciliation_divergences_total` |
| complementares | `wagering_pending_reference`, `wagering_outbox_stuck`, `wagering_inbox_payload_conflicts_total`, `wagering_consumer_pauses_total{reason}` |

Com lock pessimista, conflito de lock é espera, não exceção, por isso a métrica principal é o tempo de espera (medido em volta do `SELECT ... FOR UPDATE`, então inclui também o tempo da própria consulta).

**Health:** `GET /health/live` responde 200 sem olhar dependências (se olhasse o banco, uma queda dele reiniciaria todas as instâncias à toa); `GET /health/ready` chama a `checkReadiness()` montada na [composição](src/composition.ts), que faz um `select 1 from <as 5 tabelas> limit 0` (conecta e confere que as tabelas existem, sem ler nenhuma linha) e um `GetQueueAttributes` na fila de entrada, e responde 503 se algum falhar ou se a instância estiver desligando. O banco aparece como `unavailable` quando não responde e como `schema_missing` quando responde sem as tabelas, porque um `SELECT 1` sozinho deixaria uma instância sem schema aparecer como pronta. Os dois e o `/metrics` são públicos.

## Autenticação

**Decisão: não integrei um Identity Provider.** A autenticação não vale pontos na avaliação e não podia competir com correção financeira, concorrência e idempotência. O que existe é o **ponto de extensão explícito**: a porta `ProviderIdentityPort` e um `AuthGuard` global que a chama em toda rota não marcada como pública ([auth.ts](src/infrastructure/http/auth.ts)); health e `/metrics` são marcados com `@PublicRoute()`. Sem IdP, o adaptador é um no-op (`NoopProviderIdentity`) que não restringe nada, e ele é ligado em um lugar só ([http-app.ts](src/infrastructure/http/http-app.ts), provider `APP_GUARD`).

**O desenho que seria adotado:** um IdP externo, nunca autenticação artesanal (nada de tabela de usuários com hash de senha), o Keycloak com o realm importado na subida do compose. Cada provedor seria um cliente confidencial obtendo token pelo client credentials grant (RFC 6749, seção 4.4), o fluxo do OAuth 2.0 para chamada entre máquinas. A API validaria o JWT localmente com a biblioteca `jose`, buscando as chaves públicas no JWKS do Keycloak e conferindo emissor, audiência e expiração; o provedor do token seria comparado com o `providerId` do corpo (e do caminho, na consulta por provedor), com 403 `PROVIDER_MISMATCH` quando diferente e 401 sem token ou com token inválido. Mensagens da fila não têm token: são canal interno confiável, mas a identidade do provedor contida nelas passa pelas mesmas validações de domínio (I7).

## Testes

Os testes de integração e de concorrência usam o PostgreSQL e o MiniStack de verdade, sem mock de banco ou de fila: as falhas entram trocando uma porta na composição ou por um proxy TCP que o teste corta e religa. Os 8 cenários de concorrência da seção 13 do desafio rodam com 3 processos da aplicação via `Bun.spawn`, por HTTP e pela fila, e todo arquivo de teste que mexe em wallets termina num verificador que confere, direto no banco, que o saldo de cada wallet é o reconstruído pelo ledger, que a cadeia de lançamentos não tem buraco e que cada lançamento tem o seu evento. Relógio e aleatório entram por porta, e as esperas são por condição, não por `sleep`.

O que cada arquivo prova, com o item da seção 13 que ele cobre, e como rodar estão em [testes](docs/testes.md).

## Limitações e o que ficou de fora

- **O dono do schema consegue remover ou desligar os triggers.** A imutabilidade do ledger e das transações terminais, a proteção da wallet e da outbox, a conferência entre saldo e ledger e as regras da referência valem para qualquer papel enquanto os triggers estiverem ligados; não usei `REVOKE` nem dois papéis no banco. O teste de divergência da reconciliação usa justamente isso: desliga os dois triggers da wallet dentro da própria transação para conseguir estragar um saldo.
- **A outbox não tem rotina de retenção.** As linhas publicadas ficam para auditoria e a tabela cresce sem limpeza. O banco proíbe `DELETE` e `TRUNCATE` na outbox (migration 006), então uma limpeza futura precisa ser desenhada junto com essa proteção, em vez de um `DELETE` simples.
- **Não existe estado de rodada** (I5): `WIN` depois de uma `BET` revertida, por exemplo, é aceito.
- **Criar wallet não é idempotente.** Reenviar o mesmo `POST /wallets` depois de um timeout responde 409 `WALLET_ALREADY_EXISTS`, com o `walletId` da wallet que já existe no corpo, em vez de devolver a resposta original como um replay. O enunciado não pede idempotência na criação, e o `UNIQUE (player_id, currency)` garante que não nasce uma segunda wallet; quem reenvia usa o id que veio no 409.
- **A rejeição `PLAYER_WALLET_MISMATCH` grava e devolve o saldo observado da wallet, que é de outro player.** Como em toda rejeição, a transação guarda em `balance_after` o saldo da wallet na hora da decisão, e esse saldo sai na resposta 422, no replay, na consulta da transação e no evento `WagerTransactionRejected`; nesse caso, o `playerId` da operação não é o dono da wallet. Sem autenticação isso não muda nada, porque `GET /wallets/:walletId` é aberto e mostra o mesmo saldo. Com o IdP da seção [Autenticação](#autenticação), em que cada chamada carrega a identidade do provedor, isso passaria a mostrar a um provedor o saldo de um player que não é dele, e seria preciso gravar e devolver `balance` nulo nessa rejeição.
- **Reversão rejeitada por TTL não é refeita sozinha.** Se a referência chegar depois dos 10 minutos, ela é processada normalmente e a reversão já está rejeitada; num `ROLLBACK` de `BET`, a aposta fica debitada até o provedor mandar uma nova reversão com outro `externalTransactionId`.
- **Uma falha transitória na fila atrasa as mensagens seguintes da mesma wallet** por até o orçamento de 5 minutos da que falhou (só na fila; o HTTP continua livre).
- **Uma operação válida ainda pode chegar à DLQ em dois casos.** O primeiro é uma falha transitória que dura mais que o orçamento: uma queda do banco de mais de 5 minutos, ou a wallet travada por mais tempo que isso; o consumidor manda a mensagem para a DLQ com `RETRY_BUDGET_EXHAUSTED`, sem nada gravado. O segundo é a mensagem sair das mãos do consumidor 10 vezes (instâncias mortas ou desligadas com ela em mãos, ou o SQS sem aceitar a extensão do prazo), e aí quem move é o próprio SQS, sem `reason`. Nos dois casos a mensagem fica auditável na DLQ e aparece em `wagering_dlq_messages`.
- **O limite de mensagens em mãos é por instância.** Cada instância segura até 10 mensagens ao mesmo tempo (`CONSUMER_MAX_IN_FLIGHT_MESSAGES`). Uma wallet presa, esperando o lock ou tentando de novo dentro do orçamento, ocupa só as vagas das mensagens dela que já estavam em mãos, e as outras wallets continuam sendo buscadas e processadas; mas, se as 10 vagas de uma instância estiverem com wallets presas, ela para de buscar até alguma sair, por até 5 minutos. É só vazão, não correção, e as outras instâncias continuam recebendo.
- **Uma mensagem presa numa chamada que não volta só sai das mãos da instância depois do teto de 6 minutos.** Aí ela volta para a fila pelo prazo, com um recebimento gasto, e outra instância a processa; mas a chamada pendurada não é cancelada, e as vagas dela e das seguintes do grupo no limite de mensagens em mãos continuam ocupadas naquela instância até ela reiniciar.
- **Um patch no kysely**, a biblioteca que o MikroORM 7 usa por baixo, para a conexão cortada no meio de uma transação voltar ao pool; sem ele, cada queda do banco deixava a instância com uma conexão a menos, até ser reiniciada. O patch vale só para a versão 0.29.6, então ao atualizar o MikroORM ou o kysely é preciso conferir se a versão nova já corrige isso ou refazer o patch ([o patch e o teste que o prova](docs/banco-de-dados.md#o-patch-no-kysely)).
- **O MiniStack do compose guarda as filas só em memória**: parar o container apaga as filas e as mensagens que estavam nelas. É do emulador, não da aplicação ([como simular a queda sem perder as filas](docs/operacao.md#simular-o-sqs-fora-do-ar)).
- **Logs sem valores:** diagnosticar um problema de valor exige o banco ou a reconciliação.
- **Desligamento gracioso só em Linux/macOS.** No Windows o `SIGTERM` não chega ao processo (E1, item 9); a aplicação é feita para rodar em container.
- **Um `ECONNRESET` esporádico no cliente de teste**, numa das execuções dos testes de concorrência no Windows, com a instância continuando viva. Tentei reproduzir isolado (rajadas de 100 requisições com ociosidade maior que o keep-alive do servidor) e não consegui, então a causa não está diagnosticada; o cliente de teste reenvia com a mesma key, como o provedor faria.
- **A imagem da aplicação inclui os testes e as dependências de desenvolvimento**, para o serviço `tests` do compose usar a mesma imagem. Numa imagem de produção eu separaria em dois estágios.
- **As métricas padrão de processo do prom-client** não foram conferidas no Bun; as métricas do desafio são todas definidas no código e não dependem delas.
- **Ficaram de fora, por serem opcionais:** integração com um Identity Provider (há o desenho e o ponto de extensão, na seção [Autenticação](#autenticação)), ledger de partidas dobradas, OpenTelemetry e dashboard, e teste de carga.

## Desenvolvido com Claude Code

Este projeto foi desenvolvido com o Claude Code, o assistente de programação da Anthropic. Antes da implementação, eu preparei, a partir do enunciado, as minhas instruções de trabalho: o enunciado detalhado item por item, com os valores iniciais de cada número ajustável e as decisões de arquitetura, e com os experimentos E1 e E2 definidos como pré-condição de cada escolha. Elas não estão no repositório, porque são material de trabalho; o que delas virou decisão está neste documento e em `docs/`, com o porquê.

A partir delas, o Claude Code rodou os experimentos, escreveu o código, os testes e esta documentação, registrando cada experimento e cada decisão tomada durante a implementação. Depois, uma revisão completa contra o enunciado original apontou pontos a corrigir (o uso do Unit of Work e do Identity Map, as garantias da wallet no banco, a espera de lock da fila, entre outros), e as correções estão descritas nos documentos de cada assunto. As escolhas que o enunciado não cobria estão sinalizadas como tal onde aparecem (a coluna `balance_currency`, o 500 no replay de `FAILED`, o 400 para id malformado no caminho, os dois clientes SQS e a ordem das checagens de referência), para eu revisar e poder justificar cada uma na apresentação.
