# Testes

Como rodar a suíte, o que cada arquivo prova e as regras que valem para todos eles. Na execução de 22/10/2026, dentro do container do compose, foram **292 testes, nenhuma falha**: 163 de unidade, 119 de integração e 10 de concorrência.

## Como rodar

Os testes de integração e de concorrência usam o PostgreSQL e o MiniStack de verdade, do próprio compose, e cada execução cria um banco novo (`wagering_test_<data>_<pid>_<sufixo aleatório>`), porque os triggers do ledger impedem apagar lançamentos para limpar as tabelas.

**Dentro do Docker**, o caminho que funciona igual em qualquer sistema:

```sh
docker compose up -d --build
docker compose run --rm --no-deps tests
```

O serviço `tests` usa a mesma imagem da aplicação e roda `bun run test` em Linux, contra o PostgreSQL e o MiniStack que o `up` já subiu. O `--no-deps` faz o Compose usar esses containers como estão, sem recriar nenhum: sem ele, de vez em quando o Compose recriava o PostgreSQL antes de rodar os testes. A imagem é construída a partir do código, então depois de mudar alguma coisa é preciso o `--build` de novo.

**Na máquina, com o Bun:**

```sh
bun install
docker compose up -d postgres ministack

bun test test/unit           # unidade: não precisa de nada de pé
bun run test:integration     # integração: banco e fila reais
bun run test:concurrency     # concorrência: sobe 3 processos da aplicação com Bun.spawn
bun run test                 # os três em sequência
```

No Windows, o teste de desligamento gracioso (`SIGTERM`) é pulado, porque o Windows não entrega `SIGTERM` para o processo ([experimento E1](medicoes-e-historico.md#os-experimentos-antes-da-implementação)). No Docker ele roda. Também há `bun run typecheck` (TypeScript estrito) e `bun run lint` (inclusive a regra que impede o domínio e a aplicação de importar NestJS, MikroORM ou a infraestrutura).

## O que cada arquivo prova

Entre parênteses, o item da seção 13 do desafio que o arquivo cobre.

**Unidade** (`test/unit`), sem infraestrutura nenhuma:

| Arquivo | O que prova |
|---|---|
| [money.test.ts](../test/unit/money.test.ts) | formato aceito na entrada, aritmética exata sem arredondamento em silêncio, conflito de moeda, nenhum `number` no caminho do dinheiro (`Money`, conflito de moeda) |
| [wallet.test.ts](../test/unit/wallet.test.ts) | abertura, débito e crédito sempre com o lançamento junto, e o `WalletLedgerEntry` que confere a própria aritmética (invariantes da `Wallet`) |
| [wager-settlement.test.ts](../test/unit/wager-settlement.test.ts) | cada regra de `BET`, `WIN`, `LOSS`, `REFUND` e `ROLLBACK`, a interpretação I1 e a nova tentativa de uma pendente (regras de negócio) |
| [wager-transaction.test.ts](../test/unit/wager-transaction.test.ts) | criação, transições, estados terminais, o `matchesPayload` e a forma readonly da classe pedida no esqueleto da seção 6.3 |
| [payload-hash.test.ts](../test/unit/payload-hash.test.ts) | vetores fixos do hash; o hash muda com qualquer campo de negócio e não muda com o que é transporte; e o caso de uso, com portas falsas, devolve replay para o mesmo payload e `IdempotencyKeyConflictError` para outro (idempotency key com payload divergente) |
| [supporting-rules.test.ts](../test/unit/supporting-rules.test.ts) | backoff com aleatório injetado, classificação dos erros de infraestrutura, cursor do ledger, eventos, inbox, outbox e a ordem dos timeouts na configuração |
| [background-loop.test.ts](../test/unit/background-loop.test.ts) | a espera crescente dos laços de fundo depois de falhas seguidas |
| [metrics.test.ts](../test/unit/metrics.test.ts) | o texto do `/metrics`: os contadores de poucos labels aparecem em zero antes do primeiro incremento, e `transactions_total` e `dlq_forwarded_total` só ganham série no primeiro |

**Integração** (`test/integration`), com o PostgreSQL e o MiniStack do compose:

| Arquivo | O que prova |
|---|---|
| [migrations-and-constraints.test.ts](../test/integration/migrations-and-constraints.test.ts) | `up → down → up` e cada garantia do schema recusando a violação por SQL direto (migrations e constraints) |
| [atomicity.test.ts](../test/integration/atomicity.test.ts) | falha no meio desfaz wallet, ledger, inbox e outbox juntos, e o evento só fica visível depois do commit (atomicidade) |
| [sqs-consumer.test.ts](../test/integration/sqs-consumer.test.ts) | ack depois do commit, a mesma mensagem duas vezes sem efeito duplicado, a inbox reconhecendo cópias, e os erros permanentes indo para a DLQ (inbox e redelivery) |
| [outbox-publishers.test.ts](../test/integration/outbox-publishers.test.ts) | publishers concorrentes sobre a mesma outbox, falhas na publicação e a publicação duplicada segura para o consumidor (publishers concorrentes) |
| [retry-and-dlq.test.ts](../test/integration/retry-and-dlq.test.ts) | nova tentativa com erro injetado e com `lock_timeout` real, orçamento, DLQ, prazo de visibilidade e o teto por mensagem, no HTTP e na fila (retry e DLQ) |
| [restart-recovery.test.ts](../test/integration/restart-recovery.test.ts) | com a aplicação fora, mensagens chegam, uma pendente espera e eventos ficam sem publicar; uma instância nova termina tudo sem perder nem duplicar (recuperação após reinicialização) |
| [dependencies-unavailable.test.ts](../test/integration/dependencies-unavailable.test.ts) | PostgreSQL e SQS fora do ar por um proxy TCP cortado e religado pelo teste, a pausa do consumidor, a conexão que não pode ficar presa no pool e a readiness sem as tabelas |
| [unit-of-work.test.ts](../test/integration/unit-of-work.test.ts) | Unit of Work, Identity Map e o optimistic lock do MikroORM no banco real |
| [http-wagering.test.ts](../test/integration/http-wagering.test.ts) | a API pela borda: idempotência persistente, replay, conflitos, validação, regras e status |

**Concorrência** (`test/concurrency`), com 3 processos da aplicação via `Bun.spawn`, paralelismo real:

| Arquivo | O que prova |
|---|---|
| [multi-instance.test.ts](../test/concurrency/multi-instance.test.ts) | cenários 1 (50 cópias da mesma aposta), 2 (duas apostas de 80.00 sobre 100.00, com as duas esperando o mesmo lock), 3 (wallets distintas em paralelo), 4 (três instâncias com HTTP e fila misturados), 6 (os publishers das três instâncias sobre a mesma outbox) e 7 (`ROLLBACK` e `REFUND` antes da referência) |
| [crash-and-restart.test.ts](../test/concurrency/crash-and-restart.test.ts) | cenários 5 (worker morto depois do commit e antes do ack) e 8 (`SIGKILL` numa instância no meio da carga, com a consistência final conferida), e o desligamento gracioso com `SIGTERM` |
| [hot-wallet.test.ts](../test/concurrency/hot-wallet.test.ts) | 300 operações misturadas numa wallet só, nas 3 instâncias e na fila: prova de correção sob disputa, não teste de carga |

## Nenhum mock de banco ou de fila

As falhas entram pela composição das portas: uma outbox que lança erro entre o lançamento e a outbox (atomicidade), uma outbox que segura a transação antes do commit (evento só depois do commit), um repositório que lança `55P03` (transitório), e uma entrada só de teste, [crash-after-commit-main.ts](../test/support/crash-after-commit-main.ts), que sobe a aplicação normal com uma fila cujo `DeleteMessage` vira um `SIGKILL` no próprio processo (worker morto depois do commit e antes do ack). Portas falsas aparecem só em testes de unidade, onde o assunto é a regra e não o banco (o caso de uso diante da mesma key com outro payload, em [payload-hash.test.ts](../test/unit/payload-hash.test.ts)).

## A invariante final

Todo arquivo de teste que mexe em wallets pela aplicação termina chamando um verificador ([invariants.ts](../test/support/invariants.ts)) que, para cada wallet tocada, lê o banco com SQL direto num snapshot só e confere `balance = Σ créditos − Σ débitos`, que o `balanceBefore` de cada lançamento é o `balanceAfter` do anterior, que os `wallet_version` não têm buracos, que cada lançamento tem um `WalletBalanceChanged` na outbox e que cada transação `PROCESSED` que move saldo tem exatamente um lançamento.

## Determinismo e isolamento

Relógio e aleatório entram por porta; as esperas são por condição com prazo (`waitUntil`), não por `sleep` fixo; TTL, visibilidade e backoff ficam curtos por configuração nos testes; sorteios com semente 42. Cada execução tem um banco novo, criado e migrado no início ([fresh-database.ts](../test/support/fresh-database.ts)), porque os triggers do ledger impedem limpar tabelas; as filas têm nome único por arquivo, e os ids são únicos por teste.

Nos testes de concorrência, o cliente HTTP faz o papel do provedor: quando recebe 503 (`lock_timeout` numa wallet disputada) ou quando a conexão cai antes da resposta, reenvia a mesma operação com a mesma key e conta quantas vezes fez isso. As asserções de correção (um débito só, saldo esperado, invariantes) continuam as mesmas.
