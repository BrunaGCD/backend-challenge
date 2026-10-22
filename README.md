# Distributed Wagering Processor

Serviço que processa as operações de aposta (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) que provedores de jogos mandam por HTTP e por uma fila SQS, mantendo o saldo de cada wallet e um ledger imutável. O problema central é que a entrega é at-least-once: a mesma aposta pode chegar duas vezes, um `REFUND` pode chegar antes da `BET` que ele desfaz, e três instâncias podem mexer na mesma wallet ao mesmo tempo, então o serviço precisa continuar certo nesses casos sem duplicar débito ou crédito e sem deixar saldo negativo.

As decisões, os trade-offs e as limitações estão no [ARCHITECTURE.md](ARCHITECTURE.md). Este arquivo diz como subir, usar e testar.

## O que precisa estar instalado

- Docker com Docker Compose (testei com Docker 29.8 e Compose 5.6).
- [Bun 1.4.3](https://bun.sh) só se for rodar os testes ou a aplicação fora do Docker.

Não precisa de conta em lugar nenhum: o SQS é emulado pelo MiniStack (imagem [`ministackorg/ministack:1.5.30`](https://hub.docker.com/r/ministackorg/ministack)), que sobe sem token.

## Subir tudo

```sh
docker compose up -d --build
```

Isso sobe o PostgreSQL, o MiniStack, roda as migrations (serviço `migrate`), cria as filas (serviço `create-queues`) e sobe **3 instâncias** da aplicação apontando para o mesmo banco e as mesmas filas:

| Instância | Endereço |
|---|---|
| `app-1` | http://localhost:3001 |
| `app-2` | http://localhost:3002 |
| `app-3` | http://localhost:3003 |

As portas do computador podem ser trocadas se alguma já estiver ocupada (um PostgreSQL local, por exemplo): `POSTGRES_PORT`, `SQS_PORT`, `APP_1_PORT`, `APP_2_PORT` e `APP_3_PORT`, como em `POSTGRES_PORT=55432 docker compose up -d --build`. Dentro do Docker nada muda; só quem roda fora dele (o caminho "Na máquina, com o Bun", abaixo) precisa apontar `DATABASE_URL` ou `SQS_ENDPOINT` para a porta nova.

Para ver se estão prontas: `docker compose ps` (as três aparecem `healthy`) ou `curl localhost:3001/health/ready`. A readiness confere a conexão com o banco, as tabelas e a fila de entrada, então uma instância apontando para um banco sem as migrations aparece como não pronta.

Para derrubar: `docker compose down`. Os dados do banco ficam no volume `postgres-data`, então sobrevivem a um `down` e a uma recriação do container. Para começar do zero, apagando o volume:

```sh
docker compose down -v
```

### Migrations

Rodam sozinhas no `docker compose up`. Para rodar à mão (com o PostgreSQL do compose de pé e o Bun instalado):

```sh
bun install
bun run migrate:up      # aplica as pendentes
bun run migrate:down    # desfaz só a última
```

As migrations ficam em `src/infrastructure/persistence/migrations/`, cada uma com `up` e `down` escritos à mão.

## Usar a API

Criar uma wallet com saldo inicial:

```sh
curl -s -X POST localhost:3001/wallets -H 'content-type: application/json' \
  -d '{"playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","initialBalance":{"amount":"1000.00","currency":"BRL"}}'
```

Mandar uma aposta (troque o `walletId` pelo que voltou acima; pode mandar para qualquer uma das três instâncias):

```sh
curl -s -X POST localhost:3002/wagering/transactions -H 'content-type: application/json' \
  -H 'Idempotency-Key: provider-a:transaction-123' \
  -d '{"providerId":"provider-a","externalTransactionId":"transaction-123","playerId":"0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1","walletId":"<walletId>","roundId":"round-987","gameId":"fortune-chimp","kind":"BET","money":{"amount":"25.00","currency":"BRL"}}'
```

Repetir o mesmo comando devolve a mesma resposta com `"idempotentReplay": true`, e o saldo não muda de novo.

Os outros endpoints:

| Método e caminho | O que faz |
|---|---|
| `GET /wallets/:walletId` | wallet com saldo, `version` e `updatedAt` |
| `GET /wallets/:walletId/ledger?limit=50&cursor=...` | lançamentos em ordem de versão, com cursor opaco (`nextCursor`) |
| `POST /wallets/:walletId/reconciliation` | compara o saldo gravado com o ledger, sem corrigir nada |
| `GET /wagering/transactions/:transactionId` | transação pelo id interno |
| `GET /providers/:providerId/wagering/transactions/:externalTransactionId` | transação pelo id do provedor |
| `GET /health/live` e `GET /health/ready` | liveness e readiness (banco e fila) |
| `GET /metrics` | métricas no formato do Prometheus |

Pela fila, a mensagem vai para `wager-transactions.fifo` com `MessageGroupId` igual ao `walletId` e `MessageDeduplicationId` igual ao `messageId` do corpo (o formato da mensagem está no ARCHITECTURE.md). Os eventos publicados saem em `wagering-events.fifo`. Para mandar uma operação pela fila sem escrever código, há um script que monta a mensagem nesse formato e roda pelo container de testes (que já tem o endereço do MiniStack):

```sh
docker compose run --rm --no-deps tests bun scripts/send-wager-message.ts \
  --wallet <walletId> --player 0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1 --kind BET --amount 25.00
```

Ele imprime a mensagem enviada e o caminho para consultar o resultado (`GET /providers/provider-a/wagering/transactions/<externalTransactionId>`). Com `--reference <id externo>` manda um `REFUND`/`ROLLBACK`, repetir `--external-id` testa o replay, e repetir `--message-id` testa a inbox. Com o Bun na máquina, `bun run queue:send -- --wallet ...` faz o mesmo.

## Rodar os testes

Os testes de integração e de concorrência usam o PostgreSQL e o MiniStack de verdade, do próprio compose, e cada execução cria um banco novo (`wagering_test_<data>_<pid>_<sufixo aleatório>`), porque os triggers do ledger impedem apagar lançamentos para limpar as tabelas.

### Dentro do Docker (o caminho que funciona igual em qualquer sistema)

```sh
docker compose up -d --build
docker compose run --rm --no-deps tests
```

O serviço `tests` usa a mesma imagem da aplicação e roda `bun run test` em Linux, contra o PostgreSQL e o MiniStack que o `up` já subiu. O `--no-deps` faz o Compose usar esses containers como estão, sem recriar nenhum: sem ele, de vez em quando o Compose recriava o PostgreSQL antes de rodar os testes. A imagem é construída a partir do código, então depois de mudar alguma coisa é preciso o `--build` de novo.

### Na máquina, com o Bun

```sh
bun install
docker compose up -d postgres ministack

bun test test/unit           # unidade: não precisa de nada de pé
bun run test:integration     # integração: banco e fila reais
bun run test:concurrency     # concorrência: sobe 3 processos da aplicação com Bun.spawn
bun run test                 # os três em sequência
```

No Windows, o teste de desligamento gracioso (`SIGTERM`) é pulado, porque o Windows não entrega `SIGTERM` para o processo (o resultado do experimento E1 está no ARCHITECTURE.md). No Docker ele roda.

Também há `bun run typecheck` (TypeScript estrito) e `bun run lint` (inclusive a regra que impede o domínio e a aplicação de importar NestJS, MikroORM ou a infraestrutura).

## Configuração

Todos os números ajustáveis (timeouts, TTL, backoff, tamanho do pool, nomes das filas) vêm de variáveis de ambiente, com um valor inicial escolhido como padrão. A lista está em [.env.example](.env.example); o Bun lê um `.env` automaticamente se você copiar o exemplo. As credenciais do compose e do exemplo são fictícias, só para os containers locais.

## Estrutura

```
src/
  domain/            Money, Wallet, WalletLedgerEntry, WagerTransaction, eventos, inbox/outbox e as regras (sem NestJS nem ORM)
  application/       casos de uso, portas, validação do comando, payloadHash, publisher da outbox
  infrastructure/    MikroORM (registros, repositórios, migrations), SQS (consumidor, publicação), HTTP (NestJS)
  observability/     logs (pino) e métricas (prom-client)
  composition.ts     monta todas as peças; os testes trocam uma porta por aqui para injetar falha
test/
  unit/  integration/  concurrency/  support/
experiments/         os roteiros dos experimentos E1 e E2, rodados antes da implementação
patches/             patch do kysely aplicado pelo bun install (o porquê está no ARCHITECTURE.md, §18)
```
