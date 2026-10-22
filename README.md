# Distributed Wagering Processor

Serviço que processa as operações de aposta (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`) que provedores de jogos mandam por HTTP e por uma fila SQS, mantendo o saldo de cada wallet e um ledger imutável. O problema central é que a entrega é at-least-once: a mesma aposta pode chegar duas vezes, um `REFUND` pode chegar antes da `BET` que ele desfaz, e três instâncias podem mexer na mesma wallet ao mesmo tempo, então o serviço precisa continuar certo nesses casos sem duplicar débito ou crédito e sem deixar saldo negativo.

## Como a documentação está organizada

A documentação tem três níveis, do mais curto ao mais detalhado, para cada pessoa ler só até onde precisar:

- **Este README** diz como subir, usar e testar o projeto.
- **O [ARCHITECTURE.md](ARCHITECTURE.md)** resume as decisões: o que decidi em cada assunto, por quê e o que custa. É a leitura para entender o projeto.
- **A pasta [`docs/`](docs/)** tem o detalhe de cada assunto, para quem quiser conferir uma decisão a fundo:

| Documento | Assunto |
|---|---|
| [regras-de-negocio.md](docs/regras-de-negocio.md) | estados da transação, interpretações das regras, referências fora de ordem e códigos de falha |
| [api-http.md](docs/api-http.md) | status e corpos da API, idempotência e o algoritmo do `payloadHash`, reconciliação e cursor do ledger |
| [banco-de-dados.md](docs/banco-de-dados.md) | uso do MikroORM, lock e concorrência, todas as garantias do schema e o patch do kysely |
| [fila-e-outbox.md](docs/fila-e-outbox.md) | a vida de uma mensagem na fila, retry, DLQ, desligamento, outbox e o contrato dos eventos |
| [testes.md](docs/testes.md) | como rodar a suíte e o que cada arquivo de teste prova |
| [operacao.md](docs/operacao.md) | portas, migrations à mão, como simular quedas e crash, cuidados no Windows |
| [medicoes-e-historico.md](docs/medicoes-e-historico.md) | resultado dos experimentos, números medidos e os desenhos anteriores que levaram ao atual |

## O que precisa estar instalado

- Docker com Docker Compose (testei com Docker 29.8 e Compose 5.6).
- [Bun 1.4.3](https://bun.sh) só se for rodar os testes ou a aplicação fora do Docker.

Não precisa de conta em lugar nenhum: o SQS é emulado pelo MiniStack (imagem [`ministackorg/ministack:1.5.30`](https://hub.docker.com/r/ministackorg/ministack)), que sobe sem token. No Windows, clone perto da raiz da unidade ou rode `git config --global core.longpaths true` antes do clone ([por quê](docs/operacao.md#windows)).

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

Para ver se estão prontas: `docker compose ps` (as três aparecem `healthy`) ou `curl localhost:3001/health/ready`, que confere a conexão com o banco, as tabelas e a fila de entrada. Se alguma porta já estiver ocupada no seu computador, dá para trocá-la ([portas](docs/operacao.md#portas-ocupadas)).

Para derrubar: `docker compose down`. Os dados do banco ficam no volume `postgres-data`, então sobrevivem a um `down`; para começar do zero, `docker compose down -v`.

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

Pela fila, a mensagem vai para `wager-transactions.fifo` com `MessageGroupId` igual ao `walletId` e `MessageDeduplicationId` igual ao `messageId` do corpo, que segue o formato da seção 10 do desafio ([contrato da fila](docs/fila-e-outbox.md#as-filas-e-o-contrato)). Os eventos publicados saem em `wagering-events.fifo`. Para mandar uma operação pela fila sem escrever código, há um script que monta a mensagem e roda pelo container de testes:

```sh
docker compose run --rm --no-deps tests bun scripts/send-wager-message.ts \
  --wallet <walletId> --player 0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1 --kind BET --amount 25.00
```

Ele imprime a mensagem enviada e o caminho para consultar o resultado (`GET /providers/provider-a/wagering/transactions/<externalTransactionId>`). Com `--reference <id externo>` manda um `REFUND`/`ROLLBACK`, repetir `--external-id` testa o replay, e repetir `--message-id` testa a inbox. Com o Bun na máquina, `bun run queue:send -- --wallet ...` faz o mesmo.

## Rodar os testes

Com a stack de pé, a suíte inteira roda dentro do Docker, igual em qualquer sistema:

```sh
docker compose up -d --build
docker compose run --rm --no-deps tests
```

São testes de unidade, de integração e de concorrência; os dois últimos usam o PostgreSQL e o MiniStack de verdade, e os de concorrência sobem 3 processos da aplicação. Como rodar na máquina com o Bun, e o que cada arquivo de teste prova, está em [testes](docs/testes.md).

## Estrutura do código

O código segue a arquitetura em camadas do DDD, e dentro do domínio e da aplicação os arquivos ficam agrupados nos três módulos do serviço. Uma regra de lint impede o domínio e a aplicação de importar NestJS, MikroORM ou a infraestrutura.

```
src/
  domain/            Money, erros, backoff e eventos; wallet/, wagering/ e messaging/ com as entidades e as regras
  application/       portas, erros e consultas; wallet/, wagering/ e messaging/ com os casos de uso
  infrastructure/    MikroORM (registros, repositórios, migrations), SQS (consumidor, publicação), HTTP (NestJS)
  observability/     logs (pino) e métricas (prom-client)
  composition.ts     monta todas as peças; os testes trocam uma porta por aqui para injetar falha
test/
  unit/  integration/  concurrency/  support/
experiments/         os roteiros dos experimentos E1 e E2, rodados antes da implementação
patches/             patch do kysely aplicado pelo bun install
```

## Configuração

Os números ajustáveis (timeouts, TTL, backoff, tamanho do pool, nomes das filas) vêm de variáveis de ambiente, cada uma com um padrão no código ([src/config.ts](src/config.ts)). A lista completa, agrupada por assunto e com esses mesmos padrões, está em [.env.example](.env.example): copiar o exemplo para `.env` não muda nada até você mudar um valor, e o Bun lê o `.env` sozinho fora do Docker. As credenciais do compose e do exemplo são fictícias, só para os containers locais.
