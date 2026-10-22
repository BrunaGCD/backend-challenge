# Medições e histórico

O que está aqui sustenta decisões descritas no [ARCHITECTURE.md](../ARCHITECTURE.md) e nos outros documentos de `docs/`: o resultado dos experimentos feitos antes do código, os números medidos e o que aconteceu com desenhos anteriores que levou ao desenho atual. Ninguém precisa ler isto para entender o projeto; é a evidência para quem quiser conferir uma decisão.

## Os experimentos antes da implementação

Antes de escrever o código, deixei duas escolhas condicionadas a experimento, com o critério de cada uma definido antes de rodar. Os roteiros estão em [experiments/](../experiments/) e rodaram antes de qualquer código do projeto.

**E1: a stack no Bun** (decide o ORM, o desligamento, logs, métricas e os testes com várias instâncias). Um projeto descartável com NestJS 11.2.7, MikroORM 7.2.5, pino 10.4.0 e prom-client 15.1.3 contra o `postgres:17-alpine` do compose, rodado no Windows (Bun 1.4.3) e num container `oven/bun:1.4.3` (Linux). Pelo critério definido antes, os itens 1 a 7 passaram, então o ORM é o **MikroORM 7**; os itens 8, 10 e 11 confirmam pino com `mixin` lendo o `AsyncLocalStorage` e prom-client; o item 12 confirma as 3 instâncias via `Bun.spawn` nos testes. O item 9 (o `SIGTERM` dispara os hooks do NestJS) passou no Linux, que é onde a aplicação roda, então o desligamento ficou nos hooks do NestJS. No Windows ele falhou, porque o sistema não entrega `SIGTERM` ao processo, e por isso o teste de desligamento gracioso é pulado no Windows e roda no serviço `tests` do compose.

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

**E2: o MiniStack se comporta como o SQS FIFO?** (decide o emulador). Um script com `@aws-sdk/client-sqs` contra `ministackorg/ministack:1.5.30`. Tudo passou, então o emulador é o **MiniStack**, que sobe sem conta (desde março de 2026 a imagem `latest` do LocalStack exige token, o que obrigaria o avaliador a criar uma conta). Achei uma diferença no item 4: no envio duplicado, o MiniStack devolve o `MessageId` e o MD5 do corpo da mensagem **original**, então, se a cópia tiver corpo diferente, o SDK compara o MD5 com o corpo novo e lança `InvalidChecksumError`. Não confirmei o que a AWS devolve nesse caso. Isso não afeta o projeto, porque nos dois lugares em que ele reenvia com o mesmo `MessageDeduplicationId` (republicação de evento da outbox e reenvio explícito para a DLQ) o corpo é idêntico.

| Item | Resultado |
|---|---|
| 1 FIFO e DLQ FIFO com `RedrivePolicy` e `maxReceiveCount = 2` | passou |
| 2 um por vez no mesmo grupo (a segunda não vem com a primeira em voo) | passou |
| 3 grupos diferentes recebidos ao mesmo tempo por dois consumidores | passou |
| 4 deduplicação por `MessageDeduplicationId` | passou, com a diferença do MD5 descrita acima |
| 5 `ChangeMessageVisibility` para 0 e para N segundos | passou |
| 6 `ApproximateReceiveCount` sobe e o redrive leva para a DLQ | passou (contagens 1 e 2, depois a mensagem aparece na DLQ) |
| 7 long polling de 20 s volta assim que a mensagem chega | passou (voltou em 2,22 s com a mensagem enviada aos 2 s) |
| 8 `ApproximateNumberOfMessages` da DLQ | passou |
| 9 `SendMessageBatch` com 10 | passou |

## Por que a fila tem timeouts próprios

Com o mesmo `lock_timeout` de 2 s do HTTP, numa wallet muito disputada, uma operação válida da fila desistia do lock várias vezes seguidas e chegava à DLQ sem nada gravado. Foi isso que levou aos timeouts separados por canal: na fila, no worker e no publisher, a espera pelo lock é de 10 s ([banco de dados](banco-de-dados.md#concorrência)).

## Hot wallet medida

No teste de hot wallet, a métrica `wagering_wallet_lock_wait_seconds` registrou, nas 8 execuções feitas dentro do container Linux do compose depois da separação dos timeouts, entre 308 e 355 travas, com média de espera entre 226 e 717 ms. Parte das respostas HTTP passou dos 2 s de `lock_timeout` e voltou 503: 0 em quatro execuções, 13 e 46 em outras duas, e 84 e 92 nas duas em que a máquina estava mais carregada e a suíte inteira levou de 2 a 3 vezes mais tempo. Em nenhuma das 8 uma mensagem válida foi parar na DLQ.

O teste sobe as instâncias com `DATABASE_POOL_MAX = 5` porque, com o padrão de 20, as 12 instâncias da prova com 4 hot wallets em paralelo abririam até 240 conexões, o próprio banco recusava conexões ("too many clients already"), e a falha por conexão escondia o que estava sendo medido. Em duas rodadas de 4 hot wallets em paralelo, cada uma num `docker compose run` próprio, começando com 3 s de intervalo e com as 3 instâncias do compose paradas para sobrar CPU, o pico foi de 68 e 61 conexões, e as 8 execuções terminaram com 273 transações cada e nenhuma mensagem na DLQ.

## Quando o ApproximateReceiveCount era o orçamento

Nas primeiras versões, a contagem de recebimentos do SQS era o único orçamento de tentativas, com `maxReceiveCount = 5`: cada falha transitória devolvia a mensagem com o backoff, as seguintes da mesma wallet voltavam junto sem terem sido tentadas, e, com a espera sorteada entre 0 e min(60 s, 2 s × 2^(recebimento − 1)), os 5 recebimentos duravam em média uns 31 s e no máximo 62 s. Numa queda do banco de 64 s, duas de dez operações válidas de uma wallet foram para a DLQ. Isso levou ao orçamento de tempo no próprio consumidor ([fila e outbox](fila-e-outbox.md#o-limite-de-tentativas-é-um-orçamento-de-tempo)), e o `maxReceiveCount` subiu de 5 para 10, como rede de segurança para as saídas das mãos do consumidor.

## Os desenhos anteriores da busca e da extensão de prazo

O consumidor só buscava mensagens de novo quando todos os grupos do recebimento anterior terminavam, e com o orçamento de 5 minutos isso virou um problema: uma mensagem tentando de novo segurava as mensagens de todas as outras wallets daquela instância (com a wallet A travada, uma mensagem da wallet B, livre, esperou 25 s, o tempo que A ficou presa). Isso levou à busca que não espera os grupos em mãos, dentro do limite de mensagens em mãos de cada instância.

A primeira versão da conferência "cabe mais uma espera inteira no prazo?" devolvia as mensagens que não cabiam com visibilidade 0, em vez de estender o prazo. Cada devolução contava um recebimento, e sob disputa forte (4 hot wallets em paralelo) as do fim do lote chegavam ao limite e iam para a DLQ sem nada de errado nelas.

## Quedas medidas no compose

Com a configuração de produção, cada prova num ambiente recém-criado:

- **PostgreSQL parado** (`docker compose stop postgres`) por 129 s, com 10 operações de cada uma de duas wallets mandadas pela fila durante a queda: as 20 viraram transação, a DLQ ficou vazia e todas foram confirmadas com 1 recebimento; a que mais esperou chegou à 8ª tentativa no consumidor.
- **A mesma queda com um SIGKILL** na instância que segurava uma das mensagens: a instância voltou sozinha pelo `restart`, essa mensagem foi a única com 2 recebimentos, e a DLQ ficou vazia de novo.
- **PostgreSQL saturado:** com as 97 conexões livres do PostgreSQL ocupadas por 60 s, as instâncias continuaram processando pelas conexões que já tinham no pool. Derrubando também essas conexões a cada 2 s, houve 12 tentativas no consumidor com "too many clients" e 2 pausas, e as 20 operações viraram transação depois da liberação, a última 40 s depois (a espera entre tentativas pode chegar a 60 s), com 1 recebimento cada e a DLQ vazia.
- **MiniStack fora da rede** por 67 s (ver [operação](operacao.md#o-ministack-guarda-as-filas-só-em-memória) sobre o `docker compose stop ministack`): as 20 operações mandadas por HTTP durante a queda responderam 200, os eventos esperaram na outbox, e depois da volta estavam lá as 40 transações e os 86 eventos publicados, uma vez cada; as duas mensagens que já tinham commit repetiram só o `DeleteMessage` até a fila voltar.
- **`docker compose stop`** numa instância que esperava uma nova tentativa: ela devolveu a mensagem e parou em cerca de 1 s, e outra instância a processou com 2 recebimentos.

## SIGTERM medido

Com grupos em mãos, quando a busca aberta ainda era cancelada no desligamento, a mensagem devolvida chegava à outra instância com 3 recebimentos em vez de 2, porque a busca cancelada continuava aberta do lado do MiniStack e pegava a mensagem. Sem nada em mãos, o cancelamento continua: com 60 mensagens chegando durante o `docker compose stop` de uma instância, uma delas, enviada 108 ms depois de a instância terminar de desligar, foi pega pela busca cancelada e chegou à outra instância com 2 recebimentos (as outras 59, com 1). Com `docker compose stop`, uma instância saiu do primeiro ao último passo do desligamento em 123 ms.

## O teto do backoff das pendentes

O teto era de 60 s nas primeiras versões. Depois de um minuto pendente, a espera média já era de 30 s, e quem testasse à mão mandando o `REFUND` antes da `BET` veria `PENDING_REFERENCE` por quase um minuto depois de a `BET` chegar. Numa simulação do sorteio com a varredura de 1 s (semente 42, 2.000 pendentes que esperam o TTL inteiro), uma pendente é tentada umas 110 vezes com o teto de 10 s, contra umas 24 com 60 s.

## Os laços de fundo numa queda

Antes da espera crescente, uma queda de 65 s do PostgreSQL gerava 155 linhas de erro somando as 3 instâncias, uma por intervalo de cada laço.

## Como cheguei aos dois pontos do patch do kysely

Com o patch só do rollback, o teste "transação cortada no meio" falhou em 2 de 5 execuções. Rastreando essas falhas, achei o segundo ponto: a conexão que sobrava era uma em que o `BEGIN` tinha falhado com "Connection terminated unexpectedly", pega fora da transação do próprio teste, por quem mais usa o mesmo pool. Esse caminho depende de tempo, então o teste o exercita só às vezes, enquanto o do rollback ele exercita sempre. Com os dois pontos, foram 20 execuções sem falha, uma delas com o `BEGIN` falhando de fato.
