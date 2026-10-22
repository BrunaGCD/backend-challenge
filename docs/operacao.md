# Operação e testes manuais

O que é útil para quem vai mexer na stack à mão: trocar portas, rodar migrations, simular o SQS fora do ar e o crash de uma instância, e os cuidados no Windows. Para subir, usar e testar, o caminho normal está no [README](../README.md).

## Portas ocupadas

As portas do computador podem ser trocadas se alguma já estiver ocupada (um PostgreSQL local, por exemplo): `POSTGRES_PORT`, `SQS_PORT`, `APP_1_PORT`, `APP_2_PORT` e `APP_3_PORT`, como em `POSTGRES_PORT=55432 docker compose up -d --build`. Dentro do Docker nada muda; só quem roda fora dele (os testes na máquina, com o Bun) precisa apontar `DATABASE_URL` ou `SQS_ENDPOINT` para a porta nova.

## Migrations à mão

Elas rodam sozinhas no `docker compose up` (serviço `migrate`). Para rodar à mão, com o PostgreSQL do compose de pé e o Bun instalado:

```sh
bun install
bun run migrate:up      # aplica as pendentes
bun run migrate:down    # desfaz só a última
```

As migrations ficam em `src/infrastructure/persistence/migrations/`, cada uma com `up` e `down` escritos à mão. O que cada uma garante está em [banco de dados](banco-de-dados.md#schema-constraints-e-índices).

## Simular o SQS fora do ar

Tire o MiniStack da rede em vez de pará-lo, porque ele guarda as filas só em memória (abaixo):

```sh
docker network disconnect backend-challenge-main_default backend-challenge-main-ministack-1                  # derruba
docker network connect --alias ministack backend-challenge-main_default backend-challenge-main-ministack-1   # traz de volta
```

O `--alias ministack` devolve o nome que as instâncias usam. Os nomes da rede e do container vêm da pasta do projeto; `docker network ls` e `docker compose ps` mostram os seus. Durante a queda, o HTTP continua respondendo, a readiness acusa `sqs: unavailable` e os eventos esperam na outbox até a fila voltar.

## Simular o crash de uma instância

Mate o processo do Bun dentro do container:

```sh
docker compose exec app-2 sh -c 'kill -9 $(grep -lx bun /proc/[0-9]*/comm | cut -d/ -f3)'
```

A imagem não tem `ps` nem `pgrep`, então o PID sai do `/proc`, e o `restart: unless-stopped` traz a instância de volta em menos de 1 s. Um `docker kill` não serve para isso, porque o Docker o trata como parada manual e a instância fica parada até um `docker compose start app-2`.

## O MiniStack guarda as filas só em memória

`docker compose stop ministack` seguido de `start` apaga as filas e as mensagens que estavam nelas: uma mensagem enviada antes do stop não existia mais depois, e as filas só voltaram porque o `create-queues` rodou de novo quando as instâncias foram religadas. Com `PERSIST_STATE=1`, a opção de persistência do MiniStack, o estado também não foi salvo no stop. Isso é do emulador, não da aplicação: os eventos da outbox continuam pendentes, sem perda, até as filas existirem de novo. Para simular o SQS fora do ar sem apagar as filas, desligar o container da rede (acima) dá à aplicação o mesmo erro que o stop.

## Windows

- **Clone em pasta funda.** Clonar numa pasta funda pode falhar com "Filename too long", porque o caminho da pasta somado ao dos arquivos mais fundos do projeto (as migrations, por exemplo) passa do limite de 260 caracteres do Windows; `git config --global core.longpaths true` antes do clone resolve, e clonar perto da raiz (`C:\src`, por exemplo) também.
- **SIGTERM.** O Windows não entrega `SIGTERM` ao processo, então o desligamento gracioso só funciona em Linux e macOS, e o teste dele é pulado quando a suíte roda no Windows. A aplicação é feita para rodar em container, onde ele funciona e é testado.
