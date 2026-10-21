# Experimentos

Roteiros descartáveis rodados antes da implementação, para decidir o ORM (E1) e o emulador do SQS (E2). O resultado de cada item e a decisão estão no `ARCHITECTURE.md`.

Com o `docker compose up -d postgres ministack` rodando:

```sh
cd experiments/e1-stack-bun && bun install && bun orm-items.ts && bun process-items.ts
cd experiments/e2-ministack-sqs && bun install && bun e2.ts
```

O item 9 do E1 (SIGTERM) só passa em Linux ou macOS; no Windows, o mesmo roteiro roda num container:

```sh
docker run --rm --network backend-challenge-main_default -e DATABASE_URL=postgresql://wagering:wagering@postgres:5432/wagering -v "$PWD/experiments/e1-stack-bun:/e1-src:ro" oven/bun:1.4.3 sh -c "cp -r /e1-src /e1 && cd /e1 && rm -rf node_modules && bun install && bun orm-items.ts && bun process-items.ts"
```
