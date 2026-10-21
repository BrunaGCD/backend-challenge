# Uma imagem só para a aplicação e para a suíte de testes (o serviço "tests" do compose usa esta mesma).
FROM oven/bun:1.4.3

WORKDIR /app

COPY package.json bun.lock ./
COPY patches ./patches
RUN bun install --frozen-lockfile

COPY tsconfig.json eslint.config.js bunfig.toml ./
COPY src ./src
COPY scripts ./scripts
COPY test ./test

EXPOSE 3000

# forma exec: o Bun é o PID que recebe o SIGTERM (o compose também usa init: true)
CMD ["bun", "src/main.ts"]
