// lint mínimo: o que importa aqui são as fronteiras do domínio e da aplicação
import tseslint from "typescript-eslint";                                                     // parser e regras de TypeScript

export default tseslint.config(
  { ignores: ["node_modules/**", "experiments/**"] },
  {
    files: ["**/*.ts"],
    languageOptions: { parser: tseslint.parser },                                             // só para o ESLint ler TypeScript
  },
  {
    files: ["src/domain/**/*.ts"],
    rules: {
      "no-restricted-imports": ["error", {
        patterns: [
          { group: ["@nestjs/*"], message: "o domínio não depende do NestJS" },
          { group: ["@mikro-orm/*"], message: "o domínio não depende do ORM" },
          { group: ["**/application/**", "**/infrastructure/**", "**/composition"], message: "o domínio não importa camadas de fora" },
        ],
      }],
    },
  },
  {
    files: ["src/application/**/*.ts"],
    rules: {
      "no-restricted-imports": ["error", {
        patterns: [
          { group: ["@nestjs/*"], message: "a aplicação não depende do NestJS: os controllers recebem os casos de uso prontos" },
          { group: ["@mikro-orm/*"], message: "a aplicação não depende do ORM: fala com o banco pelas portas" },
          { group: ["@aws-sdk/*"], message: "a aplicação não depende do SQS: fala com a fila pelas portas" },
          { group: ["**/infrastructure/**", "**/composition"], message: "a aplicação não importa a infraestrutura" },
        ],
      }],
    },
  },
);
