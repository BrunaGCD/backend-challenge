// lint mínimo: o que importa aqui é a fronteira do domínio
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
          { group: ["**/application/**", "**/infrastructure/**"], message: "o domínio não importa camadas de fora" },
        ],
      }],
    },
  },
);
