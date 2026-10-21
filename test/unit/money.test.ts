import { describe, expect, test } from "bun:test";                                             // runner de testes do Bun
import { Money } from "../../src/domain/money";                                                 // valor monetário em centavos
import { CurrencyMismatchError, InvalidMoneyError } from "../../src/domain/errors";             // erros de domínio esperados

const brl = (amount: string) => Money.from({ amount, currency: "BRL" });                      // atalho para os casos em reais

describe("Money: formato aceito na entrada", () => {
  test.each(["0.00", "0.01", "25.00", "1000.00", "9999999999999999.99"])("aceita %p e serializa igual", (amount) => {
    expect(brl(amount).toJSON()).toEqual({ amount, currency: "BRL" });
  });

  test.each([
    ["25", "sem casas decimais"],
    ["25.5", "uma casa só (não vira 25.50)"],
    ["10.005", "três casas (não vira 10.01)"],
    ["025.00", "zero à esquerda"],
    ["-1.00", "negativo no contrato de entrada"],
    ["+1.00", "sinal de mais"],
    ["1e3", "notação científica"],
    ["1.5e2", "notação científica com ponto"],
    [" 25.00", "espaço antes"],
    ["25.00 ", "espaço depois"],
    ["", "string vazia"],
    ["NaN", "NaN"],
    ["Infinity", "Infinity"],
    ["-Infinity", "-Infinity"],
    ["25,00", "vírgula no lugar do ponto"],
    [".50", "sem parte inteira"],
    ["10000000000000000.00", "17 dígitos inteiros, não cabe em NUMERIC(18,2)"],
    ["0x10.00", "hexadecimal"],
  ])("rejeita %p (%s)", (amount) => {
    expect(() => brl(amount)).toThrow(InvalidMoneyError);
  });

  test("rejeita amount que não é string, inclusive number", () => {
    expect(() => Money.from({ amount: 25 as unknown as string, currency: "BRL" })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: NaN as unknown as string, currency: "BRL" })).toThrow(InvalidMoneyError);
    expect(() => Money.from({ amount: null as unknown as string, currency: "BRL" })).toThrow(InvalidMoneyError);
  });

  test.each(["brl", "BR", "BRLL", "R$", "", "12A"])("rejeita moeda %p fora do formato ISO de 3 letras", (currency) => {
    expect(() => Money.from({ amount: "1.00", currency })).toThrow(InvalidMoneyError);
  });
});

describe("Money: aritmética exata, sem arredondamento em silêncio", () => {
  test("0.10 + 0.20 dá exatamente 0.30", () => {
    expect(brl("0.10").add(brl("0.20")).toString()).toBe("0.30");
    expect(brl("0.10").add(brl("0.20")).equals(brl("0.30"))).toBe(true);
  });

  test("subtrair até zero serializa 0.00", () => {
    const zero = brl("25.00").subtract(brl("25.00"));
    expect(zero.toJSON()).toEqual({ amount: "0.00", currency: "BRL" });
    expect(zero.isZero()).toBe(true);
  });

  test("negate de 25.00 serializa -25.00, e negate de zero continua 0.00", () => {
    expect(brl("25.00").negate().toString()).toBe("-25.00");
    expect(brl("0.00").negate().toString()).toBe("0.00");
  });

  test("subtrair mais do que tem dá negativo com sinal e duas casas", () => {
    const negative = brl("10.00").subtract(brl("10.05"));
    expect(negative.toString()).toBe("-0.05");
    expect(negative.isNegative()).toBe(true);
    expect(negative.isPositive()).toBe(false);
  });

  test("valores grandes continuam exatos (onde um number perderia precisão)", () => {
    const large = brl("9999999999999999.99");
    expect(large.subtract(brl("0.01")).toString()).toBe("9999999999999999.98");
    expect(brl("4503599627370496.01").add(brl("0.01")).toString()).toBe("4503599627370496.02");
  });

  test("comparações", () => {
    expect(brl("20.00").isLessThan(brl("80.00"))).toBe(true);
    expect(brl("80.00").isLessThan(brl("80.00"))).toBe(false);
    expect(brl("0.01").isPositive()).toBe(true);
    expect(brl("0.00").isPositive()).toBe(false);
    expect(Money.zero("BRL").equals(brl("0.00"))).toBe(true);
  });

  test("é imutável: operar devolve outra instância e não muda a original", () => {
    const original = brl("10.00");
    const sum = original.add(brl("5.00"));
    expect(sum).not.toBe(original);
    expect(original.toString()).toBe("10.00");
    expect(Object.isFrozen(original)).toBe(true);
  });
});

describe("Money: conflito de moeda", () => {
  const usd = Money.from({ amount: "1.00", currency: "USD" });

  test("somar, subtrair e comparar moedas diferentes lança CurrencyMismatchError", () => {
    expect(() => brl("1.00").add(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl("1.00").subtract(usd)).toThrow(CurrencyMismatchError);
    expect(() => brl("1.00").isLessThan(usd)).toThrow(CurrencyMismatchError);
  });

  test("equals entre moedas diferentes é falso, não erro", () => {
    expect(brl("1.00").equals(usd)).toBe(false);
  });
});

describe("Money: nenhum number no caminho do dinheiro", () => {
  test("toJSON devolve string, nunca number", () => {
    const json = brl("12.34").toJSON();
    expect(typeof json.amount).toBe("string");
  });
});
