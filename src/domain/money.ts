// ====================================================================================================
//                                MONEY: VALOR EXATO EM CENTAVOS
// ====================================================================================================
// O valor fica num BigInt de centavos. Nenhuma conta do sistema divide ou multiplica dinheiro, então
// somar, subtrair, negar e comparar centavos é exato. Misturar BigInt com number lança TypeError, então
// um number que escape para cá quebra na hora em vez de arredondar escondido.
import { CurrencyMismatchError, InvalidMoneyError } from "./errors";                           // erros de valor e de moeda

export interface MoneyProps {
  amount: string;                                                                               // decimal com 2 casas, ex.: "25.00"
  currency: string;                                                                             // ISO-4217, ex.: "BRL"
}

const INPUT_AMOUNT_PATTERN = /^(0|[1-9][0-9]{0,15})\.[0-9]{2}$/;                                // até 16 dígitos inteiros: cabe em NUMERIC(18,2)
const CURRENCY_PATTERN = /^[A-Z]{3}$/;                                                          // três letras maiúsculas
const CENTS_PER_UNIT = 100n;

export class Money {
  private constructor(
    private readonly cents: bigint,
    public readonly currency: string,
  ) {
    Object.freeze(this);                                                                        // imutável de verdade, não só por convenção
  }

  static from(props: MoneyProps): Money {
    if (typeof props?.amount !== "string" || !INPUT_AMOUNT_PATTERN.test(props.amount)) {
      throw new InvalidMoneyError(`valor inválido: ${JSON.stringify(props?.amount)} (esperado como "25.00", sem sinal, com 2 casas)`);
    }
    assertValidCurrency(props.currency);
    const [integerPart, fractionPart] = props.amount.split(".") as [string, string];           // o padrão já garantiu as duas partes
    const cents = BigInt(integerPart) * CENTS_PER_UNIT + BigInt(fractionPart);
    return new Money(cents, props.currency);
  }

  static zero(currency: string): Money {
    assertValidCurrency(currency);
    return new Money(0n, currency);
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.cents + other.cents, this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.cents - other.cents, this.currency);
  }

  negate(): Money {
    return new Money(-this.cents, this.currency);
  }

  isZero(): boolean {
    return this.cents === 0n;
  }

  isPositive(): boolean {
    return this.cents > 0n;
  }

  isNegative(): boolean {
    return this.cents < 0n;
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.cents < other.cents;
  }

  equals(other: Money): boolean {
    return this.currency === other.currency && this.cents === other.cents;                     // moeda diferente não é igual, nem é erro
  }

  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }

  toString(): string {
    const sign = this.cents < 0n ? "-" : "";                                                    // só o Money interno pode ser negativo
    const absoluteCents = this.cents < 0n ? -this.cents : this.cents;
    const integerPart = absoluteCents / CENTS_PER_UNIT;                                         // divisão inteira de BigInt, sem arredondar
    const fractionPart = (absoluteCents % CENTS_PER_UNIT).toString().padStart(2, "0");
    return `${sign}${integerPart}.${fractionPart}`;
  }

  private assertSameCurrency(other: Money): void {
    if (other.currency !== this.currency) {
      throw new CurrencyMismatchError(`moedas diferentes: ${this.currency} e ${other.currency}`);
    }
  }
}

function assertValidCurrency(currency: unknown): void {
  if (typeof currency !== "string" || !CURRENCY_PATTERN.test(currency)) {
    throw new InvalidMoneyError(`moeda inválida: ${JSON.stringify(currency)} (esperado código ISO de 3 letras, ex.: "BRL")`);
  }
}
