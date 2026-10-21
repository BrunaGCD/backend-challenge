// ====================================================================================================
//                     BACKOFF EXPONENCIAL COM FULL JITTER (COMUM AOS TRÊS PONTOS)
// ====================================================================================================
// espera = aleatório(0, min(teto, base × 2^tentativa)). O jitter espalha as novas tentativas quando
// muitas falham juntas. O aleatório entra de fora, então nos testes a espera é determinística.

export interface RetryPolicy {
  baseMilliseconds: number;
  capMilliseconds: number;
  random: () => number;                                                                         // número em [0, 1)
}

export function fullJitterDelayMilliseconds(attempt: number, policy: RetryPolicy): number {
  const exponentialMilliseconds = policy.baseMilliseconds * 2 ** attempt;                       // cresce a cada tentativa
  const ceilingMilliseconds = Math.min(policy.capMilliseconds, exponentialMilliseconds);        // mas nunca passa do teto
  return Math.floor(policy.random() * ceilingMilliseconds);
}
