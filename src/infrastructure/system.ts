// ====================================================================================================
//                         RELÓGIO, IDS E ALEATÓRIO DO SISTEMA (IMPLEMENTAÇÕES DAS PORTAS)
// ====================================================================================================
import type { Clock, IdGenerator, RandomSource } from "../application/ports";

export const systemClock: Clock = { now: () => new Date() };

export const uuidV7Generator: IdGenerator = { newId: () => Bun.randomUUIDv7() };               // id nasce na aplicação, sem DEFAULT no banco

export const mathRandom: RandomSource = { next: () => Math.random() };                         // só para o jitter do backoff
