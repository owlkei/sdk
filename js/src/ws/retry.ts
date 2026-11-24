export const NORMAL = 1000;
export const LIFETIME = 4408;
export const BUSY = 503;

/** The shortest wait after a 1013 close, and before a subscribe refused with a 503 is sent again. */
export const RETRY_FLOOR_MS = 1000;

export type Plan =
  | { readonly kind: "stop" }
  | { readonly kind: "retry"; readonly scale: number; readonly floorMs: number };

const STOP: Plan = { kind: "stop" };
const BACKOFF: Plan = { kind: "retry", scale: 1, floorMs: 0 };
const LONG_BACKOFF: Plan = { kind: "retry", scale: 4, floorMs: RETRY_FLOOR_MS };
const AT_ONCE: Plan = { kind: "retry", scale: 0, floorMs: 0 };

const STOP_CODES = new Set([1000, 1002, 1003, 1007, 1008, 4401, 4403]);

export function planFor(code: number): Plan {
  if (code === 1013) return LONG_BACKOFF;
  if (code === LIFETIME) return AT_ONCE;
  if (code === 4429) return BACKOFF;
  if (STOP_CODES.has(code) || code >= 3000) return STOP;
  return BACKOFF;
}

export interface Backoff {
  readonly minMs: number;
  readonly maxMs: number;
}

export function delayFor(
  plan: Plan,
  attempt: number,
  backoff: Backoff,
  random: () => number,
): number {
  if (plan.kind === "stop") return Infinity;
  if (plan.scale === 0) return Math.floor(random() * backoff.minMs);
  const ceiling = Math.min(backoff.maxMs, backoff.minMs * plan.scale * 2 ** attempt);
  const floor = plan.floorMs;
  return floor + Math.floor(random() * Math.max(0, ceiling - floor));
}

/** The wait before a subscribe refused with a 503 is sent again: `retryAfter` seconds, else 1 s. */
export function subscribeRetryMs(retryAfter: number | undefined): number {
  return retryAfter === undefined ? RETRY_FLOOR_MS : retryAfter * 1000;
}
