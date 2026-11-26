// Bounded start-to-start rate for this process only. It does not claim to know
// the shared egress allowance. The caller must still fail closed on first 429.
import { performance } from 'node:perf_hooks';
export function createRedeemPacer({
  mono = () => performance.now(),
  wait = (ms) => new Promise((r) => setTimeout(r, ms)),
  intervalMs = 3100,
  initialWaitMs = 0
} = {}) {
  if (
    typeof mono !== 'function' ||
    typeof wait !== 'function' ||
    !Number.isSafeInteger(intervalMs) ||
    intervalMs < 3100 ||
    !Number.isSafeInteger(initialWaitMs) ||
    initialWaitMs < 0 ||
    initialWaitMs > intervalMs + 3000
  )
    throw Error('REDEEM_PACER_INVALID');
  const created = mono();
  if (!Number.isFinite(created) || created < 0) throw Error('REDEEM_PACER_CLOCK_INVALID');
  let next = initialWaitMs ? created + initialWaitMs : null,
    busy = false,
    last = created;
  async function take() {
    if (busy) throw Error('REDEEM_PACER_BUSY');
    busy = true;
    try {
      const before = mono();
      if (!Number.isFinite(before) || before < 0 || (last !== null && before < last))
        throw Error('REDEEM_PACER_CLOCK_INVALID');
      // Node timers may wake a fraction of a millisecond before a floating-point
      // monotonic deadline. Wait out that remainder; never dispatch early or
      // mistake a normal early wake for a backwards clock.
      let observed = before,
        attempts = 0;
      while (next !== null && observed < next) {
        if (++attempts > 8) throw Error('REDEEM_PACER_CLOCK_INVALID');
        await wait(Math.ceil(next - observed));
        const after = mono();
        if (!Number.isFinite(after) || after <= observed) throw Error('REDEEM_PACER_CLOCK_INVALID');
        observed = after;
      }
      const start = mono();
      if (!Number.isFinite(start) || start < observed || (next !== null && start < next))
        throw Error('REDEEM_PACER_CLOCK_INVALID');
      last = start;
      next = start + intervalMs;
      return { start_ms: start };
    } finally {
      busy = false;
    }
  }
  function markStart() {
    const actual = mono();
    if (!Number.isFinite(actual) || actual < 0 || (last !== null && actual < last))
      throw Error('REDEEM_PACER_CLOCK_INVALID');
    last = actual;
    next = actual + intervalMs;
  }
  return Object.freeze({ take, markStart, interval_ms: intervalMs });
}
