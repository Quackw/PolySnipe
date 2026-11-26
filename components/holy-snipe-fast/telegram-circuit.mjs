// One sender owns all attempts. UNKNOWN is terminal; a probe is a different queued action.
export function createTelegramCircuit({ now = Date.now } = {}) {
  let failures = 0,
    until = 0,
    totalUnknown = 0;
  return Object.freeze({
    ready: () => now() >= until,
    observe(result) {
      if (result.kind === 'OK') {
        failures = 0;
        until = 0;
      } else if (result.kind === 'AMBIGUOUS') {
        totalUnknown++;
        failures++;
        if (failures >= 3) until = now() + Math.min(60000, 5000 * 2 ** Math.min(4, failures - 3));
      }
      return this.snapshot();
    },
    snapshot: () => ({
      state: now() < until ? 'COOLDOWN' : failures ? 'DEGRADED' : 'READY',
      consecutive_unknown: failures,
      total_unknown: totalUnknown,
      cooldown_remaining_ms: Math.max(0, until - now()),
      unknown_replay: false
    })
  });
}
