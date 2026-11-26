function validRetryAfter(value) {
  return Number.isFinite(value) && value > 0 ? value : 0;
}

// This gate only admits attempts from the normal queue. UNKNOWN remains terminal.
export function createTelegramCircuit({
  now = Date.now,
  failureThreshold = 3,
  baseCooldownMs = 5000,
  maxCooldownMs = 60_000
} = {}) {
  if (
    ![failureThreshold, baseCooldownMs, maxCooldownMs].every(
      (value) => Number.isSafeInteger(value) && value > 0
    )
  ) {
    throw new RangeError('Telegram circuit thresholds must be positive integers');
  }

  let failures = 0;
  let consecutiveUnknown = 0;
  let totalUnknown = 0;
  let until = 0;
  let probeInFlight = false;

  function snapshot() {
    const remaining = Math.max(0, until - now());
    let state = failures > 0 ? 'DEGRADED' : 'READY';
    if (until > 0) state = remaining > 0 ? 'COOLDOWN' : 'HALF_OPEN';
    return {
      state,
      consecutive_unknown: consecutiveUnknown,
      consecutive_failures: failures,
      total_unknown: totalUnknown,
      cooldown_remaining_ms: remaining,
      unknown_replay: false
    };
  }

  function ready() {
    if (now() < until || probeInFlight) return false;
    if (until > 0) probeInFlight = true;
    return true;
  }

  function releaseProbe() {
    probeInFlight = false;
  }

  function observe(result) {
    if (!result || typeof result !== 'object') return snapshot();
    const { kind } = result;
    const rateLimited = result.status === 429 || result.error_code === 429;

    if (rateLimited || kind === 'AMBIGUOUS' || kind === 'RETRYABLE') {
      releaseProbe();
      failures += 1;
      if (kind === 'AMBIGUOUS') {
        totalUnknown += 1;
        consecutiveUnknown += 1;
      } else {
        consecutiveUnknown = 0;
      }

      const retryAfterMs = Math.max(
        validRetryAfter(result.retryAfterMs),
        validRetryAfter(result.retry_after_ms)
      );
      if (rateLimited || retryAfterMs > 0 || until > 0 || failures >= failureThreshold) {
        const backoffMs = Math.min(
          maxCooldownMs,
          baseCooldownMs * 2 ** Math.max(0, failures - failureThreshold)
        );
        // The local backoff cap still allows a longer pause directed by Telegram.
        until = Math.max(until, now() + Math.max(backoffMs, retryAfterMs));
      }
    } else if (kind === 'OK' || kind === 'FINAL') {
      releaseProbe();
      consecutiveUnknown = 0;
      // A confirmed rejection shows Telegram is reachable. An active pause still applies.
      if (now() >= until) {
        failures = 0;
        until = 0;
      }
    }

    return snapshot();
  }

  return Object.freeze({ ready, observe, snapshot, releaseProbe });
}
