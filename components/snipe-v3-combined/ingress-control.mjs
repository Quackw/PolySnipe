// Reports lost redundancy without stopping the healthy input, sender or owner.
// No reconnect, queue mutation, REST, notification send or replay lives here.
export function createIngressControl({
  mono = () => performance.now(),
  wall = Date.now,
  sink = () => {}
} = {}) {
  let previous = null,
    changed = null,
    unprotectedSince = null,
    stoppedSince = null;
  function sample({ A = null, B = null, BChannel = 'OFF' } = {}) {
    const now = mono(),
      finite = (x) => typeof x === 'number' && Number.isFinite(x) && x >= 0;
    const a =
      !!A &&
      A.phase === 'OPEN' &&
      finite(A.last_valid_activity_age_ms) &&
      A.last_valid_activity_age_ms < 8000;
    const roundtrip = B?.roundtrip_ms ?? 0;
    const b =
      !!B &&
      BChannel === 'B_AUTHENTICATED' &&
      B.state === 'FRESH' &&
      B.phase === 'OPEN' &&
      finite(roundtrip) &&
      roundtrip <= 2000 &&
      finite(B.sample_age_ms) &&
      B.sample_age_ms <= 7000 &&
      finite(B.last_valid_activity_age_ms) &&
      B.last_valid_activity_age_ms + B.sample_age_ms + roundtrip < 8000;
    const state =
      a && b ? 'TWO_INPUTS_HEALTHY' : a ? 'ONLY_A_HEALTHY' : b ? 'ONLY_B_HEALTHY' : 'NO_HEALTHY_INPUT';
    if (!a || !b) unprotectedSince ??= now;
    else unprotectedSince = null;
    if (!a && !b) stoppedSince ??= now;
    else stoppedSince = null;
    const journal =
      B?.journal_ok === false ? 'DEGRADED' : B?.journal_ok === true && B.state === 'FRESH' ? 'OK' : 'UNKNOWN';
    const identity = state + '|' + journal;
    if (identity !== previous) {
      previous = identity;
      changed = now;
      try {
        sink({
          kind: 'RTDS_INGRESS_STATE',
          state,
          journal,
          alarm: state !== 'TWO_INPUTS_HEALTHY' || journal === 'DEGRADED',
          wall_ms: wall()
        });
      } catch {}
    }
    return {
      state,
      alarm: state !== 'TWO_INPUTS_HEALTHY' || journal === 'DEGRADED',
      A_healthy: a,
      B_healthy: b,
      B_evidence: B?.state ?? 'UNKNOWN',
      B_journal: journal,
      changed_at_ms_ago: now - changed,
      redundancy_lost_ms: unprotectedSince === null ? 0 : Math.max(0, now - unprotectedSince),
      both_inputs_unhealthy_ms: stoppedSince === null ? 0 : Math.max(0, now - stoppedSince),
      delivery_authorized: false
    };
  }
  return { sample };
}
