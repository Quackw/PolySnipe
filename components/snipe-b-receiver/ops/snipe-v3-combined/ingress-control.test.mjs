import test from 'node:test';
import assert from 'node:assert/strict';
import { createIngressControl } from './ingress-control.mjs';
import { checkIngressHealth } from './check-ingress-health.mjs';
const A = { phase: 'OPEN', last_valid_activity_age_ms: 0 },
  B = { phase: 'OPEN', state: 'FRESH', last_valid_activity_age_ms: 0, sample_age_ms: 0, journal_ok: true };
test('authenticated SSH without fresh RTDS evidence is degraded, not two healthy inputs', () => {
  let t = 0;
  const rows = [],
    c = createIngressControl({ mono: () => t, wall: () => t, sink: (r) => rows.push(r) });
  assert.equal(c.sample({ A, BChannel: 'B_AUTHENTICATED' }).state, 'ONLY_A_HEALTHY');
  assert.equal(c.sample({ A, B, BChannel: 'B_AUTHENTICATED' }).state, 'TWO_INPUTS_HEALTHY');
  t = 1000;
  assert.equal(
    c.sample({ A: { ...A, phase: 'STOPPED' }, B, BChannel: 'B_AUTHENTICATED' }).state,
    'ONLY_B_HEALTHY'
  );
  t = 6000;
  assert.equal(
    c.sample({ A: { ...A, phase: 'STOPPED' }, B, BChannel: 'B_AUTHENTICATED' }).redundancy_lost_ms,
    5000
  );
  assert.equal(
    c.sample({ A: { ...A, phase: 'STOPPED' }, B: { state: 'UNKNOWN' }, BChannel: 'B_AUTHENTICATED' }).state,
    'NO_HEALTHY_INPUT'
  );
  assert.ok(rows.at(-1).alarm);
  const n = rows.length;
  c.sample({});
  assert.equal(rows.length, n);
  assert.equal(c.sample({}).delivery_authorized, false);
});
test('old data, stale samples and journal loss remain explicit while healthy A continues', () => {
  const c = createIngressControl();
  for (const broken of [
    { ...B, last_valid_activity_age_ms: 7999, sample_age_ms: 1 },
    { ...B, state: 'UNKNOWN' },
    { ...B, phase: 'STOPPED' }
  ])
    assert.equal(c.sample({ A, B: broken, BChannel: 'B_AUTHENTICATED' }).state, 'ONLY_A_HEALTHY');
  const h = c.sample({ A, B: { ...B, journal_ok: false }, BChannel: 'B_AUTHENTICATED' });
  assert.equal(h.state, 'TWO_INPUTS_HEALTHY');
  assert.equal(h.alarm, true);
  assert.equal(h.B_journal, 'DEGRADED');
});
test('read-only checker rejects stale status and historical enabled-only false green health', () => {
  const base = { at_ms: 1000, enabled: true };
  assert.equal(checkIngressHealth(base, { mode: 'FAST', now: 1000 }).alarm, true);
  const healthy = {
    ...base,
    rtds_ingress: { state: 'TWO_INPUTS_HEALTHY', alarm: false },
    rtds_peers: { A, B: { channel: 'B_AUTHENTICATED', health: B } }
  };
  assert.equal(checkIngressHealth(healthy, { mode: 'FAST', now: 1000 }).alarm, false);
  assert.equal(checkIngressHealth(healthy, { mode: 'FAST', now: 11001 }).alarm, true);
  assert.equal(checkIngressHealth(healthy, { mode: 'FAST', now: 8500 }).alarm, true);
  const b = {
    at_ms: 1000,
    state: 'RUNNING',
    channel: 'READY',
    peer: { phase: 'OPEN', last_valid_activity_age_ms: 0 },
    diagnostics: {
      started: true,
      disabled: false,
      errors: 0,
      dropped: 0,
      protected_retention_ms: 48 * 3600000
    }
  };
  assert.equal(checkIngressHealth(b, { mode: 'B', now: 1000 }).alarm, false);
  assert.equal(checkIngressHealth(b, { mode: 'B', now: 9000 }).alarm, true);
  b.diagnostics.disabled = true;
  assert.equal(checkIngressHealth(b, { mode: 'B', now: 1000 }).alarm, true);
});
test('B liveness conservatively includes the measured roundtrip instead of assuming zero return transit', () => {
  const c = createIngressControl();
  assert.equal(
    c.sample({
      A,
      B: { ...B, last_valid_activity_age_ms: 6000, roundtrip_ms: 2000 },
      BChannel: 'B_AUTHENTICATED'
    }).state,
    'ONLY_A_HEALTHY'
  );
  assert.equal(
    c.sample({
      A,
      B: { ...B, last_valid_activity_age_ms: 5999, roundtrip_ms: 2000 },
      BChannel: 'B_AUTHENTICATED'
    }).state,
    'TWO_INPUTS_HEALTHY'
  );
});
