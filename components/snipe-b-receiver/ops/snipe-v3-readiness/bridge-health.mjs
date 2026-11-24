// Health is evidence only. It cannot submit an event, acknowledge a send or
// bypass the queue owner's independent 2000ms transactional state fence.
import { createHmac, randomBytes, timingSafeEqual } from 'node:crypto';
export const HEALTH_LIMIT_MS = 2000,
  HEALTH_PERIOD_MS = 5000;
const hex = (x) => typeof x === 'string' && /^[a-f0-9]{64}$/.test(x);
const phases = new Set([
  'PREPARED',
  'CONNECTING',
  'OPEN',
  'WAIT_CLOSE',
  'WAIT_ABORT',
  'BACKOFF',
  'COOLDOWN',
  'STOPPED'
]);
const signature = (key, value) => createHmac('sha256', key).update(JSON.stringify(value)).digest('hex');
const verify = (key, value, mac) =>
  hex(mac) && timingSafeEqual(Buffer.from(signature(key, value), 'hex'), Buffer.from(mac, 'hex'));
const identity = (c) => [c.runId, c.scopeFingerprint, c.nonce];
const fields = (h) => [
  h.phase,
  h.generation,
  h.activity_frames,
  h.last_valid_activity_age_ms,
  h.stop_reason,
  h.journal_ok
];
export function safePeerHealth(h) {
  const reason = h?.stop_reason ?? null,
    age = h?.last_valid_activity_age_ms ?? null;
  if (
    !phases.has(h?.phase) ||
    !Number.isSafeInteger(h.generation) ||
    h.generation < 0 ||
    !Number.isSafeInteger(h.activity_frames) ||
    h.activity_frames < 0 ||
    (age !== null && (!Number.isFinite(age) || age < 0)) ||
    (reason !== null && !(typeof reason === 'string' && /^[A-Z][A-Z0-9_]{0,95}$/.test(reason))) ||
    typeof h.journal_ok !== 'boolean'
  )
    throw Error('BRIDGE_HEALTH_INVALID');
  return {
    phase: h.phase,
    generation: h.generation,
    activity_frames: h.activity_frames,
    last_valid_activity_age_ms: age,
    stop_reason: reason,
    journal_ok: h.journal_ok
  };
}
export function createHealthOwner(config, mono = () => performance.now()) {
  const c = { ...config, key: Buffer.from(config.key) };
  let sequence = 0,
    pending = null,
    last = null,
    closed = false;
  function challenge() {
    if (closed) throw Error('BRIDGE_HEALTH_CLOSED');
    const at = mono();
    if (!Number.isFinite(at)) throw Error('BRIDGE_HEALTH_CLOCK');
    const nonce = randomBytes(32).toString('hex');
    pending = { sequence: ++sequence, nonce, at };
    return {
      kind: 'HEALTH_CHALLENGE',
      sequence,
      nonce,
      mac: signature(c.key, ['B_HEALTH_CHALLENGE_V1', ...identity(c), sequence, nonce])
    };
  }
  function receive(frame) {
    const p = pending,
      age = p ? mono() - p.at : NaN;
    if (
      closed ||
      !p ||
      frame?.kind !== 'HEALTH_REPLY' ||
      frame.sequence !== p.sequence ||
      frame.nonce !== p.nonce ||
      !Number.isFinite(age) ||
      age < 0 ||
      age > HEALTH_LIMIT_MS
    )
      throw Error('BRIDGE_HEALTH_STALE_OR_REPLAY');
    const health = safePeerHealth(frame.health);
    if (
      !verify(c.key, ['B_HEALTH_REPLY_V1', ...identity(c), p.sequence, p.nonce, ...fields(health)], frame.mac)
    )
      throw Error('BRIDGE_HEALTH_MAC_INVALID');
    pending = null;
    last = { ...health, received_mono_ms: mono(), roundtrip_ms: age };
    return snapshot();
  }
  function snapshot() {
    const age = last ? mono() - last.received_mono_ms : NaN;
    return last && Number.isFinite(age) && age >= 0 && age <= HEALTH_PERIOD_MS + HEALTH_LIMIT_MS
      ? { ...last, state: 'FRESH', sample_age_ms: age }
      : { state: 'UNKNOWN', reason: 'B_HEALTH_NOT_FRESH' };
  }
  return {
    challenge,
    receive,
    snapshot,
    close() {
      closed = true;
      pending = null;
      last = null;
      c.key.fill(0);
    }
  };
}
export function createHealthPeer(config) {
  const c = { ...config, key: Buffer.from(config.key) };
  let sequence = 0,
    closed = false;
  return {
    answer(frame, health) {
      if (
        closed ||
        frame?.kind !== 'HEALTH_CHALLENGE' ||
        !Number.isSafeInteger(frame.sequence) ||
        frame.sequence !== sequence + 1 ||
        !hex(frame.nonce) ||
        !verify(c.key, ['B_HEALTH_CHALLENGE_V1', ...identity(c), frame.sequence, frame.nonce], frame.mac)
      )
        throw Error('BRIDGE_HEALTH_CHALLENGE_INVALID');
      const value = safePeerHealth(health);
      sequence = frame.sequence;
      return {
        kind: 'HEALTH_REPLY',
        sequence,
        nonce: frame.nonce,
        health: value,
        mac: signature(c.key, ['B_HEALTH_REPLY_V1', ...identity(c), sequence, frame.nonce, ...fields(value)])
      };
    },
    close() {
      closed = true;
      c.key.fill(0);
    }
  };
}
