// Adapter into the existing FAST writer/handler. It creates no queue, sender,
// connection or historical replay. Results are queue decisions, never delivery.
import { randomUUID } from 'node:crypto';
export function createFastBridgeOwner({ scopeFingerprint, isReady, dispatch, now = Date.now }) {
  if (
    !/^[a-f0-9]{64}$/.test(scopeFingerprint ?? '') ||
    typeof isReady !== 'function' ||
    typeof dispatch !== 'function'
  )
    throw Error('FAST_BRIDGE_OWNER_CONFIGURATION_INVALID');
  let session = null,
    last = null,
    generation = 0;
  function openSource(source) {
    if (source !== 'B' || session || isReady() !== true) throw Error('FAST_BRIDGE_OWNER_NOT_READY');
    const at = now();
    if (!Number.isSafeInteger(at) || at <= 0) throw Error('FAST_BRIDGE_CLOCK_INVALID');
    session = {
      source: 'B',
      session_id: randomUUID(),
      generation: ++generation,
      scope_fingerprint: scopeFingerprint,
      opened_wall_ms: at
    };
    last = null;
    return { ...session, status: 'CONNECTED' };
  }
  function disconnectSource(candidate) {
    if (session && candidate?.session_id === session.session_id) {
      session = null;
      last = null;
    }
  }
  function submit(packet) {
    if (!session || packet?.session_id !== session.session_id || packet.generation !== session.generation)
      return { status: 'BLOCKED', reason: 'SOURCE_DISCONNECTED_OR_OBSOLETE' };
    if (isReady() !== true) return { status: 'BLOCKED', reason: 'FROZEN_SCOPE_CHANGED' };
    if (last && packet.sequence === last.sequence && packet.message_id === last.message_id)
      return { status: 'ALREADY_COMMITTED', results: last.results };
    if (
      !Number.isSafeInteger(packet.sequence) ||
      packet.sequence !== (last?.sequence ?? 0) + 1 ||
      typeof packet.message_id !== 'string'
    )
      return { status: 'BLOCKED', reason: 'IDENTITY_CONFLICT' };
    let admissions;
    try {
      admissions = dispatch(packet.envelope, session.opened_wall_ms);
    } catch {
      return { status: 'BLOCKED', reason: 'PERSISTENCE_FAILURE' };
    }
    if (!Array.isArray(admissions)) return { status: 'BLOCKED', reason: 'PERSISTENCE_FAILURE' };
    if (
      admissions.some(
        (x) =>
          x?.kind === 'BLOCKED' &&
          ![
            'SUBSCRIPTION_INACTIVE',
            'BEFORE_EXISTING_PROTECTION_BOUNDARY',
            'UNMAPPED_LEGACY_DELIVERY'
          ].includes(x.reason)
      )
    )
      return { status: 'BLOCKED', reason: 'ADMISSIONS_HALTED' };
    const results = admissions.map((x) => ({
      status: x.kind === 'BLOCKED' || x.ambiguous === true ? 'BLOCKED' : 'OK'
    }));
    const status = !admissions.length
      ? 'REJECTED'
      : admissions.some((x) => x.kind === 'SEND_FIRST_ACTION')
        ? 'ACCEPTED'
        : admissions.some((x) => x.kind === 'SUPPRESS_SAME_ACTION')
          ? 'DUPLICATE'
          : 'BLOCKED';
    last = { sequence: packet.sequence, message_id: packet.message_id, results };
    return { status, results };
  }
  return Object.freeze({ openSource, disconnectSource, submit });
}
