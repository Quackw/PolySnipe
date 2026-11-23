// B owns its RTDS subscription independently of any one FAST bridge session.
import { RtdsPeer } from '../snipe-rtds-resilient/connection.mjs';
import { canonicalTrade, normalizeAddress } from '../snipe-fast-isolated/identity.mjs';
import { eventEvidence, scopeEvidence, validationCode } from './b-diagnostics.mjs';

const CHANNEL_DEADLINE_MS = 10000;
const RETRY_MIN_MS = 250;
const RETRY_MAX_MS = 4000;
const safeReason = (reason, fallback) => typeof reason === 'string' && /^[A-Z][A-Z0-9_]{0,95}$/.test(reason) ? reason : fallback;

export function createRemoteB({ scope, ssh, capture = null, socketFactory, mono, wall = Date.now,
  Peer = RtdsPeer, onState = () => {}, onDiagnostic = () => {}, continuous = false }) {
  if (!Array.isArray(scope) || !scope.length || typeof ssh?.start !== 'function' ||
      typeof socketFactory !== 'function' || typeof mono !== 'function' || typeof Peer !== 'function')
    throw Error('REMOTE_B_CONFIGURATION_INVALID');
  let wallets = new Set(scope.map(pair => pair.wallet));
  if ([...wallets].some(wallet => normalizeAddress(wallet) !== wallet)) throw Error('REMOTE_B_SCOPE_INVALID');

  let peer, opened = null, state = 'PREPARED', attempt = null, retryAt = 0, delay = RETRY_MIN_MS;
  let gaps = 0, matched = 0, attention = 0, stale = 0, disconnects = 0, diagnosticErrors = 0;
  let scopeVersion = 0, eventSequence = 0, captureFailures = 0, captureError = null, channelReason = null;
  function diagnostic(kind, details = {}) {
    try { onDiagnostic({ kind, at_ms: wall(), mono_ms: mono(), source: 'B', ...details }); }
    catch { diagnosticErrors++; }
  }

  function channelState() {
    if (!attempt) return state === 'RUNNING' ? 'RECONNECTING' : 'NOT_STARTED';
    const phase = attempt.relay?.status().state ?? 'CONNECTING';
    return phase === 'READY' && attempt.scopeAt === null ? 'WAIT_SCOPE' : phase;
  }

  function loseChannel(candidate, reason) {
    if (state !== 'RUNNING' || attempt !== candidate) return;
    attempt = null;
    candidate?.relay?.close();
    channelReason = reason;
    retryAt = mono() + delay;
    delay = Math.min(delay * 2, RETRY_MAX_MS);
    gaps++;
    diagnostic('B_STATE', { state, reason: 'REMOTE_B_CHANNEL_LOST', channel_state: 'RECONNECTING', channel_reason: reason, gaps });
  }

  function checkChannel() {
    const candidate = attempt;
    if (!candidate) return;
    const current = candidate.relay?.status();
    if (current?.state === 'CLOSED') {
      loseChannel(candidate, safeReason(current.reason, 'BRIDGE_CLOSED'));
    } else if (mono() >= candidate.deadline && channelState() !== 'READY') {
      loseChannel(candidate, 'B_CHANNEL_SCOPE_OR_AUTH_TIMEOUT');
    }
  }

  function connectChannel() {
    const candidate = { relay: null, scopeAt: null, deadline: mono() + CHANNEL_DEADLINE_MS };
    attempt = candidate;
    try {
      candidate.relay = ssh.start({ onScope(values) {
        if (attempt !== candidate || state !== 'RUNNING') throw Error('B_OBSOLETE_CHANNEL_SCOPE');
        if (!Array.isArray(values) || values.some(wallet => normalizeAddress(wallet) !== wallet))
          throw Error('REMOTE_B_SCOPE_INVALID');
        wallets = new Set(values);
        scopeVersion++;
        diagnostic('B_SCOPE', { scope_version: scopeVersion, ...scopeEvidence(wallets) });
        if (candidate.scopeAt === null) {
          candidate.scopeAt = mono();
          delay = RETRY_MIN_MS;
          channelReason = null;
          diagnostic('B_STATE', { state, channel_state: 'READY', reason: 'B_CHANNEL_SCOPE_CONFIRMED' });
        }
      } });
      checkChannel();
    } catch (error) { loseChannel(candidate, safeReason(error?.message, 'B_CHANNEL_START_FAILED')); }
  }

  function storageFailure(evidence, error) {
    captureFailures++;
    captureError = safeReason(error?.message, 'B_CAPTURE_STORAGE_ERROR');
    diagnostic('B_CAPTURE_RESULT', { ...evidence, state: 'CAPTURE_INCOMPLETE', reason: captureError, delivery_authorized: false });
  }

  function onActivity(envelope, context = {}) {
    const at = wall();
    const evidence = { ...eventEvidence(envelope, context), scope_version: scopeVersion,
      event_seq: ++eventSequence, received_wall_ms: context.received_wall_ms ?? at };
    if (state !== 'RUNNING' || opened === null) {
      diagnostic('B_EVENT_DROPPED', { ...evidence, reason: 'SOURCE_NOT_OPEN' });
      return;
    }
    const payload = envelope?.payload;
    const wallet = normalizeAddress(payload?.proxyWallet);
    if (!wallets.has(wallet)) {
      diagnostic('B_EVENT_DROPPED', { ...evidence, reason: wallet ? 'OUTSIDE_ACTIVE_SCOPE' : 'INVALID_PROXY_WALLET' });
      return;
    }
    const boundary = Math.max(0, opened - 2000);
    if (!canonicalTrade(payload, wallet, boundary, at)) {
      stale++;
      const valid = canonicalTrade(payload, wallet, 0, Number.MAX_SAFE_INTEGER);
      const reason = !valid ? 'INVALID_PAYLOAD' : valid.timestamp * 1000 < boundary ? 'BEFORE_RECONNECT_BOUNDARY'
        : valid.timestamp * 1000 > at + 5000 ? 'SOURCE_TIMESTAMP_IN_FUTURE' : 'VALIDATION_REJECT';
      diagnostic('B_EVENT_DROPPED', { ...evidence, boundary_ms: boundary, validation_code: !valid ? validationCode(payload) : null, reason });
      return;
    }
    diagnostic('B_EVENT_RECEIVED', { ...evidence, boundary_ms: boundary });
    matched++;
    checkChannel();
    const candidate = attempt;
    const ready = channelState() === 'READY';
    const sendLive = ready && (context.received_mono_ms ?? mono()) >= candidate.scopeAt;
    const captureReason = sendLive ? null : ready ? 'RECEIVED_BEFORE_CHANNEL_READY' : 'FAST_CHANNEL_NOT_READY';

    let record;
    try { record = capture?.save(envelope, evidence, sendLive, captureReason); }
    catch (error) { storageFailure(evidence, error); return; }
    if (!sendLive) {
      diagnostic('B_CAPTURE_RESULT', { ...evidence, state: capture ? 'CAPTURED_ONLY' : 'CAPTURE_INCOMPLETE',
        reason: captureReason, delivery_authorized: false });
      return;
    }

    diagnostic('B_FORWARD_REQUESTED', evidence);
    function finish(result) {
      const acknowledged = ['QUEUED_OR_DEDUPLICATED', 'PROCESSED_WITH_ATTENTION'].includes(result?.status);
      const needsAttention = result?.attention === true || result?.status === 'PROCESSED_WITH_ATTENTION';
      const reason = safeReason(result?.reason, acknowledged ? null : 'BRIDGE_RESULT_UNCONFIRMED');
      if (needsAttention) attention++;
      if (capture && state === 'RUNNING') {
        const recordState = !acknowledged ? 'ACK_UNCERTAIN' : needsAttention ? 'OWNER_ATTENTION' : 'QUEUE_ACK';
        try { capture.resolve(record, recordState, reason ?? (needsAttention ? result.owner_status : null)); }
        catch (error) { storageFailure(evidence, error); }
      }
      diagnostic('B_FORWARD_RESULT', { ...evidence, queue_status: result?.status ?? 'OBSERVATION_GAP', reason,
        attention: needsAttention, owner_status: result?.owner_status ?? null, blocked_pairs: result?.blocked_pairs ?? null, delivery_authorized: false });
      if (!acknowledged) loseChannel(candidate, reason);
    }
    try { Promise.resolve(candidate.relay.enqueue(envelope)).then(finish, () => finish({ reason: 'RELAY_ENQUEUE_FAILURE' })); }
    catch { finish({ reason: 'RELAY_ENQUEUE_THROW' }); }
  }

  function stop(reason = 'REMOTE_B_STOPPED') {
    if (state === 'STOPPED') return;
    const priorChannel = channelState();
    state = 'STOPPED';
    peer?.stop(reason);
    attempt?.relay?.close();
    attempt = null;
    ssh.close?.();
    diagnostic('B_STATE', { state, reason, channel_state: priorChannel, channel_reason: channelReason, gaps, matched, attention_count: attention, stale });
    onState({ state, reason, gaps, matched, attention, stale });
  }

  async function start() {
    if (state !== 'PREPARED') throw Error('REMOTE_B_SINGLE_USE');
    state = 'RUNNING';
    diagnostic('B_SCOPE', { scope_version: scopeVersion, ...scopeEvidence(wallets) });
    try {
      peer = new Peer({ source: 'B', socketFactory, mono, wall, continuous,
        onOpen: () => { opened = wall(); },
        onDisconnect: () => { if (opened !== null) disconnects++; opened = null; },
        observeFrame: row => { if (row.decision !== 'ACTIVITY') diagnostic('B_FRAME_DECISION', row); },
        sink: ({ kind, ...row }) => { diagnostic('B_RTDS_LIFECYCLE', { ...row, lifecycle_kind: kind }); onState({ event: kind, source: 'B' }); },
        onActivity });
      peer.start();
      connectChannel();
      diagnostic('B_STATE', { state, channel_state: channelState() });
      onState({ state });
      return { state };
    } catch (error) { stop('B_START_FAILED'); throw error; }
  }

  function tick() {
    if (state !== 'RUNNING') return;
    peer.tick();
    if (peer.health?.().phase === 'STOPPED') { gaps++; stop('REMOTE_B_PEER_STOPPED'); return; }
    checkChannel();
    if (!attempt && mono() >= retryAt) connectChannel();
  }

  function captureStatus() {
    try { return { ...capture?.snapshot(), failures: captureFailures, last_error: captureError }; }
    catch { return { failures: captureFailures + 1, last_error: 'B_CAPTURE_STATUS_FAILED' }; }
  }

  return Object.freeze({ start, tick, stop, status: () => ({
    state, matched, attention, stale, gaps, disconnects, diagnostic_errors: diagnosticErrors,
    scope_version: scopeVersion, scope_wallet_count: wallets.size, channel: channelState(),
    channel_reason: channelReason, capture: captureStatus(), peer: peer?.health?.() ?? null
  }) });
}
