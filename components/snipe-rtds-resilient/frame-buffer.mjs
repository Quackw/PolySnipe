// A-only receipt window plus a separate bounded native-frame queue. No socket,
// sender, wallet policy or identity authority lives in this state machine.
import { createHash } from 'node:crypto';
// A receipt warning never releases or retransmits a frame. Its hard deadline
// is bounded independently from delivery-state freshness; overload stays explicit.
// Input residence is separate from the unchanged 2000ms authoritative state
// budget. A bounded burst may wait here; each later admission reads fresh state.
export const FRAME_BUFFER_DEFAULTS = Object.freeze({
  maxBytes: 2 * 1024 ** 2,
  maxFrames: 4096,
  maxFrameBytes: 262144,
  maxWaitMs: 16000,
  ackTimeoutMs: 8000,
  ackWarningMs: 2000
});
export const FRAME_FAILURES = new Set([
  'FRAME_NON_TEXT',
  'FRAME_TOO_LARGE',
  'FRAME_QUEUE_BYTES',
  'FRAME_QUEUE_COUNT',
  'FRAME_QUEUE_TIMEOUT',
  'FRAME_RECEIPT_TIMEOUT',
  'FRAME_RECEIPT_INVALID',
  'PIPELINE_HANDLER_THROW',
  'PIPELINE_RECEIPT_MISSING',
  'PIPELINE_ASYNC_UNSUPPORTED',
  'WORKER_PROTOCOL_ERROR',
  'WORKER_ERROR',
  'WORKER_EXIT',
  'NATIVE_SOCKET_ERROR',
  'NATIVE_SEND_ERROR',
  'SOCKET_CONSTRUCT_ERROR'
]);
export const frameMono = () => Number(process.hrtime.bigint()) / 1e6;
export function validateFrameLimits(limits = {}) {
  const cfg = { ...FRAME_BUFFER_DEFAULTS, ...limits };
  for (const [key, n] of Object.entries(cfg))
    if (
      !Object.hasOwn(FRAME_BUFFER_DEFAULTS, key) ||
      !Number.isSafeInteger(n) ||
      n < 1 ||
      n > FRAME_BUFFER_DEFAULTS[key]
    )
      throw Error('FRAME_BUFFER_LIMIT_INVALID');
  if (cfg.maxFrameBytes > cfg.maxBytes) throw Error('FRAME_BUFFER_CONFIGURATION');
  return cfg;
}
export function createFrameBuffer({
  post,
  onFault = () => {},
  onDrained = () => {},
  mono = frameMono,
  wall = Date.now,
  limits = {}
} = {}) {
  const cfg = validateFrameLimits(limits);
  if (typeof post !== 'function') throw Error('FRAME_BUFFER_CONFIGURATION');
  let seq = 0,
    queue = [],
    head = 0,
    inflight = null,
    bytes = 0,
    ended = false,
    failed = false,
    drained = false,
    tail = 0,
    tailFirst = null;
  const stats = {
    native_frames: 0,
    accepted: 0,
    rejected: 0,
    receipted: 0,
    max_buffer_bytes: 0,
    max_buffer_frames: 0,
    max_queue_wait_ms: 0,
    max_observed_wait_ms: 0,
    max_receipt_ms: 0,
    receipt_stalls: 0,
    receipt_recoveries: 0,
    faults: 0
  };
  const pending = () => queue.length - head + (inflight ? 1 : 0);
  const publish = (value) => post(value);
  function release() {
    if (inflight) {
      bytes -= inflight.meta.bytes;
      inflight = null;
    }
  }
  function compact() {
    if (head > 256 && head * 2 >= queue.length) {
      queue = queue.slice(head);
      head = 0;
    }
  }
  function finish() {
    if (ended && !failed && !pending() && !drained) {
      drained = true;
      publish({ kind: 'buffer_drained', stats: snapshot() });
      onDrained();
    }
  }
  function fail(reason) {
    if (failed) return;
    failed = true;
    ended = true;
    stats.faults++;
    // Main owns whether an in-flight callback already completed. Do not declare
    // it undelivered here: a receipt can be in transit while the fault is posted.
    publish({
      kind: 'buffer_fault',
      reason,
      first_pending: inflight?.meta.seq ?? queue[head]?.meta.seq ?? null,
      last_announced: seq,
      stats: snapshot()
    });
    queue = [];
    head = 0;
    inflight = null;
    bytes = 0;
    onFault(reason);
  }
  function metadata(data, n) {
    return {
      seq: n,
      bytes: typeof data === 'string' ? Buffer.byteLength(data) : 0,
      received_mono_ms: mono(),
      received_wall_ms: wall(),
      frame_sha256:
        typeof data === 'string' && Buffer.byteLength(data) <= cfg.maxFrameBytes
          ? createHash('sha256').update(data).digest('hex')
          : null
    };
  }
  function accept(data) {
    stats.native_frames++;
    const n = ++seq;
    if (ended || failed) {
      tail++;
      tailFirst ??= n;
      return false;
    }
    const meta = metadata(data, n);
    let reason = null;
    if (typeof data !== 'string') reason = 'FRAME_NON_TEXT';
    else if (meta.bytes > cfg.maxFrameBytes) reason = 'FRAME_TOO_LARGE';
    else if (bytes + meta.bytes > cfg.maxBytes) reason = 'FRAME_QUEUE_BYTES';
    else if (pending() + 1 > cfg.maxFrames) reason = 'FRAME_QUEUE_COUNT';
    publish({ kind: 'frame_observed', ...meta, accepted: reason === null, ...(reason ? { reason } : {}) });
    if (reason) {
      stats.rejected++;
      fail(reason);
      return false;
    }
    queue.push({ meta, data });
    bytes += meta.bytes;
    stats.accepted++;
    stats.max_buffer_bytes = Math.max(stats.max_buffer_bytes, bytes);
    stats.max_buffer_frames = Math.max(stats.max_buffer_frames, pending());
    pump();
    return true;
  }
  function pump() {
    if (failed || inflight) return;
    const item = queue[head];
    if (!item) {
      finish();
      return;
    }
    const age = mono() - item.meta.received_mono_ms;
    stats.max_observed_wait_ms = Math.max(stats.max_observed_wait_ms, age);
    if (!Number.isFinite(age) || age < 0 || age > cfg.maxWaitMs) {
      fail('FRAME_QUEUE_TIMEOUT');
      return;
    }
    queue[head] = null;
    head++;
    compact();
    inflight = { ...item, dispatched_mono_ms: mono() };
    stats.max_queue_wait_ms = Math.max(stats.max_queue_wait_ms, age);
    // Only one payload can be outstanding on MessagePort. The rest stay in the
    // byte/count-bounded worker queue instead of filling the main event loop.
    publish({ kind: 'frame', ...item.meta, queue_wait_ms: age, data: item.data });
  }
  function acknowledge(frame) {
    if (failed) return false;
    if (
      !inflight ||
      frame?.seq !== inflight.meta.seq ||
      !['PIPELINE_COMPLETED', 'DISCARDED'].includes(frame.status) ||
      typeof frame.reason !== 'string' ||
      !/^[A-Z][A-Z0-9_]{0,95}$/.test(frame.reason)
    ) {
      fail('FRAME_RECEIPT_INVALID');
      return false;
    }
    const age = mono() - inflight.dispatched_mono_ms;
    // Enforce the hard bound even if the receipt arrives before the next tick.
    if (!Number.isFinite(age) || age < 0 || age > cfg.ackTimeoutMs) {
      fail('FRAME_RECEIPT_TIMEOUT');
      return false;
    }
    observeReceiptWait(age);
    stats.max_receipt_ms = Math.max(stats.max_receipt_ms, age);
    if (inflight.warned) {
      stats.receipt_recoveries++;
      publish({ kind: 'receipt_recovered', seq: inflight.meta.seq, wait_ms: age, stats: snapshot() });
    }
    stats.receipted++;
    release();
    pump();
    return true;
  }
  function observeReceiptWait(age) {
    if (age > cfg.ackWarningMs && !inflight.warned) {
      inflight.warned = true;
      stats.receipt_stalls++;
      publish({ kind: 'receipt_stalled', seq: inflight.meta.seq, wait_ms: age, stats: snapshot() });
    }
  }
  function tick() {
    if (failed) return;
    if (queue[head])
      stats.max_observed_wait_ms = Math.max(
        stats.max_observed_wait_ms,
        mono() - queue[head].meta.received_mono_ms
      );
    if (inflight) {
      const age = mono() - inflight.dispatched_mono_ms;
      if (age > cfg.ackTimeoutMs) {
        fail('FRAME_RECEIPT_TIMEOUT');
        return;
      }
      observeReceiptWait(age);
    }
    if (queue[head] && mono() - queue[head].meta.received_mono_ms > cfg.maxWaitMs) {
      fail('FRAME_QUEUE_TIMEOUT');
      return;
    }
    pump();
  }
  function end() {
    if (failed) return;
    ended = true;
    pump();
    finish();
  }
  function cancel(reason = 'SOURCE_GENERATION_FENCED') {
    if (!failed) {
      ended = true;
      publish({
        kind: 'buffer_cancelled',
        reason,
        first_pending: inflight?.meta.seq ?? queue[head]?.meta.seq ?? null,
        last_announced: seq,
        stats: snapshot()
      });
      queue = [];
      head = 0;
      inflight = null;
      bytes = 0;
    }
  }
  function snapshot() {
    return {
      ...stats,
      pending: pending(),
      buffer_bytes: bytes,
      limits: { ...cfg },
      ended,
      failed,
      tail_after_fence: tail,
      tail_first_seq: tailFirst,
      last_native_seq: seq
    };
  }
  return Object.freeze({ accept, acknowledge, pump, tick, end, cancel, fail, snapshot });
}
