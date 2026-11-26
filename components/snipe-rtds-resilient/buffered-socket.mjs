// A-only payload pump. "receipt" means the existing synchronous pipeline made
// its decision, never Telegram delivery or a new send permission.
import { Worker } from 'node:worker_threads';
import { randomUUID } from 'node:crypto';
import { FRAME_FAILURES, frameMono, validateFrameLimits } from './frame-buffer.mjs';
import { safeSocketError } from '../snipe-rtds-passive-ab/diagnostics.mjs';
const RECEIPT_REASONS = new Set([
  'COMMON_PIPELINE_RETURNED',
  'OBSOLETE_CONNECTION_GENERATION',
  'FRAME_NON_TEXT',
  'FRAME_TOO_LARGE',
  'APPLICATION_CONTROL',
  'INVALID_JSON',
  'EMPTY_FRAME',
  'UNSUBSCRIBED_TOPIC',
  'UNSUBSCRIBED_TYPE',
  'INVALID_PAYLOAD_ENVELOPE',
  'PIPELINE_HANDLER_THROW',
  'PIPELINE_ASYNC_UNSUPPORTED'
]);
const FILTER_REASONS = new Set([
  'APPLICATION_CONTROL',
  'INVALID_JSON',
  'EMPTY_FRAME',
  'UNSUBSCRIBED_TOPIC',
  'UNSUBSCRIBED_TYPE',
  'INVALID_PAYLOAD_ENVELOPE'
]);
export class BufferedRtdsSocket {
  constructor(
    url,
    {
      WorkerClass = Worker,
      workerURL = new URL('./buffered-worker.mjs', import.meta.url),
      limits = {},
      onDiagnostic = () => {},
      onDisposition = () => {}
    } = {}
  ) {
    if (url !== 'wss://ws-live-data.polymarket.com/') throw Error('RTDS_TARGET_REQUIRED');
    this.id = randomUUID();
    this.listeners = new Map();
    this.readyState = 0;
    this.bufferedAmount = 0;
    this.exited = false;
    this.closed = null;
    this.termination = null;
    this.fenced = false;
    this.failed = null;
    this.pending = new Map();
    this.lastSeq = 0;
    this.workerStats = null;
    this.onDiagnostic = onDiagnostic;
    this.onDisposition = onDisposition;
    this.limits = validateFrameLimits(limits);
    this.stats = {
      observed: 0,
      accepted: 0,
      pipeline_completed: 0,
      filtered: 0,
      discarded: 0,
      unknown: 0,
      rejected: 0,
      max_pending_frames: 0,
      max_queue_wait_ms: 0,
      late_receipts: 0,
      incomplete_intervals: 0,
      disposition_errors: 0
    };
    this.worker = new WorkerClass(workerURL, {
      workerData: { url, protocol: 'A_BUFFER_V1', limits },
      execArgv: [],
      env: {},
      resourceLimits: { maxOldGenerationSizeMb: 64 }
    });
    this.worker.on('message', (row) => this.receive(row));
    this.worker.on('error', (error) => {
      this.failed = 'WORKER_ERROR';
      this.diag({
        kind: 'A_WORKER_ERROR',
        reason: 'WORKER_ERROR',
        error_class: ['Error', 'TypeError', 'RangeError', 'DOMException'].includes(error?.name)
          ? error.name
          : 'OTHER',
        error_code: error?.code === 'ERR_WORKER_OUT_OF_MEMORY' ? 'ERR_WORKER_OUT_OF_MEMORY' : 'UNAVAILABLE'
      });
    });
    this.exit = new Promise((resolve) =>
      this.worker.once('exit', (code) => {
        this.exited = true;
        this.readyState = 3;
        if (!this.closed && !this.fenced) this.failed ??= 'WORKER_EXIT';
        const reason = this.failed ?? (this.fenced ? 'SOURCE_GENERATION_FENCED' : 'WORKER_EXIT');
        this.discardPending(reason);
        if (!this.closed && !this.fenced) this.markGap(reason, true);
        if (this.failed) this.emit('error', { reason: this.failed, buffer_protocol: 'A_BUFFER_V1' });
        this.emit('close', this.closed ?? { code: 1006, reason });
        resolve(true);
      })
    );
  }
  diag(row) {
    try {
      this.onDiagnostic({ ...row, source: 'A', socket_id: this.id });
    } catch {
      this.stats.disposition_errors++;
    }
  }
  markGap(reason, tailUnknown = false) {
    this.stats.incomplete_intervals++;
    this.diag({
      kind: 'A_OBSERVATION_INCOMPLETE',
      reason,
      tail_unknown: tailUnknown,
      pending_frames: this.pending.size,
      last_native_seq: this.lastSeq
    });
  }
  disposition(meta, status, reason, queueWaitMs = null) {
    const row = {
      kind: 'A_FRAME_DISPOSITION',
      source: 'A',
      receipt_id: this.id + ':' + meta.seq,
      sequence: meta.seq,
      frame_sha256: meta.frame_sha256,
      bytes: meta.bytes,
      received_wall_ms: meta.received_wall_ms,
      status,
      reason,
      queue_wait_ms: queueWaitMs
    };
    try {
      this.onDisposition(row);
    } catch {
      this.stats.disposition_errors++;
    }
    if (status !== 'PIPELINE_COMPLETED') this.diag(row);
  }
  discardPending(reason) {
    for (const [seq, meta] of this.pending) {
      this.pending.delete(seq);
      const unknown = meta.started === true;
      this.stats[unknown ? 'unknown' : 'discarded']++;
      this.disposition(meta, unknown ? 'PIPELINE_RESULT_UNKNOWN' : 'DISCARDED', reason);
    }
  }
  fail(reason) {
    if (this.failed) return;
    this.failed = FRAME_FAILURES.has(reason) ? reason : 'WORKER_PROTOCOL_ERROR';
    this.markGap(this.failed);
    // Account for frames now. The worker may still be waiting for its native socket to close.
    this.discardPending(this.failed);
    try {
      this.emit('error', { reason: this.failed, buffer_protocol: 'A_BUFFER_V1' });
    } finally {
      this.close();
    }
  }
  receive(row) {
    if (this.exited || !row || typeof row.kind !== 'string') return;
    if (row.kind === 'open') {
      if (!this.fenced) {
        this.readyState = 1;
        this.emit('open');
      }
      return;
    }
    if (row.kind === 'frame_observed') {
      if (
        !Number.isSafeInteger(row.seq) ||
        row.seq !== this.lastSeq + 1 ||
        !Number.isSafeInteger(row.bytes) ||
        row.bytes < 0 ||
        typeof row.accepted !== 'boolean' ||
        !Number.isFinite(row.received_mono_ms) ||
        !Number.isSafeInteger(row.received_wall_ms) ||
        !(
          row.frame_sha256 === null ||
          (typeof row.frame_sha256 === 'string' && /^[a-f0-9]{64}$/.test(row.frame_sha256))
        )
      ) {
        this.fail('WORKER_PROTOCOL_ERROR');
        return;
      }
      this.lastSeq = row.seq;
      this.stats.observed++;
      const meta = {
        seq: row.seq,
        bytes: row.bytes,
        frame_sha256: row.frame_sha256,
        received_mono_ms: row.received_mono_ms,
        received_wall_ms: row.received_wall_ms
      };
      if (!row.accepted) {
        this.stats.rejected++;
        this.disposition(
          meta,
          'REJECTED',
          FRAME_FAILURES.has(row.reason) ? row.reason : 'WORKER_PROTOCOL_ERROR'
        );
        return;
      }
      this.stats.accepted++;
      if (this.fenced || this.failed) {
        this.stats.discarded++;
        this.disposition(meta, 'DISCARDED', this.failed ?? 'SOURCE_GENERATION_FENCED');
        return;
      }
      if (this.pending.size >= this.limits.maxFrames) {
        this.stats.discarded++;
        this.disposition(meta, 'DISCARDED', 'WORKER_PROTOCOL_ERROR');
        this.fail('WORKER_PROTOCOL_ERROR');
        return;
      }
      this.pending.set(row.seq, meta);
      this.stats.max_pending_frames = Math.max(this.stats.max_pending_frames, this.pending.size);
      return;
    }
    if (row.kind === 'frame') {
      const meta = this.pending.get(row.seq);
      if (!meta || this.fenced || this.failed) return;
      const age = frameMono() - meta.received_mono_ms;
      if (
        typeof row.data !== 'string' ||
        Buffer.byteLength(row.data) !== meta.bytes ||
        !Number.isFinite(age) ||
        age < 0
      ) {
        this.fail('WORKER_PROTOCOL_ERROR');
        return;
      }
      if (age > this.limits.maxWaitMs) {
        this.fail('FRAME_QUEUE_TIMEOUT');
        return;
      }
      this.stats.max_queue_wait_ms = Math.max(this.stats.max_queue_wait_ms, age);
      let settled = false;
      const complete = (status, reason) => {
        if (settled) return;
        settled = true;
        if (!this.pending.has(row.seq) || this.exited || this.fenced || this.failed) {
          this.stats.late_receipts++;
          return;
        }
        if (!['PIPELINE_COMPLETED', 'DISCARDED'].includes(status) || !RECEIPT_REASONS.has(reason)) {
          this.fail('FRAME_RECEIPT_INVALID');
          return;
        }
        this.pending.delete(row.seq);
        const unknown = ['PIPELINE_HANDLER_THROW', 'PIPELINE_ASYNC_UNSUPPORTED'].includes(reason),
          filtered = FILTER_REASONS.has(reason);
        this.stats[
          unknown
            ? 'unknown'
            : filtered
              ? 'filtered'
              : status === 'PIPELINE_COMPLETED'
                ? 'pipeline_completed'
                : 'discarded'
        ]++;
        this.disposition(
          meta,
          unknown ? 'PIPELINE_RESULT_UNKNOWN' : filtered ? 'FILTERED' : status,
          reason,
          age
        );
        this.worker.postMessage({ kind: 'receipt', seq: row.seq, status, reason });
      };
      try {
        if (!this.listeners.get('message')?.size) {
          this.fail('PIPELINE_RECEIPT_MISSING');
          return;
        }
        meta.started = true;
        this.emit('message', {
          data: row.data,
          receipt_id: this.id + ':' + row.seq,
          queue_wait_ms: age,
          complete
        });
      } catch {
        this.fail('PIPELINE_HANDLER_THROW');
      }
      return;
    }
    if (row.kind === 'receipt_stalled' || row.kind === 'receipt_recovered') {
      this.workerStats = row.stats;
      this.diag({
        kind: row.kind === 'receipt_stalled' ? 'A_RECEIPT_STALLED' : 'A_RECEIPT_RECOVERED',
        sequence: row.seq,
        wait_ms: row.wait_ms
      });
      return;
    }
    if (row.kind === 'buffer_fault' || row.kind === 'transport_fault') {
      this.workerStats = row.stats ?? this.workerStats;
      this.fail(row.reason);
      this.discardPending(this.failed ?? 'WORKER_PROTOCOL_ERROR');
      return;
    }
    if (row.kind === 'buffer_cancelled') {
      this.workerStats = row.stats ?? this.workerStats;
      this.discardPending('SOURCE_GENERATION_FENCED');
      return;
    }
    if (row.kind === 'buffer_drained' || row.kind === 'buffer_stats') {
      this.workerStats = row.stats;
      return;
    }
    if (row.kind === 'socket_close') {
      this.closed = {
        code: Number.isInteger(row.code) ? row.code : 1006,
        reason: FRAME_FAILURES.has(row.reason) ? row.reason : ''
      };
      if (FRAME_FAILURES.has(row.reason)) this.failed ??= row.reason;
      return;
    }
    if (row.kind === 'diagnostic') {
      const v = row.value ?? {},
        safe = safeSocketError({ name: v.error_class, code: v.error_code });
      this.diag({
        kind: 'A_NATIVE_SIGNAL',
        signal: ['PING', 'PONG', 'SOCKET_ERROR', 'NATIVE_SOCKET_ERROR', 'NATIVE_CLOSE_ERROR'].includes(v.kind)
          ? v.kind
          : 'OTHER',
        ...safe
      });
    }
  }
  snapshot() {
    return {
      ...this.stats,
      pending: this.pending.size,
      limits: { ...this.limits },
      worker: this.workerStats,
      worker_exited: this.exited,
      fenced: this.fenced,
      last_error: this.failed,
      accepted_accounted:
        this.stats.pipeline_completed +
          this.stats.filtered +
          this.stats.discarded +
          this.stats.unknown +
          this.pending.size ===
        this.stats.accepted
    };
  }
  addEventListener(kind, fn) {
    if (!this.listeners.has(kind)) this.listeners.set(kind, new Set());
    this.listeners.get(kind).add(fn);
  }
  removeEventListener(kind, fn) {
    this.listeners.get(kind)?.delete(fn);
  }
  emit(kind, value = {}) {
    for (const fn of [...(this.listeners.get(kind) ?? [])]) fn(value);
  }
  send(data) {
    if (this.readyState !== 1 || typeof data !== 'string' || Buffer.byteLength(data) > 16384)
      throw Error('RTDS_SEND_NOT_OPEN');
    this.worker.postMessage({ kind: 'send', data });
  }
  close() {
    if (this.exited || this.fenced) return;
    this.fenced = true;
    this.readyState = 2;
    this.discardPending(this.failed ?? 'SOURCE_GENERATION_FENCED');
    this.worker.postMessage({ kind: 'close' });
  }
  terminate() {
    if (!this.termination)
      this.termination = (async () => {
        if (!this.exited) await this.worker.terminate();
        await this.exit;
        return this.exited;
      })();
    return this.termination;
  }
}
