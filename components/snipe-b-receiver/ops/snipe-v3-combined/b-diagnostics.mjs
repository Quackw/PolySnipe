// Payload-free B evidence and a bounded asynchronous local journal. Inert until
// start(); no sockets, HTTP, database handles, senders or replay path.
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { createHash, randomUUID } from 'node:crypto';
import {
  canonicalTrade,
  eventKey,
  normalizeAddress,
  userActionKey
} from '../snipe-fast-isolated/identity.mjs';
const hash = (x) => createHash('sha256').update(x).digest('hex');
const HEX = /^[a-f0-9]{64}$/;
export function validationCode(p) {
  if (!normalizeAddress(p?.proxyWallet)) return 'INVALID_PROXY_WALLET';
  if (typeof p?.transactionHash !== 'string' || !/^0x[a-f0-9]{64}$/.test(p.transactionHash.toLowerCase()))
    return 'INVALID_TRANSACTION_HASH';
  if (typeof p?.asset !== 'string' || !p.asset.trim() || p.asset.trim().length > 256) return 'INVALID_ASSET';
  if (!['BUY', 'SELL'].includes(p?.side)) return 'INVALID_SIDE';
  if (
    (typeof p?.size !== 'number' && (typeof p?.size !== 'string' || !p.size.trim())) ||
    !Number.isFinite(Number(p.size)) ||
    Number(p.size) <= 0 ||
    Number(Number(p.size).toFixed(8)) <= 0
  )
    return 'INVALID_SIZE';
  if (
    (typeof p?.price !== 'number' && (typeof p?.price !== 'string' || !p.price.trim())) ||
    !Number.isFinite(Number(p.price)) ||
    Number(p.price) < 0 ||
    Number(p.price) > 1
  )
    return 'INVALID_PRICE';
  if (!Number.isSafeInteger(Number(p?.timestamp))) return 'INVALID_TIMESTAMP';
  return 'UNCLASSIFIED_PAYLOAD_VALIDATION';
}
export function scopeEvidence(wallets) {
  const values = [...wallets].sort();
  return {
    wallet_count: values.length,
    scope_digest: hash(JSON.stringify(values)),
    wallet_tags: values.map(hash)
  };
}
export function eventEvidence(envelope, context = {}) {
  const p = envelope?.payload,
    wallet = normalizeAddress(p?.proxyWallet),
    tx = typeof p?.transactionHash === 'string' ? p.transactionHash.toLowerCase() : '';
  const asset = typeof p?.asset === 'string' ? p.asset.trim() : '';
  const identity =
    wallet &&
    /^0x[a-f0-9]{64}$/.test(tx) &&
    asset.length > 0 &&
    asset.length <= 256 &&
    ['BUY', 'SELL'].includes(p?.side)
      ? { wallet, hash: tx, asset, side: p.side }
      : null;
  const valid = wallet ? canonicalTrade(p, wallet, 0, Number.MAX_SAFE_INTEGER) : null;
  return {
    frame_id: context.frame_id ?? null,
    generation: context.generation ?? null,
    connection_id: context.connection_id ?? null,
    frame_seq: context.frame_seq ?? null,
    received_wall_ms: context.received_wall_ms ?? null,
    received_mono_ms: context.received_mono_ms ?? null,
    raw_sha256: context.raw_sha256 ?? null,
    raw_bytes: context.raw_bytes ?? null,
    topic: envelope?.topic === 'activity' ? 'activity' : 'OTHER',
    type: ['trades', 'orders_matched'].includes(envelope?.type) ? envelope.type : 'OTHER',
    source_timestamp: Number.isSafeInteger(Number(p?.timestamp)) ? Number(p.timestamp) : null,
    wallet_tag: wallet ? hash(wallet) : null,
    action_key: identity ? userActionKey(identity) : null,
    representation_key: valid ? eventKey(valid) : null
  };
}
const KINDS = new Set([
  'B_STATE',
  'B_SCOPE',
  'B_FRAME_DECISION',
  'B_EVENT_RECEIVED',
  'B_EVENT_DROPPED',
  'B_BACKGROUND_ACTIVITY',
  'B_CAPTURE_RESULT',
  'B_FORWARD_REQUESTED',
  'B_FORWARD_RESULT',
  'B_RTDS_LIFECYCLE',
  'B_NATIVE_SIGNAL',
  'B_DIAGNOSTICS_SIGNAL_LOST',
  'B_PROCESS_SIGNAL'
]);
const NUMBERS = new Set([
  'at_ms',
  'mono_ms',
  'generation',
  'frame_seq',
  'event_seq',
  'received_wall_ms',
  'received_mono_ms',
  'raw_bytes',
  'source_timestamp',
  'scope_version',
  'wallet_count',
  'boundary_ms',
  'gaps',
  'matched',
  'attention_count',
  'stale',
  'wall_ms',
  'connect_ms',
  'connection_age_ms',
  'last_raw_age_ms',
  'last_data_age_ms',
  'last_control_age_ms',
  'silence_ms',
  'backoff_ms',
  'since_connect_ms',
  'since_subscribe_ms',
  'code',
  'buffered_bytes',
  'connection_generation',
  'event_count',
  'first_at_ms',
  'last_at_ms',
  'first_frame_seq',
  'last_frame_seq'
]);
const HASHES = new Set([
  'raw_sha256',
  'wallet_tag',
  'action_key',
  'representation_key',
  'scope_digest',
  'reason_sha256'
]);
const TOKENS = new Set([
  'state',
  'reason',
  'channel_state',
  'channel_reason',
  'validation_code',
  'decision',
  'queue_status',
  'owner_status',
  'lifecycle_kind',
  'initiator',
  'local_trigger',
  'transport',
  'error_class',
  'error_code',
  'correlation',
  'protocol_pong'
]);
const UUID = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
export function safeDiagnostic(row) {
  if (!row || !KINDS.has(row.kind)) throw Error('DIAGNOSTIC_SCHEMA');
  const out = { kind: row.kind, source: 'B' };
  for (const [k, v] of Object.entries(row)) {
    if (NUMBERS.has(k) && (v === null || (typeof v === 'number' && Number.isFinite(v)))) out[k] = v;
    else if (HASHES.has(k) && (v === null || (typeof v === 'string' && HEX.test(v)))) out[k] = v;
    else if (TOKENS.has(k) && (v === null || (typeof v === 'string' && /^[A-Z][A-Z0-9_]{0,95}$/.test(v))))
      out[k] = v;
  }
  for (const k of ['attention', 'blocked_pairs'])
    if (typeof row[k] === 'boolean' || row[k] === null) out[k] = row[k];
  if (row.delivery_authorized === false) out.delivery_authorized = false;
  for (const k of ['connection_id'])
    if (row[k] === null || (typeof row[k] === 'string' && UUID.test(row[k]))) out[k] = row[k];
  if (typeof row.frame_id === 'string' && /^[a-f0-9-]{36}:[0-9]{1,16}$/.test(row.frame_id))
    out.frame_id = row.frame_id;
  if (['activity', 'OTHER'].includes(row.topic)) out.topic = row.topic;
  if (['trades', 'orders_matched', 'PING', 'PONG', 'OTHER', 'SIGTERM', 'SIGINT'].includes(row.type))
    out.type = row.type;
  if (
    Array.isArray(row.wallet_tags) &&
    row.wallet_tags.length <= 1000 &&
    row.wallet_tags.every((x) => typeof x === 'string' && HEX.test(x))
  )
    out.wallet_tags = row.wallet_tags.slice();
  if (row.ack === 'NOT_MEASURED') out.ack = row.ack;
  if (
    ['Error', 'TypeError', 'RangeError', 'DOMException', 'AggregateError', 'OTHER', 'NOT_EXPOSED'].includes(
      row.error_class
    )
  )
    out.error_class = row.error_class;
  if (row.rtt_ms === null) out.rtt_ms = null;
  if (Array.isArray(row.topics))
    out.topics = row.topics
      .filter((x) => x?.topic === 'activity' && ['trades', 'orders_matched'].includes(x.type))
      .map((x) => ({ topic: 'activity', type: x.type }));
  return out;
}
export function createBJournal({
  directory,
  runId,
  now = Date.now,
  mono = () => performance.now(),
  io = fsp,
  maxQueueBytes = 2 * 1024 ** 2,
  maxFileBytes = 64 * 1024 ** 2,
  maxFiles = 256,
  maxTotalBytes = 8 * 1024 ** 3,
  minFreeBytes = 1024 ** 3,
  maxRecordBytes = 128 * 1024,
  writeBytes = null,
  retentionMs = 48 * 3600000,
  aggregateBackground = true
} = {}) {
  if (
    typeof directory !== 'string' ||
    !path.isAbsolute(directory) ||
    typeof runId !== 'string' ||
    !/^[a-z0-9][a-z0-9-]{7,95}$/.test(runId) ||
    [maxQueueBytes, maxFileBytes, maxFiles, maxTotalBytes, maxRecordBytes].some(
      (n) => !Number.isSafeInteger(n) || n < 1
    ) ||
    !Number.isSafeInteger(minFreeBytes) ||
    minFreeBytes < 0 ||
    !Number.isSafeInteger(retentionMs) ||
    retentionMs < 0 ||
    typeof mono !== 'function' ||
    typeof aggregateBackground !== 'boolean' ||
    maxRecordBytes > maxQueueBytes ||
    maxRecordBytes > maxFileBytes ||
    maxFileBytes > maxTotalBytes
  )
    throw Error('JOURNAL_CONFIGURATION');
  const execution = randomUUID(),
    prefix = 'b-diag-',
    stats = {
      accepted: 0,
      written: 0,
      dropped: 0,
      errors: 0,
      pruned_files: 0,
      coverage_complete: true,
      last_error: null,
      background_events: 0,
      background_summary_records: 0
    };
  const background = new Map();
  let backgroundSince = null;
  let started = false,
    closing = false,
    disabled = false,
    handle = null,
    current = null,
    fileBytes = 0,
    totalBytes = 0,
    files = [],
    queue = [],
    queuedBytes = 0,
    inflightBytes = 0,
    seq = 0,
    segment = 0,
    task = null,
    gapPending = 0,
    retentionPending = 0;
  const fail = (reason, n = 1) => {
    stats.errors++;
    stats.last_error = reason;
    stats.dropped += n;
    gapPending += n;
    stats.coverage_complete = false;
  };
  const packet = (value) =>
    Buffer.from(
      JSON.stringify({ schema: 1, run_id: runId, execution_id: execution, seq: ++seq, ...value }) + '\n'
    );
  async function inventory() {
    files = [];
    totalBytes = 0;
    for (const name of await io.readdir(directory)) {
      if (!/^b-diag-[0-9]{13}-[a-f0-9-]{36}-[0-9]{5}\.jsonl$/.test(name)) continue;
      const file = path.join(directory, name),
        s = await io.lstat(file);
      if (!s.isFile() || s.isSymbolicLink() || s.uid !== process.getuid() || (s.mode & 0o077) !== 0)
        throw Error('JOURNAL_FILE_UNSAFE');
      files.push({
        file,
        bytes: s.size,
        created_ms: Number(name.slice(7, 20)),
        last_wall_ms: Math.max(Number(name.slice(7, 20)), s.mtimeMs),
        last_mono_ms: mono()
      });
      totalBytes += s.size;
    }
    files.sort((a, b) => a.file.localeCompare(b.file));
  }
  async function prune(extra = 0, newFile = false) {
    while (totalBytes + extra > maxTotalBytes || files.length + (newFile ? 1 : 0) > maxFiles) {
      // Both clocks must cover the youngest record, not merely the segment's
      // creation time. Previously existing files get a conservative fresh hold
      // after a process restart; a forward wall-clock jump cannot age them out.
      const index = files.findIndex(
        (x) =>
          x.file !== current &&
          Number.isFinite(x.last_wall_ms) &&
          Number.isFinite(x.last_mono_ms) &&
          now() - x.last_wall_ms >= retentionMs &&
          mono() - x.last_mono_ms >= retentionMs
      );
      if (index < 0) throw Error('JOURNAL_PROTECTED_RETENTION_BUDGET');
      const [old] = files.splice(index, 1);
      await io.unlink(old.file);
      totalBytes -= old.bytes;
      stats.pruned_files++;
      retentionPending++;
      stats.coverage_complete = false;
    }
  }
  async function rotate() {
    if (handle) {
      await handle.close();
      handle = null;
    }
    current = null;
    await prune(0, true);
    const disk = await io.statfs(directory);
    if (disk.bavail * disk.bsize < minFreeBytes + maxFileBytes) throw Error('JOURNAL_DISK_RESERVE');
    current = path.join(
      directory,
      `${prefix}${String(now()).padStart(13, '0')}-${execution}-${String(segment++).padStart(5, '0')}.jsonl`
    );
    handle = await io.open(
      current,
      fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_EXCL | fs.constants.O_NOFOLLOW,
      0o600
    );
    fileBytes = 0;
    files.push({ file: current, bytes: 0, created_ms: now(), last_wall_ms: now(), last_mono_ms: mono() });
  }
  async function append(buf) {
    if (buf.length > maxFileBytes) throw Error('JOURNAL_RECORD_LIMIT');
    if (!handle || fileBytes + buf.length > maxFileBytes) await rotate();
    await prune(buf.length);
    if (writeBytes) await writeBytes(handle, buf);
    else {
      let at = 0;
      while (at < buf.length) {
        const r = await handle.write(buf, at, buf.length - at);
        if (r.bytesWritten <= 0) throw Error('JOURNAL_SHORT_WRITE');
        at += r.bytesWritten;
      }
    }
    fileBytes += buf.length;
    totalBytes += buf.length;
    Object.assign(
      files.find((x) => x.file === current),
      { bytes: fileBytes, last_wall_ms: now(), last_mono_ms: mono() }
    );
  }
  async function drain() {
    try {
      while (!disabled && (queue.length || gapPending || retentionPending)) {
        if (!queue.length && gapPending) {
          const n = gapPending;
          gapPending = 0;
          await append(
            packet({ kind: 'B_LOG_GAP', at_ms: now(), dropped_records: n, reason: stats.last_error })
          );
          continue;
        }
        if (!queue.length && retentionPending) {
          const n = retentionPending;
          retentionPending = 0;
          await append(
            packet({
              kind: 'B_LOG_RETENTION',
              at_ms: now(),
              pruned_files: n,
              total_pruned_files: stats.pruned_files
            })
          );
          continue;
        }
        const item = queue.shift();
        queuedBytes -= item.length;
        inflightBytes = item.length;
        await append(item);
        inflightBytes = 0;
        stats.written++;
      }
    } catch (error) {
      const lost = queue.length + (inflightBytes ? 1 : 0);
      fail(
        error?.message === 'JOURNAL_PROTECTED_RETENTION_BUDGET'
          ? 'JOURNAL_PROTECTED_RETENTION_BUDGET'
          : 'JOURNAL_WRITE_FAILED',
        lost
      );
      disabled = true;
      queue = [];
      queuedBytes = 0;
      inflightBytes = 0;
      if (handle) {
        try {
          await handle.close();
        } catch {}
        handle = null;
      }
    }
  }
  function schedule() {
    if (!task && !disabled)
      task = Promise.resolve()
        .then(drain)
        .finally(() => {
          task = null;
          if (!disabled && (queue.length || gapPending || retentionPending)) schedule();
        });
  }
  function enqueue(value) {
    let buf;
    try {
      buf = packet(safeDiagnostic(value));
    } catch {
      fail('JOURNAL_SCHEMA_REJECT');
      schedule();
      return false;
    }
    if (buf.length > maxRecordBytes || queuedBytes + inflightBytes + buf.length > maxQueueBytes) {
      fail(buf.length > maxRecordBytes ? 'JOURNAL_RECORD_LIMIT' : 'JOURNAL_QUEUE_FULL');
      schedule();
      return false;
    }
    queue.push(buf);
    queuedBytes += buf.length;
    stats.accepted++;
    schedule();
    return true;
  }
  function flushBackground() {
    for (const row of background.values()) {
      if (enqueue(row)) stats.background_summary_records++;
    }
    background.clear();
    backgroundSince = null;
  }
  function record(row) {
    if (disabled) {
      stats.dropped++;
      stats.coverage_complete = false;
      return false;
    }
    if (!started || closing) {
      fail('JOURNAL_UNAVAILABLE');
      return false;
    }
    if (aggregateBackground && row?.kind === 'B_EVENT_DROPPED' && row.reason === 'OUTSIDE_ACTIVE_SCOPE') {
      const value = safeDiagnostic(row),
        at = now();
      if (
        backgroundSince !== null &&
        (!Number.isFinite(at - backgroundSince) || at - backgroundSince < 0 || at - backgroundSince >= 1000)
      )
        flushBackground();
      const key = [value.connection_id, value.generation, value.scope_version, value.type].join('|');
      if (!background.has(key) && background.size >= 32) flushBackground();
      let b = background.get(key);
      if (!b) {
        backgroundSince ??= at;
        b = {
          kind: 'B_BACKGROUND_ACTIVITY',
          reason: 'OUTSIDE_ACTIVE_SCOPE',
          at_ms: value.at_ms,
          connection_id: value.connection_id,
          generation: value.generation,
          scope_version: value.scope_version,
          type: value.type,
          first_at_ms: value.at_ms,
          last_at_ms: value.at_ms,
          first_frame_seq: value.frame_seq,
          last_frame_seq: value.frame_seq,
          event_count: 0
        };
        background.set(key, b);
      }
      b.event_count++;
      b.last_at_ms = value.at_ms;
      b.last_frame_seq = value.frame_seq;
      stats.background_events++;
      return true;
    }
    // Flush before membership/connection changes: never merge epochs or disguise
    // an eligible event/drop as background activity.
    if (background.size) flushBackground();
    return enqueue(row);
  }
  async function start() {
    if (started) throw Error('JOURNAL_SINGLE_USE');
    started = true;
    try {
      try {
        await io.mkdir(directory, { mode: 0o700 });
      } catch (e) {
        if (e.code !== 'EEXIST') throw e;
      }
      const d = await io.lstat(directory);
      if (
        !d.isDirectory() ||
        d.isSymbolicLink() ||
        d.uid !== process.getuid() ||
        (d.mode & 0o077) !== 0 ||
        (await io.realpath(directory)) !== path.resolve(directory)
      )
        throw Error('JOURNAL_DIRECTORY_UNSAFE');
      await inventory();
      await rotate();
      await append(
        packet({
          kind: 'B_LOG_OPENED',
          at_ms: now(),
          max_queue_bytes: maxQueueBytes,
          max_retained_bytes: maxTotalBytes,
          protected_retention_ms: retentionMs,
          background_aggregation: aggregateBackground,
          coverage_scope: 'CURRENT_EXECUTION'
        })
      );
      if (retentionPending) schedule();
    } catch (error) {
      disabled = true;
      fail(
        error?.message === 'JOURNAL_PROTECTED_RETENTION_BUDGET'
          ? 'JOURNAL_PROTECTED_RETENTION_BUDGET'
          : 'JOURNAL_INIT_FAILED'
      );
      if (handle) {
        try {
          await handle.close();
        } catch {}
        handle = null;
      }
    }
    return snapshot();
  }
  async function flush() {
    if (!disabled) flushBackground();
    while (task) await task;
  }
  async function close() {
    const wasClosing = closing;
    closing = true;
    await flush();
    if (handle) {
      try {
        if (!wasClosing && !disabled) {
          await append(
            packet({
              kind: 'B_LOG_CLOSED',
              at_ms: now(),
              accepted: stats.accepted,
              written: stats.written,
              dropped: stats.dropped,
              coverage_complete: stats.coverage_complete
            })
          );
          if (retentionPending) {
            schedule();
            await flush();
          }
        }
        await handle?.sync();
      } catch (error) {
        fail(
          error?.message === 'JOURNAL_PROTECTED_RETENTION_BUDGET'
            ? 'JOURNAL_PROTECTED_RETENTION_BUDGET'
            : 'JOURNAL_CLOSE_FAILED',
          0
        );
      }
      try {
        await handle?.close();
      } catch {
        fail('JOURNAL_CLOSE_FAILED', 0);
      }
      handle = null;
    }
  }
  function snapshot() {
    return {
      ...stats,
      started,
      disabled,
      closing,
      queue_bytes: queuedBytes + inflightBytes,
      max_queue_bytes: maxQueueBytes,
      retained_bytes: totalBytes,
      max_retained_bytes: maxTotalBytes,
      retained_files: files.length,
      max_files: maxFiles,
      protected_retention_ms: retentionMs,
      retention_clock: 'WALL_AND_MONOTONIC_CONSERVATIVE_RESTART',
      retained_from_ms: files.length ? Math.min(...files.map((x) => x.created_ms)) : null,
      coverage_scope: 'CURRENT_EXECUTION',
      background_pending_events: [...background.values()].reduce((n, x) => n + x.event_count, 0),
      background_aggregation: aggregateBackground,
      execution_id: execution,
      sync_policy: 'ASYNC_APPEND_FSYNC_ON_CLOSE',
      power_loss_durability: 'NOT_GUARANTEED'
    };
  }
  return Object.freeze({ start, record, snapshot, flush, close });
}
