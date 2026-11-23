import fs from 'node:fs';
import path from 'node:path';
import { DatabaseSync } from 'node:sqlite';

// Private B evidence. Deliberately exposes no API for reading back envelopes.
const MAX_FRAME_BYTES = 262144;
const MAX_DATABASE_BYTES = 64 * 1024 ** 2;
const MAX_EVENTS = 10000;
const MAX_PAYLOAD_BYTES = 32 * 1024 ** 2;
const MIN_FREE_BYTES = 64 * 1024 ** 2;

export function openBCaptureStore({ directory, runId, scopeFingerprint,
  maxEvents = MAX_EVENTS, maxPayloadBytes = MAX_PAYLOAD_BYTES, minFreeBytes = MIN_FREE_BYTES }) {
  if (!path.isAbsolute(directory) || !/^[a-z0-9][a-z0-9-]{7,95}$/.test(runId) ||
      !/^[a-f0-9]{64}$/.test(scopeFingerprint) ||
      !Number.isSafeInteger(maxEvents) || maxEvents < 1 || maxEvents > MAX_EVENTS ||
      !Number.isSafeInteger(maxPayloadBytes) || maxPayloadBytes < 1 || maxPayloadBytes > MAX_PAYLOAD_BYTES ||
      !Number.isSafeInteger(minFreeBytes) || minFreeBytes < 0) throw Error('B_CAPTURE_CONFIG_INVALID');

  const dir = fs.lstatSync(directory);
  if (!dir.isDirectory() || dir.uid !== process.getuid() || (dir.mode & 0o077)) throw Error('B_CAPTURE_DIRECTORY_UNSAFE');
  const file = path.join(directory, 'channel-capture.sqlite');
  const fd = fs.openSync(file, fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK, 0o600);
  try {
    const st = fs.fstatSync(fd);
    if (!st.isFile() || st.uid !== process.getuid() || (st.mode & 0o077) || st.nlink !== 1 || st.size > MAX_DATABASE_BYTES) {
      throw Error('B_CAPTURE_FILE_UNSAFE');
    }
  } finally { fs.closeSync(fd); }

  const db = new DatabaseSync(file);
  try {
    db.exec(`PRAGMA journal_mode=DELETE; PRAGMA synchronous=EXTRA; PRAGMA busy_timeout=0;
      PRAGMA max_page_count=${Math.floor(MAX_DATABASE_BYTES / db.prepare('PRAGMA page_size').get().page_size)};
      CREATE TABLE IF NOT EXISTS capture_identity (id INTEGER PRIMARY KEY CHECK (id=1), run_id TEXT, scope_fingerprint TEXT);
      CREATE TABLE IF NOT EXISTS capture_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, received_ms INTEGER NOT NULL, scope_version INTEGER NOT NULL,
        connection_id TEXT, generation INTEGER, frame_seq INTEGER, action_key TEXT NOT NULL,
        envelope_json TEXT NOT NULL, payload_bytes INTEGER NOT NULL,
        state TEXT NOT NULL, reason TEXT
      );
      CREATE INDEX IF NOT EXISTS capture_payload_bytes ON capture_events(payload_bytes);`);
    db.prepare('INSERT OR IGNORE INTO capture_identity VALUES (1, ?, ?)').run(runId, scopeFingerprint);
    const identity = db.prepare('SELECT run_id, scope_fingerprint FROM capture_identity').get();
    if (identity.run_id !== runId || identity.scope_fingerprint !== scopeFingerprint) throw Error('B_CAPTURE_IDENTITY_MISMATCH');
    const insert = db.prepare(`INSERT INTO capture_events
      (received_ms, scope_version, connection_id, generation, frame_seq, action_key, envelope_json, payload_bytes, state, reason)
      SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
      FROM (SELECT count(*) AS events, coalesce(sum(payload_bytes), 0) AS bytes FROM capture_events)
      WHERE events < ? AND bytes + ? <= ?`);
    const mark = db.prepare("UPDATE capture_events SET state=?, reason=? WHERE id=? AND state='PENDING_ACK'");
    const remove = db.prepare("DELETE FROM capture_events WHERE id=? AND state='PENDING_ACK'");
    const totals = db.prepare('SELECT state, count(*) AS events, sum(payload_bytes) AS bytes FROM capture_events GROUP BY state');
    const interrupted = db.prepare("UPDATE capture_events SET state='ACK_UNCERTAIN', reason='B_PROCESS_INTERRUPTED' WHERE state='PENDING_ACK'").run().changes;

    function save(envelope, evidence, sendLive, reason) {
      const json = JSON.stringify(envelope);
      const bytes = Buffer.byteLength(json);
      if (bytes > MAX_FRAME_BYTES) throw Error('B_CAPTURE_FRAME_LIMIT');
      const disk = fs.statfsSync(directory);
      if (disk.bavail * disk.bsize < minFreeBytes + MAX_DATABASE_BYTES) throw Error('B_CAPTURE_LOW_DISK');
      const row = insert.run(evidence.received_wall_ms, evidence.scope_version, evidence.connection_id,
        evidence.generation, evidence.frame_seq, evidence.action_key, json, bytes,
        sendLive ? 'PENDING_ACK' : 'CAPTURED_ONLY', reason, maxEvents, bytes, maxPayloadBytes);
      if (row.changes !== 1) throw Error('B_CAPTURE_FULL');
      return row.lastInsertRowid;
    }

    return Object.freeze({
      save, resolve(id, state, reason) {
        if (state === 'QUEUE_ACK') remove.run(id);
        else mark.run(state, reason, id);
      },
      snapshot() {
        const counts = {}; let bytes = 0;
        for (const row of totals.all()) { counts[row.state] = row.events; bytes += row.bytes; }
        return { counts, bytes, max_events: maxEvents, max_payload_bytes: maxPayloadBytes, interrupted };
      },
      close: () => db.close()
    });
  } catch (error) { db.close(); throw error; }
}
