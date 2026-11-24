// Local-only FAST-compatible queue owner. Importing this file opens no resources.
import fs from 'node:fs';
import path from 'node:path';
import os from 'node:os';
import { fileURLToPath } from 'node:url';
import { DatabaseSync } from 'node:sqlite';
import { performance } from 'node:perf_hooks';
import { randomUUID, createHmac } from 'node:crypto';
import {
  canonicalTrade,
  eventKey,
  userActionKey,
  userActionIdentity,
  formatAlert
} from '../snipe-fast-isolated/identity.mjs';
import { normalizePairs, scopeFingerprint, sha, validateScopeApproval } from './scope.mjs';

export const MAX_STATE_AGE_MS = 2000;
const SOURCES = new Set(['A', 'B']);
const KNOWN_STATES = new Set([
  'PENDING',
  'SENDING',
  'FAILED_RETRYABLE',
  'FAILED_FINAL',
  'DELIVERED',
  'DELIVERY_UNKNOWN',
  'CANCELLED_SUBSCRIPTION',
  'LOCAL_SIMULATED'
]);
const safeToken = (s) => typeof s === 'string' && /^[A-Z][A-Z0-9_]{0,95}$/.test(s);
const repo = fileURLToPath(new URL('../..', import.meta.url));

function localDirectory(directory) {
  if (
    typeof directory !== 'string' ||
    !path.isAbsolute(directory) ||
    !/^fast-dual-local-[a-zA-Z0-9_-]+$/.test(path.basename(directory))
  )
    throw Error('LOCAL_DIRECTORY_REQUIRED');
  const parent = fs.realpathSync(path.dirname(directory));
  const allowed = [fs.realpathSync(os.tmpdir()), '/private/tmp', path.join(repo, 'artifacts')];
  if (!allowed.includes(parent) || path.join(parent, path.basename(directory)) !== directory)
    throw Error('LOCAL_DIRECTORY_REQUIRED');
  if (fs.existsSync(directory)) {
    if (fs.lstatSync(directory).isSymbolicLink() || !fs.statSync(directory).isDirectory())
      throw Error('LOCAL_DIRECTORY_REQUIRED');
  } else fs.mkdirSync(directory, { mode: 0o700 });
  fs.chmodSync(directory, 0o700);
  return directory;
}

const schema = `
CREATE TABLE deliveries (
 event_key TEXT NOT NULL,chat_id INTEGER NOT NULL,wallet TEXT NOT NULL,
 source_timestamp INTEGER NOT NULL,first_seen_at_ms INTEGER NOT NULL,
 message_html TEXT NOT NULL,state TEXT NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,
 ambiguous_retries INTEGER NOT NULL DEFAULT 0,possible_duplicate INTEGER NOT NULL DEFAULT 0,
 next_at_ms INTEGER NOT NULL DEFAULT 0,claim_token TEXT,http_status INTEGER,last_error TEXT,
 delivered_at_ms INTEGER,send_started_at_ms INTEGER,PRIMARY KEY(event_key,chat_id)
);
CREATE TABLE send_once_actions (
 action_key TEXT NOT NULL,chat_id INTEGER NOT NULL,identity_json TEXT NOT NULL,
 first_event_key TEXT NOT NULL,first_trace_id TEXT,created_at_ms INTEGER NOT NULL,
 PRIMARY KEY(action_key,chat_id)
);
CREATE TABLE dual_meta (key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE dual_pairs (wallet TEXT,chat_id INTEGER,notify_from_ts INTEGER,PRIMARY KEY(wallet,chat_id));
CREATE TABLE dual_active_pairs (wallet TEXT,chat_id INTEGER,notify_from_ts INTEGER,PRIMARY KEY(wallet,chat_id));
CREATE TABLE dual_sources (source TEXT PRIMARY KEY,session TEXT,generation INTEGER NOT NULL,connected INTEGER NOT NULL,opened_wall_ms INTEGER NOT NULL);
CREATE TABLE dual_receipts (
 source TEXT,session TEXT,message_hash TEXT,sequence INTEGER,envelope_hash TEXT,result_json TEXT,
 PRIMARY KEY(source,session,message_hash),UNIQUE(source,session,sequence)
);
CREATE TABLE dual_blocks (action_key TEXT,chat_id INTEGER,reason TEXT NOT NULL,PRIMARY KEY(action_key,chat_id));
CREATE TABLE dual_simulations (claim_token TEXT PRIMARY KEY,action_key TEXT,chat_id INTEGER,status TEXT NOT NULL);
CREATE INDEX idx_fast_due ON deliveries(state,next_at_ms);
`;

export function createLocalFastIngress({
  directory,
  scope,
  scopeApproval = null,
  startMs,
  now = Date.now,
  mono = () => performance.now(),
  reopen = false,
  telegramEnabled = false,
  mode = 'LOCAL_ONLY',
  activeSubsetMode = false
} = {}) {
  if (mode !== 'LOCAL_ONLY' || telegramEnabled !== false) throw Error('LOCAL_ONLY_TELEGRAM_OFF_REQUIRED');
  if (
    !Number.isSafeInteger(startMs) ||
    startMs <= 0 ||
    typeof now !== 'function' ||
    typeof mono !== 'function'
  )
    throw Error('INVALID_LOCAL_CONFIG');
  if (scopeApproval) scopeApproval = validateScopeApproval(scope, scopeApproval);
  scope = normalizePairs(scope, { exact14: scopeApproval === null });
  const canonicalScope = JSON.stringify(scope),
    fingerprint = scopeFingerprint(scope, { exact14: scopeApproval === null }),
    approvalText = scopeApproval ? JSON.stringify(scopeApproval) : 'LEGACY_APPROVED14';
  directory = localDirectory(directory);
  const databasePath = path.join(directory, 'local-fast-dual.sqlite'),
    lock = path.join(directory, 'owner.lock'),
    owner = randomUUID();
  if (fs.existsSync(lock)) throw Error('QUEUE_OWNER_ALREADY_EXISTS');
  if (reopen ? !fs.existsSync(databasePath) : fs.readdirSync(directory).length !== 0)
    throw Error('LOCAL_DATABASE_MODE_MISMATCH');
  const fd = fs.openSync(lock, 'wx', 0o600);
  fs.writeFileSync(fd, owner);
  fs.closeSync(fd);
  let db,
    closed = false,
    memoryHalt = false;
  try {
    if (fs.existsSync(databasePath) && fs.lstatSync(databasePath).isSymbolicLink())
      throw Error('LOCAL_DATABASE_SYMLINK');
    db = new DatabaseSync(databasePath);
    fs.chmodSync(databasePath, 0o600);
    db.exec('PRAGMA journal_mode=WAL; PRAGMA busy_timeout=1000;');
    if (!reopen) {
      db.exec('BEGIN IMMEDIATE;');
      try {
        db.exec(schema);
        const add = db.prepare('INSERT INTO dual_meta VALUES(?,?)');
        for (const [k, v] of Object.entries({
          mode: 'LOCAL_ONLY',
          telegram_enabled: 'false',
          scope: canonicalScope,
          fingerprint,
          scope_approval: approvalText,
          start_ms: String(startMs),
          active_subset_mode: String(activeSubsetMode),
          tag_salt: randomUUID(),
          halted: ''
        }))
          add.run(k, v);
        for (const table of ['dual_pairs', 'dual_active_pairs'])
          for (const p of scope)
            db.prepare(`INSERT INTO ${table} VALUES(?,?,?)`).run(p.wallet, p.recipient, p.notify_from_ts);
        for (const source of SOURCES) db.prepare('INSERT INTO dual_sources VALUES(?,NULL,0,0,0)').run(source);
        db.exec('COMMIT;');
      } catch (e) {
        db.exec('ROLLBACK;');
        throw e;
      }
    }
    const stored = Object.fromEntries(
      db
        .prepare('SELECT * FROM dual_meta')
        .all()
        .map((r) => [r.key, r.value])
    );
    if (
      stored.mode !== 'LOCAL_ONLY' ||
      stored.telegram_enabled !== 'false' ||
      stored.scope !== canonicalScope ||
      stored.fingerprint !== fingerprint ||
      stored.start_ms !== String(startMs)
    )
      throw Error('LOCAL_DATABASE_SCOPE_MISMATCH');
    if ((stored.scope_approval ?? 'LEGACY_APPROVED14') !== approvalText)
      throw Error('LOCAL_SCOPE_APPROVAL_MISMATCH');
    if ((stored.active_subset_mode ?? 'false') !== String(activeSubsetMode))
      throw Error('LOCAL_SUBSCRIPTION_MODE_MISMATCH');
    db.exec('BEGIN IMMEDIATE;');
    try {
      db.prepare('UPDATE dual_sources SET connected=0,session=NULL').run();
      db.prepare(
        "UPDATE deliveries SET state='DELIVERY_UNKNOWN',claim_token=NULL,possible_duplicate=1,last_error='LOCAL_OWNER_INTERRUPTED' WHERE state='SENDING'"
      ).run();
      db.prepare("UPDATE dual_simulations SET status='OWNER_INTERRUPTED' WHERE status='CLAIMED'").run();
      db.exec('COMMIT;');
    } catch (e) {
      db.exec('ROLLBACK;');
      throw e;
    }
  } catch (e) {
    try {
      db?.close();
    } catch {}
    fs.unlinkSync(lock);
    throw e;
  }

  const metrics = { accepted: 0, duplicate_pairs: 0, blocked: 0, rejected: 0, receipt_replays: 0 };
  const salt = db.prepare("SELECT value FROM dual_meta WHERE key='tag_salt'").get().value;
  const tag = (chat) => createHmac('sha256', salt).update(String(chat)).digest('hex').slice(0, 24);
  const action = db.prepare('SELECT * FROM send_once_actions WHERE action_key=? AND chat_id=?');
  const delivery = db.prepare('SELECT * FROM deliveries WHERE event_key=? AND chat_id=?');
  const insert = db.prepare(
    "INSERT INTO deliveries(event_key,chat_id,wallet,source_timestamp,first_seen_at_ms,message_html,state,next_at_ms) VALUES(?,?,?,?,?,?,'PENDING',?)"
  );
  const reserve = db.prepare('INSERT INTO send_once_actions VALUES(?,?,?,?,?,?)');
  const block = db.prepare('SELECT reason FROM dual_blocks WHERE action_key=? AND chat_id=?');
  function alive() {
    if (closed) throw Error('LOCAL_QUEUE_CLOSED');
  }
  function elapsed(start) {
    const n = mono() - start;
    if (!Number.isFinite(n) || n < 0 || n > MAX_STATE_AGE_MS) {
      const error = Error('STATE_TOO_STALE');
      error.stateAgeMs = Number.isFinite(n) ? n : null;
      throw error;
    }
    return n;
  }
  function guard() {
    if (memoryHalt) throw Error('ADMISSIONS_HALTED');
    if (db.prepare("SELECT value FROM dual_meta WHERE key='halted'").get().value)
      throw Error('ADMISSIONS_HALTED');
    const fixed = db
      .prepare('SELECT wallet,chat_id AS recipient,notify_from_ts FROM dual_pairs ORDER BY wallet,chat_id')
      .all();
    if (JSON.stringify(fixed) !== canonicalScope) throw Error('FROZEN_SCOPE_CHANGED');
    if (activeSubsetMode) {
      for (const p of db.prepare('SELECT wallet,chat_id,notify_from_ts FROM dual_active_pairs').all()) {
        const approved = scope.find((x) => x.wallet === p.wallet && x.recipient === p.chat_id);
        if (!approved || p.notify_from_ts < approved.notify_from_ts) throw Error('ACTIVE_SCOPE_CHANGED');
      }
    } else
      for (const p of scope) {
        const current = db
          .prepare('SELECT notify_from_ts FROM dual_active_pairs WHERE wallet=? AND chat_id=?')
          .get(p.wallet, p.recipient);
        if (!current || current.notify_from_ts !== p.notify_from_ts) throw Error('ACTIVE_SCOPE_CHANGED');
      }
  }
  function transaction(start, fn, { simulationClaim = null } = {}) {
    alive();
    let begun = false,
      committed = false,
      committedValue;
    try {
      db.exec('BEGIN IMMEDIATE;');
      begun = true;
      elapsed(start);
      guard();
      const value = fn();
      committedValue = value;
      elapsed(start);
      db.exec('COMMIT;');
      begun = false;
      committed = true;
      value.state_age_ms = elapsed(start);
      if (value.status === 'BLOCKED') metrics.blocked++;
      return { ...value, ack_only: true, delivery_authorized: false, telegram_sends: 0 };
    } catch (e) {
      if (begun)
        try {
          db.exec('ROLLBACK;');
        } catch {}
      if (committed) {
        memoryHalt = true;
        try {
          db.exec('BEGIN IMMEDIATE;');
          db.prepare("UPDATE dual_meta SET value='COMMIT_EXCEEDED_STATE_BUDGET' WHERE key='halted'").run();
          if (
            typeof simulationClaim === 'string' &&
            ['WOULD_SEND', 'AMBIGUOUS', 'KNOWN_FAILURE'].includes(committedValue?.status)
          ) {
            const sim = db.prepare('SELECT * FROM dual_simulations WHERE claim_token=?').get(simulationClaim),
              a = sim && action.get(sim.action_key, sim.chat_id);
            if (a) {
              db.prepare(
                "UPDATE dual_simulations SET status='INVALIDATED_STATE_BUDGET' WHERE claim_token=?"
              ).run(simulationClaim);
              db.prepare(
                "UPDATE deliveries SET state='DELIVERY_UNKNOWN',possible_duplicate=1,claim_token=NULL,last_error='LOCAL_COMMIT_EXCEEDED_STATE_BUDGET' WHERE event_key=? AND chat_id=?"
              ).run(a.first_event_key, sim.chat_id);
            }
          }
          db.exec('COMMIT;');
        } catch {
          try {
            db.exec('ROLLBACK;');
          } catch {}
        }
      }
      const reason = [
        'STATE_TOO_STALE',
        'ADMISSIONS_HALTED',
        'FROZEN_SCOPE_CHANGED',
        'ACTIVE_SCOPE_CHANGED',
        'IDENTITY_CONFLICT',
        'QUEUE_CONFLICT'
      ].includes(e.message)
        ? e.message
        : /locked|busy/i.test(e.message)
          ? 'SQLITE_BUSY'
          : 'PERSISTENCE_FAILURE';
      metrics.blocked++;
      return {
        status: 'BLOCKED',
        reason,
        state_age_ms: e.stateAgeMs ?? null,
        committed,
        ack_only: true,
        delivery_authorized: false,
        telegram_sends: 0
      };
    }
  }
  function sourceRow(packet) {
    const s = db.prepare('SELECT * FROM dual_sources WHERE source=?').get(packet.source);
    return s && s.connected === 1 && s.session === packet.session_id && s.generation === packet.generation
      ? s
      : null;
  }
  function submit(packet) {
    alive();
    const started = mono();
    if (
      !packet ||
      !SOURCES.has(packet.source) ||
      typeof packet.session_id !== 'string' ||
      !Number.isSafeInteger(packet.generation) ||
      !Number.isSafeInteger(packet.sequence) ||
      packet.sequence < 1 ||
      typeof packet.message_id !== 'string' ||
      !/^[a-zA-Z0-9_-]{1,96}$/.test(packet.message_id) ||
      packet.scope_fingerprint !== fingerprint
    )
      return { status: 'REJECTED', reason: 'INVALID_PACKET', telegram_sends: 0 };
    let serialized;
    try {
      serialized = JSON.stringify(packet.envelope);
    } catch {
      return { status: 'REJECTED', reason: 'INVALID_ENVELOPE', telegram_sends: 0 };
    }
    if (typeof serialized !== 'string' || Buffer.byteLength(serialized) > 262144)
      return { status: 'REJECTED', reason: 'FRAME_BUDGET', telegram_sends: 0 };
    const messageHash = sha(packet.message_id),
      envelopeHash = sha(serialized),
      envelope = JSON.parse(serialized);
    const result = transaction(started, () => {
      const s = sourceRow(packet);
      if (!s) return { status: 'BLOCKED', reason: 'SOURCE_DISCONNECTED_OR_OBSOLETE' };
      const old = db
        .prepare('SELECT * FROM dual_receipts WHERE source=? AND session=? AND message_hash=?')
        .get(packet.source, packet.session_id, messageHash);
      if (old) {
        if (old.sequence !== packet.sequence || old.envelope_hash !== envelopeHash)
          return { status: 'BLOCKED', reason: 'MESSAGE_ID_REUSED' };
        metrics.receipt_replays++;
        return { status: 'ALREADY_COMMITTED', original_status: JSON.parse(old.result_json).status };
      }
      if (
        db
          .prepare('SELECT 1 FROM dual_receipts WHERE source=? AND session=? AND sequence=?')
          .get(packet.source, packet.session_id, packet.sequence)
      )
        return { status: 'BLOCKED', reason: 'SEQUENCE_REUSED' };
      const e = envelope,
        wallet =
          typeof e?.payload?.proxyWallet === 'string' ? e.payload.proxyWallet.trim().toLowerCase() : null;
      if (e?.topic !== 'activity' || !['trades', 'orders_matched'].includes(e.type))
        return { status: 'REJECTED', reason: 'UNSUBSCRIBED_ENVELOPE' };
      const recipients = activeSubsetMode
        ? db
            .prepare(
              'SELECT wallet,chat_id AS recipient,notify_from_ts FROM dual_active_pairs WHERE wallet=?'
            )
            .all(wallet)
        : scope.filter((p) => p.wallet === wallet);
      if (!recipients.length) return { status: 'REJECTED', reason: 'OUTSIDE_FROZEN_SCOPE' };
      // A reconnect only changes A's boundary. Enable/start and subscription boundaries stay strict.
      const trade = canonicalTrade(e.payload, wallet, Math.max(startMs, s.opened_wall_ms - 2000), now());
      if (!trade) return { status: 'REJECTED', reason: 'INVALID_OR_OUTSIDE_LIVE_WINDOW' };
      const key = userActionKey(trade),
        identity = userActionIdentity(trade),
        fill = eventKey(trade),
        seen = now(),
        results = [];
      for (const p of recipients) {
        const pair = { recipient_tag: tag(p.recipient) };
        if (trade.timestamp < p.notify_from_ts) {
          results.push({ ...pair, status: 'REJECTED', reason: 'BEFORE_SUBSCRIPTION_START' });
          continue;
        }
        const stopped = block.get(key, p.recipient);
        if (stopped) {
          results.push({ ...pair, status: 'BLOCKED', reason: stopped.reason });
          continue;
        }
        const orphan = db
          .prepare(
            'SELECT 1 FROM deliveries d WHERE d.wallet=? AND d.chat_id=? AND NOT EXISTS(SELECT 1 FROM send_once_actions a WHERE a.first_event_key=d.event_key AND a.chat_id=d.chat_id) LIMIT 1'
          )
          .get(wallet, p.recipient);
        if (orphan) {
          results.push({ ...pair, status: 'BLOCKED', reason: 'UNMAPPED_LEGACY_DELIVERY' });
          continue;
        }
        const prior = action.get(key, p.recipient);
        if (prior) {
          if (prior.identity_json !== identity) throw Error('IDENTITY_CONFLICT');
          const d = delivery.get(prior.first_event_key, p.recipient);
          if (!d) {
            results.push({ ...pair, status: 'BLOCKED', reason: 'ACTION_RESERVED_WITHOUT_DELIVERY' });
            continue;
          }
          if (d.wallet !== wallet || d.chat_id !== p.recipient || !KNOWN_STATES.has(d.state))
            throw Error('QUEUE_CONFLICT');
          const uncertain = d.state === 'DELIVERY_UNKNOWN' || d.possible_duplicate !== 0;
          results.push({
            ...pair,
            status: uncertain ? 'BLOCKED' : 'DUPLICATE',
            reason: uncertain ? 'DELIVERY_UNKNOWN' : d.state
          });
          continue;
        }
        insert.run(fill, p.recipient, wallet, trade.timestamp, seen, formatAlert(trade), seen);
        reserve.run(key, p.recipient, identity, fill, null, seen);
        elapsed(started);
        results.push({ ...pair, status: 'ACCEPTED' });
      }
      const status = results.some((r) => r.status === 'ACCEPTED')
        ? 'ACCEPTED'
        : results.every((r) => r.status === 'DUPLICATE')
          ? 'DUPLICATE'
          : results.some((r) => r.status === 'BLOCKED')
            ? 'BLOCKED'
            : 'REJECTED';
      const value = { status, action_key: key, results };
      db.prepare('INSERT INTO dual_receipts VALUES(?,?,?,?,?,?)').run(
        packet.source,
        packet.session_id,
        messageHash,
        packet.sequence,
        envelopeHash,
        JSON.stringify(value)
      );
      return value;
    });
    if (result.status === 'ACCEPTED') metrics.accepted++;
    metrics.duplicate_pairs += result.results?.filter((r) => r.status === 'DUPLICATE').length ?? 0;
    if (result.status === 'REJECTED') metrics.rejected++;
    return result;
  }
  function openSource(source) {
    alive();
    if (!SOURCES.has(source)) throw Error('SOURCE_INVALID');
    const result = transaction(mono(), () => {
      const previous = db.prepare('SELECT generation FROM dual_sources WHERE source=?').get(source),
        session = randomUUID(),
        generation = previous.generation + 1;
      db.prepare(
        'UPDATE dual_sources SET session=?,generation=?,connected=1,opened_wall_ms=? WHERE source=?'
      ).run(session, generation, now(), source);
      return { status: 'CONNECTED', source, session_id: session, generation, scope_fingerprint: fingerprint };
    });
    if (result.status !== 'CONNECTED') throw Error(result.reason);
    return result;
  }
  function disconnectSource(session) {
    alive();
    if (!session || !SOURCES.has(session.source)) throw Error('SOURCE_INVALID');
    // Disconnect remains available when the cohort guard has stopped admissions.
    db.prepare('UPDATE dual_sources SET connected=0 WHERE source=? AND session=? AND generation=?').run(
      session.source,
      session.session_id,
      session.generation
    );
  }
  function replaceActiveScope(rows) {
    alive();
    rows = normalizePairs(rows, { exact14: false });
    if (activeSubsetMode) {
      if (
        rows.some(
          (p) =>
            !scope.some(
              (s) =>
                s.wallet === p.wallet && s.recipient === p.recipient && p.notify_from_ts >= s.notify_from_ts
            )
        )
      )
        throw Error('OUTSIDE_APPROVED_SCOPE');
      return transaction(mono(), () => {
        db.exec('DELETE FROM dual_active_pairs;');
        for (const p of rows)
          db.prepare('INSERT INTO dual_active_pairs VALUES(?,?,?)').run(
            p.wallet,
            p.recipient,
            p.notify_from_ts
          );
        db.exec(
          "UPDATE deliveries SET state='CANCELLED_SUBSCRIPTION',last_error='LOCAL_SUBSCRIPTION_REMOVED' WHERE state IN ('PENDING','FAILED_RETRYABLE') AND NOT EXISTS(SELECT 1 FROM dual_active_pairs p WHERE p.wallet=deliveries.wallet AND p.chat_id=deliveries.chat_id AND deliveries.source_timestamp>=p.notify_from_ts)"
        );
        return { status: 'LOCAL_ACTIVE_SCOPE_UPDATED' };
      });
    }
    db.exec('BEGIN IMMEDIATE;');
    try {
      db.exec('DELETE FROM dual_active_pairs;');
      for (const p of rows)
        db.prepare('INSERT INTO dual_active_pairs VALUES(?,?,?)').run(
          p.wallet,
          p.recipient,
          p.notify_from_ts
        );
      db.exec('COMMIT;');
    } catch (e) {
      db.exec('ROLLBACK;');
      throw e;
    }
  }
  function claimForSimulation() {
    const started = mono();
    return transaction(started, () => {
      const row = db
        .prepare(
          "SELECT d.*,a.action_key,a.identity_json FROM deliveries d JOIN send_once_actions a ON a.first_event_key=d.event_key AND a.chat_id=d.chat_id WHERE d.state IN ('PENDING','FAILED_RETRYABLE') AND d.source_timestamp*1000>=? AND d.source_timestamp*1000<=? AND d.possible_duplicate=0 AND d.claim_token IS NULL AND d.next_at_ms<=? AND NOT EXISTS(SELECT 1 FROM dual_blocks b WHERE b.action_key=a.action_key AND b.chat_id=d.chat_id) ORDER BY d.first_seen_at_ms,d.event_key,d.chat_id LIMIT 1"
        )
        .get(startMs, now() + 5000, now());
      if (!row) return { status: 'EMPTY' };
      const t = scope.find((p) => p.wallet === row.wallet && p.recipient === row.chat_id);
      if (!t || row.source_timestamp < t.notify_from_ts) throw Error('QUEUE_CONFLICT');
      if (
        activeSubsetMode &&
        !db
          .prepare('SELECT 1 FROM dual_active_pairs WHERE wallet=? AND chat_id=? AND notify_from_ts<=?')
          .get(row.wallet, row.chat_id, row.source_timestamp)
      )
        return { status: 'BLOCKED', reason: 'SUBSCRIPTION_INACTIVE' };
      verifyReservation(row, row);
      if (
        db
          .prepare(
            'SELECT 1 FROM deliveries d WHERE d.wallet=? AND d.chat_id=? AND NOT EXISTS(SELECT 1 FROM send_once_actions a WHERE a.first_event_key=d.event_key AND a.chat_id=d.chat_id) LIMIT 1'
          )
          .get(row.wallet, row.chat_id)
      )
        return { status: 'BLOCKED', reason: 'UNMAPPED_LEGACY_DELIVERY' };
      const claim = randomUUID();
      const updated = db
        .prepare(
          "UPDATE deliveries SET state='SENDING',claim_token=?,send_started_at_ms=?,attempts=attempts+1 WHERE event_key=? AND chat_id=? AND state=? AND possible_duplicate=0 AND claim_token IS NULL"
        )
        .run(claim, now(), row.event_key, row.chat_id, row.state);
      if (updated.changes !== 1) throw Error('QUEUE_CONFLICT');
      db.prepare("INSERT INTO dual_simulations VALUES(?,?,?,'CLAIMED')").run(
        claim,
        row.action_key,
        row.chat_id
      );
      return {
        status: 'CLAIMED_FOR_SIMULATION',
        claim_token: claim,
        action_key: row.action_key,
        recipient_tag: tag(row.chat_id)
      };
    });
  }
  function finishSimulation(claim, { outcome = 'WOULD_SEND' } = {}) {
    if (!['WOULD_SEND', 'AMBIGUOUS', 'KNOWN_FAILURE'].includes(outcome))
      throw Error('SIMULATION_OUTCOME_INVALID');
    return transaction(
      mono(),
      () => {
        const sim = db.prepare('SELECT * FROM dual_simulations WHERE claim_token=?').get(claim?.claim_token);
        const a = sim && action.get(sim.action_key, sim.chat_id),
          d = a && delivery.get(a.first_event_key, sim.chat_id);
        if (
          !sim ||
          sim.status !== 'CLAIMED' ||
          !d ||
          d.state !== 'SENDING' ||
          d.claim_token !== claim.claim_token ||
          d.possible_duplicate !== 0
        )
          return { status: 'BLOCKED', reason: 'CLAIM_NOT_OWNED' };
        verifyReservation(a, d);
        if (
          activeSubsetMode &&
          !db
            .prepare('SELECT 1 FROM dual_active_pairs WHERE wallet=? AND chat_id=? AND notify_from_ts<=?')
            .get(d.wallet, d.chat_id, d.source_timestamp)
        )
          return { status: 'BLOCKED', reason: 'SUBSCRIPTION_INACTIVE' };
        if (
          !scope.some(
            (p) =>
              p.wallet === d.wallet && p.recipient === d.chat_id && d.source_timestamp >= p.notify_from_ts
          ) ||
          d.source_timestamp * 1000 > now() + 5000
        )
          throw Error('QUEUE_CONFLICT');
        const state =
          outcome === 'WOULD_SEND'
            ? 'LOCAL_SIMULATED'
            : outcome === 'AMBIGUOUS'
              ? 'DELIVERY_UNKNOWN'
              : 'FAILED_FINAL';
        db.prepare(
          'UPDATE deliveries SET state=?,claim_token=NULL,possible_duplicate=?,last_error=?,delivered_at_ms=NULL WHERE event_key=? AND chat_id=? AND claim_token=?'
        ).run(
          state,
          outcome === 'AMBIGUOUS' ? 1 : 0,
          'LOCAL_SIMULATION_ONLY',
          d.event_key,
          d.chat_id,
          claim.claim_token
        );
        db.prepare('UPDATE dual_simulations SET status=? WHERE claim_token=?').run(
          outcome,
          claim.claim_token
        );
        return { status: outcome, action_key: sim.action_key, recipient_tag: tag(sim.chat_id) };
      },
      { simulationClaim: claim?.claim_token ?? null }
    );
  }
  function verifyReservation(a, d) {
    let identity;
    try {
      identity = JSON.parse(a.identity_json);
    } catch {
      throw Error('IDENTITY_CONFLICT');
    }
    if (
      !Array.isArray(identity) ||
      identity.length !== 4 ||
      identity[0] !== d.wallet ||
      identity.some((v) => typeof v !== 'string') ||
      sha(identity.join('|')) !== a.action_key
    )
      throw Error('IDENTITY_CONFLICT');
  }
  function importLocalSnapshot(entries) {
    // Explicit local fixtures/copies only; never opens or reads a production database.
    alive();
    if (!Array.isArray(entries) || entries.length > 10000) throw Error('SNAPSHOT_INVALID');
    if (
      db.prepare('SELECT COUNT(*) n FROM dual_receipts').get().n ||
      db.prepare('SELECT COUNT(*) n FROM dual_sources WHERE connected=1').get().n
    )
      throw Error('SNAPSHOT_AFTER_INGRESS_FORBIDDEN');
    return transaction(mono(), () => {
      for (const e of entries) {
        const wallet = e.payload?.proxyWallet?.trim().toLowerCase(),
          p = scope.find((p) => p.wallet === wallet && p.recipient === e.recipient);
        const t = p && canonicalTrade(e.payload, wallet, 0, Number.MAX_SAFE_INTEGER);
        if (!p || !t || !KNOWN_STATES.has(e.state)) throw Error('QUEUE_CONFLICT');
        const key = userActionKey(t),
          fill = eventKey(t),
          prior = action.get(key, p.recipient);
        db.prepare(
          'INSERT INTO deliveries(event_key,chat_id,wallet,source_timestamp,first_seen_at_ms,message_html,state,possible_duplicate,next_at_ms,delivered_at_ms) VALUES(?,?,?,?,?,?,?,?,?,?)'
        ).run(
          fill,
          p.recipient,
          wallet,
          t.timestamp,
          startMs,
          formatAlert(t),
          e.state,
          e.state === 'DELIVERY_UNKNOWN' ? 1 : 0,
          startMs,
          e.state === 'DELIVERED' ? startMs : null
        );
        if (e.originVerified !== true) continue;
        if (prior) {
          db.prepare('INSERT OR IGNORE INTO dual_blocks VALUES(?,?,?)').run(
            key,
            p.recipient,
            'LEGACY_MULTIPLE_ROWS'
          );
          continue;
        }
        reserve.run(key, p.recipient, userActionIdentity(t), fill, null, startMs);
      }
      return { status: 'LOCAL_SNAPSHOT_IMPORTED' };
    });
  }
  function summary() {
    alive();
    return {
      mode: 'LOCAL_ONLY',
      scope_pairs: scope.length,
      scope_fingerprint: fingerprint,
      queue_rows: db.prepare('SELECT COUNT(*) n FROM deliveries').get().n,
      action_recipient_reservations: db.prepare('SELECT COUNT(*) n FROM send_once_actions').get().n,
      unique_actions: db.prepare('SELECT COUNT(DISTINCT action_key) n FROM send_once_actions').get().n,
      delivery_states: Object.fromEntries(
        db
          .prepare('SELECT state,COUNT(*) n FROM deliveries GROUP BY state')
          .all()
          .map((r) => [r.state, r.n])
      ),
      simulation_outcomes: Object.fromEntries(
        db
          .prepare('SELECT status,COUNT(*) n FROM dual_simulations GROUP BY status')
          .all()
          .map((r) => [r.status, r.n])
      ),
      metrics: { ...metrics },
      telegram_sends: 0,
      REST_requests: 0,
      sockets_opened: 0
    };
  }
  function halt(reason) {
    alive();
    if (!safeToken(reason)) throw Error('HALT_REASON_INVALID');
    db.prepare("UPDATE dual_meta SET value=? WHERE key='halted'").run(reason);
  }
  function close() {
    if (closed) return;
    try {
      db.prepare('UPDATE dual_sources SET connected=0,session=NULL').run();
    } finally {
      db.close();
      closed = true;
      if (fs.readFileSync(lock, 'utf8') === owner) fs.unlinkSync(lock);
    }
  }
  return Object.freeze({
    submit,
    openSource,
    disconnectSource,
    replaceActiveScope,
    claimForSimulation,
    finishSimulation,
    importLocalSnapshot,
    summary,
    halt,
    close,
    databasePath
  });
}
