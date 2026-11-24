import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { createBJournal, eventEvidence, safeDiagnostic, scopeEvidence } from './b-diagnostics.mjs';
import { userActionKey } from '../snipe-fast-isolated/identity.mjs';
const wallet = '0x' + '1'.repeat(40),
  tx = '0x' + '2'.repeat(64),
  now = 1800000000000;
const event = {
  topic: 'activity',
  type: 'trades',
  payload: {
    proxyWallet: wallet,
    transactionHash: tx,
    asset: 'asset-secret',
    side: 'BUY',
    size: 5,
    price: 0.5,
    timestamp: now / 1000,
    title: 'private title',
    token: 'SECRET_TOKEN',
    chat_id: 12345
  }
};
const fixture = (options) => {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'B-diag-')),
    directory = path.join(root, 'logs');
  const journal = createBJournal({
    directory,
    runId: 'test-b-diagnostics',
    now: () => now,
    minFreeBytes: 0,
    retentionMs: 0,
    ...options
  });
  return {
    root,
    directory,
    journal,
    rows: () =>
      fs
        .readdirSync(directory)
        .filter((x) => x.endsWith('.jsonl'))
        .sort()
        .flatMap((x) =>
          fs
            .readFileSync(path.join(directory, x), 'utf8')
            .trim()
            .split('\n')
            .filter(Boolean)
            .map((x) => JSON.parse(x))
        ),
    cleanup: () => fs.rmSync(root, { recursive: true, force: true })
  };
};
test('journal creation is inert; evidence uses exactly the shared action identity and contains no payload', async () => {
  const f = fixture();
  try {
    assert.equal(fs.existsSync(f.directory), false);
    const e = eventEvidence(event);
    assert.equal(e.action_key, userActionKey({ wallet, hash: tx, asset: 'asset-secret', side: 'BUY' }));
    assert.ok(e.representation_key);
    assert.equal(scopeEvidence(new Set([wallet])).wallet_tags.length, 1);
    await f.journal.start();
    f.journal.record({
      kind: 'B_EVENT_RECEIVED',
      at_ms: now,
      ...e,
      payload: event.payload,
      token: 'SECRET_TOKEN',
      recipient: 12345
    });
    await f.journal.close();
    const text = JSON.stringify(f.rows());
    for (const x of [wallet, tx, 'asset-secret', 'private title', 'SECRET_TOKEN', '12345'])
      assert.ok(!text.includes(x));
    assert.equal(fs.statSync(f.directory).mode & 0o777, 0o700);
    for (const x of fs.readdirSync(f.directory))
      assert.equal(fs.statSync(path.join(f.directory, x)).mode & 0o777, 0o600);
    assert.equal(f.journal.snapshot().written, 1);
    assert.equal(f.journal.snapshot().coverage_complete, true);
  } finally {
    await f.journal.close();
    f.cleanup();
  }
});
test('rotation caps managed files/bytes, reports retention loss and never touches unrelated files', async () => {
  const f = fixture({
    maxRecordBytes: 1024,
    maxQueueBytes: 8192,
    maxFileBytes: 1024,
    maxFiles: 2,
    maxTotalBytes: 2048
  });
  try {
    await f.journal.start();
    fs.writeFileSync(path.join(f.directory, 'unrelated.txt'), 'untouched');
    for (let i = 0; i < 30; i++) {
      f.journal.record({ kind: 'B_STATE', state: 'RUNNING', at_ms: now + i });
      await f.journal.flush();
    }
    await f.journal.close();
    assert.ok(f.journal.snapshot().retained_bytes <= 2048);
    assert.ok(f.journal.snapshot().retained_files <= 2);
    assert.ok(f.journal.snapshot().pruned_files > 0);
    assert.equal(f.journal.snapshot().coverage_complete, false);
    assert.equal(fs.readFileSync(path.join(f.directory, 'unrelated.txt'), 'utf8'), 'untouched');
    for (const x of fs.readdirSync(f.directory).filter((x) => x.endsWith('.jsonl')))
      assert.ok(fs.statSync(path.join(f.directory, x)).size <= 1024);
  } finally {
    await f.journal.close();
    f.cleanup();
  }
});
test('a stalled disk keeps bounded memory, never blocks record(), and emits an explicit gap after recovery', async () => {
  let release, entered;
  const wait = new Promise((r) => (release = r)),
    seen = new Promise((r) => (entered = r));
  let stalled = true,
    writes = 0;
  const f = fixture({
    maxRecordBytes: 1024,
    maxQueueBytes: 2048,
    writeBytes: async (h, b) => {
      if (++writes > 1 && stalled) {
        entered();
        await wait;
        stalled = false;
      }
      await h.writeFile(b);
    }
  });
  try {
    await f.journal.start();
    f.journal.record({ kind: 'B_STATE', state: 'RUNNING', at_ms: now });
    await seen;
    for (let i = 0; i < 100; i++) f.journal.record({ kind: 'B_STATE', state: 'RUNNING', at_ms: now + i });
    assert.ok(f.journal.snapshot().queue_bytes <= 2048);
    assert.ok(f.journal.snapshot().dropped > 0);
    assert.equal(f.journal.snapshot().disabled, false);
    release();
    await f.journal.flush();
    f.journal.record({ kind: 'B_STATE', state: 'STOPPED', at_ms: now + 200 });
    await f.journal.close();
    const rows = f.rows();
    assert.ok(rows.some((x) => x.kind === 'B_LOG_GAP' && x.dropped_records > 0));
    assert.equal(rows.filter((x) => x.kind === 'B_STATE').at(-1).state, 'STOPPED');
    assert.equal(f.journal.snapshot().coverage_complete, false);
    for (let i = 1; i < rows.length; i++) assert.ok(rows[i].seq > rows[i - 1].seq);
  } finally {
    release();
    await f.journal.close();
    f.cleanup();
  }
});
test('disk failures disable only the journal and surface loss without throwing into the receiver', async () => {
  const f = fixture({
    writeBytes: async (h, b) => {
      if (String(b).includes('B_STATE')) throw Error('secret filesystem path');
      await h.writeFile(b);
    }
  });
  try {
    await f.journal.start();
    assert.doesNotThrow(() => f.journal.record({ kind: 'B_STATE', state: 'RUNNING', at_ms: now }));
    await f.journal.flush();
    assert.equal(f.journal.snapshot().disabled, true);
    assert.equal(f.journal.snapshot().last_error, 'JOURNAL_WRITE_FAILED');
    assert.equal(f.journal.snapshot().dropped, 1);
    assert.equal(f.journal.record({ kind: 'B_STATE', state: 'RUNNING' }), false);
    assert.ok(!JSON.stringify(f.journal.snapshot()).includes('secret'));
  } finally {
    await f.journal.close();
    f.cleanup();
  }
});
test('unsafe directories, symlinks and low disk reserve produce visible initialization failures', async () => {
  for (const mode of ['symlink', 'public', 'reserve']) {
    const f = fixture({ minFreeBytes: mode === 'reserve' ? Number.MAX_SAFE_INTEGER : 0 });
    try {
      if (mode === 'symlink') fs.symlinkSync(f.root, f.directory);
      if (mode === 'public') {
        fs.mkdirSync(f.directory, { mode: 0o755 });
        fs.chmodSync(f.directory, 0o755);
      }
      await f.journal.start();
      assert.equal(f.journal.snapshot().disabled, true);
      assert.equal(f.journal.snapshot().coverage_complete, false);
    } finally {
      await f.journal.close();
      f.cleanup();
    }
  }
});
test('invalid and oversized records are accounted for without payload or exception-message leakage', async () => {
  const f = fixture({ maxRecordBytes: 1024, maxQueueBytes: 4096 });
  try {
    await f.journal.start();
    assert.equal(f.journal.record({ kind: 'SECRET_KIND', message: 'secret' }), false);
    assert.equal(f.journal.record({ kind: 'B_SCOPE', wallet_tags: Array(30).fill('a'.repeat(64)) }), false);
    assert.equal(f.journal.record({ kind: 'B_STATE', state: 'RUNNING', message: 'SECRET_VALUE' }), true);
    await f.journal.close();
    assert.equal(f.journal.snapshot().dropped, 2);
    assert.ok(f.rows().some((x) => x.kind === 'B_LOG_GAP'));
    assert.ok(!JSON.stringify(f.rows()).includes('SECRET_VALUE'));
    assert.throws(() => safeDiagnostic({ kind: 'unknown' }));
  } finally {
    await f.journal.close();
    f.cleanup();
  }
});
test('a new execution retains the previous private journal and never resumes its sequence', async () => {
  const f = fixture();
  let second;
  try {
    await f.journal.start();
    f.journal.record({ kind: 'B_STATE', state: 'RUNNING' });
    await f.journal.close();
    second = createBJournal({
      directory: f.directory,
      runId: 'test-b-diagnostics',
      minFreeBytes: 0,
      now: () => now
    });
    await second.start();
    second.record({ kind: 'B_STATE', state: 'RUNNING' });
    await second.close();
    const rows = f.rows(),
      states = rows.filter((x) => x.kind === 'B_STATE');
    assert.equal(states.length, 2);
    assert.notEqual(states[0].execution_id, states[1].execution_id);
    assert.equal(rows.filter((x) => x.kind === 'B_LOG_OPENED').length, 2);
    assert.equal(rows.filter((x) => x.kind === 'B_LOG_CLOSED').length, 2);
    assert.equal(states[0].seq, 2);
    assert.equal(states[1].seq, 2);
  } finally {
    await second?.close();
    await f.journal.close();
    f.cleanup();
  }
});
test('invalid numeric/source identity and private free text never enter the journal schema', () => {
  const x = safeDiagnostic({
    kind: 'B_RTDS_LIFECYCLE',
    generation: NaN,
    connection_id: wallet,
    frame_id: 'secret',
    reason: 'https://secret',
    message: wallet,
    raw_payload: tx,
    source: 'A',
    rtt_ms: null,
    correlation: 'PROCESS_ONLY_GENERATION_UNKNOWN'
  });
  assert.deepEqual(x, {
    kind: 'B_RTDS_LIFECYCLE',
    source: 'B',
    correlation: 'PROCESS_ONLY_GENERATION_UNKNOWN',
    rtt_ms: null
  });
});
test('background traffic is compact while every tracked decision and scope boundary stays detailed', async () => {
  let at = now;
  const f = fixture({ now: () => at, retentionMs: 48 * 3600000 });
  try {
    await f.journal.start();
    for (let i = 0; i < 50000; i++) {
      at = now + Math.floor(i / 1000) * 1000;
      f.journal.record({
        kind: 'B_EVENT_DROPPED',
        reason: 'OUTSIDE_ACTIVE_SCOPE',
        at_ms: at,
        frame_seq: i + 1,
        generation: 1,
        scope_version: i < 25000 ? 0 : 1,
        type: i % 2 ? 'trades' : 'orders_matched',
        wallet_tag: 'a'.repeat(64),
        action_key: 'b'.repeat(64)
      });
      if (i % 5000 === 0) {
        f.journal.record({ kind: 'B_EVENT_RECEIVED', at_ms: at, action_key: String(i).padStart(64, '0') });
        await f.journal.flush();
      }
      if (i === 24999) f.journal.record({ kind: 'B_SCOPE', at_ms: at, scope_version: 1, wallet_count: 1 });
    }
    f.journal.record({
      kind: 'B_EVENT_DROPPED',
      reason: 'BEFORE_RECONNECT_BOUNDARY',
      at_ms: at,
      action_key: 'c'.repeat(64)
    });
    await f.journal.close();
    const rows = f.rows(),
      background = rows.filter((x) => x.kind === 'B_BACKGROUND_ACTIVITY');
    assert.equal(
      background.reduce((n, x) => n + x.event_count, 0),
      50000
    );
    assert.ok(background.length < 150);
    assert.equal(rows.filter((x) => x.kind === 'B_EVENT_RECEIVED').length, 10);
    assert.equal(rows.filter((x) => x.kind === 'B_EVENT_DROPPED').length, 1);
    assert.ok(background.every((x) => !('action_key' in x) && !('wallet_tag' in x)));
    assert.equal(f.journal.snapshot().coverage_complete, true);
    assert.equal(f.journal.snapshot().background_pending_events, 0);
  } finally {
    await f.journal.close();
    f.cleanup();
  }
});
test('full budget cannot delete logs younger than the protected 48h window', async () => {
  const f = fixture({
    maxRecordBytes: 512,
    maxQueueBytes: 4096,
    maxFileBytes: 1024,
    maxFiles: 2,
    maxTotalBytes: 2048,
    retentionMs: 48 * 3600000
  });
  try {
    await f.journal.start();
    const initial = fs.readdirSync(f.directory)[0];
    for (let i = 0; i < 100 && !f.journal.snapshot().disabled; i++) {
      f.journal.record({ kind: 'B_STATE', state: 'RUNNING', at_ms: now + i });
      await f.journal.flush();
    }
    const s = f.journal.snapshot();
    assert.equal(s.disabled, true);
    assert.equal(s.pruned_files, 0);
    assert.equal(s.last_error, 'JOURNAL_PROTECTED_RETENTION_BUDGET');
    assert.equal(s.coverage_complete, false);
    assert.ok(fs.existsSync(path.join(f.directory, initial)));
  } finally {
    await f.journal.close();
    f.cleanup();
  }
});
test('files older than 48h may be pruned while current protected evidence is retained', async () => {
  let at = now;
  const f = fixture({
    now: () => at,
    mono: () => at - now,
    maxRecordBytes: 512,
    maxQueueBytes: 4096,
    maxFileBytes: 1024,
    maxFiles: 2,
    maxTotalBytes: 2048,
    retentionMs: 48 * 3600000
  });
  try {
    await f.journal.start();
    for (let i = 0; i < 4; i++) {
      f.journal.record({ kind: 'B_STATE', state: 'RUNNING', at_ms: at });
      await f.journal.flush();
    }
    at += 49 * 3600000;
    for (let i = 0; i < 10 && !f.journal.snapshot().pruned_files; i++) {
      f.journal.record({ kind: 'B_STATE', state: 'RUNNING', at_ms: at });
      await f.journal.flush();
    }
    assert.ok(f.journal.snapshot().pruned_files > 0);
    assert.equal(f.journal.snapshot().disabled, false);
  } finally {
    await f.journal.close();
    f.cleanup();
  }
});
test('a forward wall-clock jump cannot delete protected records before 48 monotonic hours', async () => {
  let at = now;
  const f = fixture({
    now: () => at,
    mono: () => 0,
    maxRecordBytes: 512,
    maxQueueBytes: 4096,
    maxFileBytes: 1024,
    maxFiles: 2,
    maxTotalBytes: 2048,
    retentionMs: 48 * 3600000
  });
  try {
    await f.journal.start();
    for (let i = 0; i < 4; i++) {
      f.journal.record({ kind: 'B_STATE', state: 'RUNNING', at_ms: at });
      await f.journal.flush();
    }
    at += 72 * 3600000;
    for (let i = 0; i < 30 && !f.journal.snapshot().disabled; i++) {
      f.journal.record({ kind: 'B_STATE', state: 'RUNNING', at_ms: at });
      await f.journal.flush();
    }
    assert.equal(f.journal.snapshot().pruned_files, 0);
    assert.equal(f.journal.snapshot().last_error, 'JOURNAL_PROTECTED_RETENTION_BUDGET');
  } finally {
    await f.journal.close();
    f.cleanup();
  }
});
test('retention protects the youngest record in a segment, not merely its filename timestamp', async () => {
  let at = now;
  const f = fixture({
    now: () => at,
    mono: () => at - now,
    maxRecordBytes: 512,
    maxQueueBytes: 4096,
    maxFileBytes: 1024,
    maxFiles: 2,
    maxTotalBytes: 2048,
    retentionMs: 48 * 3600000
  });
  try {
    await f.journal.start();
    f.journal.record({ kind: 'B_STATE', state: 'RUNNING', at_ms: at });
    await f.journal.flush();
    at = now + 47 * 3600000;
    for (let i = 0; i < 20 && f.journal.snapshot().retained_files < 2; i++) {
      f.journal.record({ kind: 'B_STATE', state: 'RUNNING', at_ms: at });
      await f.journal.flush();
    }
    at = now + 50 * 3600000;
    for (let i = 0; i < 30 && !f.journal.snapshot().disabled; i++) {
      f.journal.record({ kind: 'B_STATE', state: 'RUNNING', at_ms: at });
      await f.journal.flush();
    }
    assert.equal(f.journal.snapshot().pruned_files, 0);
    assert.equal(f.journal.snapshot().last_error, 'JOURNAL_PROTECTED_RETENTION_BUDGET');
  } finally {
    await f.journal.close();
    f.cleanup();
  }
});
