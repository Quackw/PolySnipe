import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { setTimeout as delay } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import Database from 'better-sqlite3';
import { ensureSchema } from '../snipe-v2-runtime/src/db/schema.js';

const fast = fileURLToPath(new URL('./fast.mjs', import.meta.url));
const fixture = fileURLToPath(new URL('./startup-network-fixture.mjs', import.meta.url));
const sha256 = (data) => createHash('sha256').update(data).digest('hex');

function seedSubscriptions(root, since) {
  const seed = [
    { user: 101, chat: 101, type: 'private', granted: false, active: true },
    { user: 102, chat: 102, type: 'private', granted: true, active: true },
    { user: 103, chat: 103, type: 'private', granted: false, active: false },
    { user: 104, chat: -104, type: 'group', granted: true, active: false },
    { user: 105, chat: 106, type: 'private', granted: true, active: false }
  ];
  const db = new Database(path.join(root, 'state.db'));
  try {
    ensureSchema(db);
    const account = db.prepare('INSERT INTO tracked_accounts(proxy_address) VALUES(?)');
    const subscriber = db.prepare(
      'INSERT INTO subscribers(telegram_user_id, telegram_chat_id, chat_type) VALUES(?, ?, ?)'
    );
    const grant = db.prepare('INSERT INTO access_grants(telegram_user_id) VALUES(?)');
    const subscription = db.prepare(
      'INSERT INTO subscriptions(subscriber_id, telegram_chat_id, proxy_address, created_by_user_id, notify_from_ts) VALUES(?, ?, ?, ?, ?)'
    );
    return seed.flatMap(({ user, chat, type, granted, active }, i) => {
      const wallet = '0x' + String(i + 1).repeat(40);
      account.run(wallet);
      const { lastInsertRowid } = subscriber.run(user, chat, type);
      if (granted) grant.run(user);
      subscription.run(lastInsertRowid, chat, wallet, user, since);
      return active ? [{ wallet, recipient: chat, notify_from_ts: since }] : [];
    });
  } finally {
    db.close();
  }
}

test('the full FAST process starts with eligible subscriptions and reaches RTDS and Telegram polling', async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'fast-'));
  let child, done;
  t.after(async () => {
    if (child && child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    await done;
    fs.rmSync(root, { recursive: true, force: true });
  });

  const startMs = Date.now() - 60_000;
  const scope = seedSubscriptions(root, Math.floor(startMs / 1000));
  const fingerprint = sha256(JSON.stringify(scope));
  const scopeJson = JSON.stringify({ scope, fingerprint });
  function write(name, value) {
    fs.writeFileSync(path.join(root, name), JSON.stringify(value), { mode: 0o600 });
  }
  write('wallet-labels.json', {});
  write('ENABLE_CANARY', {});
  write('menu-backlog-cleared', {});
  write('redeem-config.json', { version: 1, start_ms: startMs, subscriptions: scope });
  write('send-once-canary.json', { version: 2, start_ms: startMs, subscriptions: scope });
  fs.writeFileSync(path.join(root, 'dual-rtds-scope.json'), scopeJson, { mode: 0o600 });
  write('rtds-b-bridge.json', { run_id: 'startup-fixture', scope_fingerprint: fingerprint });
  const bridgeDir = path.join(root, 'rtds-b-private');
  fs.mkdirSync(bridgeDir, { mode: 0o700 });
  fs.writeFileSync(path.join(bridgeDir, 'bridge.key'), Buffer.alloc(32, 1), { mode: 0o600 });

  let stdout = '';
  let stderr = '';
  child = spawn(process.execPath, ['--import', fixture, fast], {
    cwd: root,
    env: {
      POLYSNIPE_FAST_ENABLE: '1',
      POLYSNIPE_FAST_STATE_DIR: root,
      TELEGRAM_BOT_TOKEN: 'fixture',
      EXPECTED_TELEGRAM_USERNAME: 'fixture_bot',
      OWNER_TELEGRAM_USER_IDS: '101,107',
      POLYSNIPE_REDEEM_ENABLE: '0',
      POLYSNIPE_RECOVERY_ENABLE: '0',
      FAST_RTDS_RESILIENT: '1',
      FAST_RTDS_B_ENABLED: '1',
      FAST_DUAL_SCOPE_SHA256: sha256(scopeJson)
    },
    stdio: ['ignore', 'pipe', 'pipe']
  });
  done = new Promise((resolve) => {
    child.once('error', (error) => {
      stderr += String(error);
      resolve(null);
    });
    child.once('close', (code, signal) => resolve([code, signal]));
  });
  child.stdout.setEncoding('utf8').on('data', (chunk) => {
    stdout += chunk;
  });
  child.stderr.setEncoding('utf8').on('data', (chunk) => {
    stderr += chunk;
  });

  const healthFile = path.join(root, 'health.json');
  const deadline = performance.now() + 15_000;
  let health;
  while (performance.now() < deadline && child.exitCode === null && child.signalCode === null) {
    if (fs.existsSync(healthFile)) {
      health = JSON.parse(fs.readFileSync(healthFile, 'utf8'));
      if (
        health.rtds_peers.A.activity_frames > 0 &&
        health.rtds_peers.B.channel === 'B_CHANNEL_READY' &&
        stdout.includes('FIXTURE_TELEGRAM_GET_UPDATES')
      )
        break;
    }
    await delay(25);
  }
  const output = () => stdout + stderr;
  assert.equal(health?.rtds_peers.A.phase, 'OPEN', output());
  assert.ok(health.rtds_peers.A.activity_frames > 0, output());
  assert.equal(health.rtds_peers.B.channel, 'B_CHANNEL_READY', output());
  assert.match(stdout, /\[menu\] polling_active/);
  assert.match(stdout, /FIXTURE_TELEGRAM_GET_UPDATES/);
  assert.equal(health.subscriptions, scope.length);
  assert.equal(health.cohort_wallets, scope.length);
  assert.equal(health.send_once_canary.rollback, false);
  assert.equal(health.enabled, true);
  assert.equal(health.recovery_canary.enabled, false);
  assert.equal(health.activity_http_requests, 0);
  assert.equal(health.delivery_states.DELIVERED ?? 0, 0);

  assert.equal(child.kill('SIGTERM'), true);
  assert.deepEqual(await Promise.race([done, delay(6000, null, { ref: false })]), [0, null], output());
  assert.equal(JSON.parse(fs.readFileSync(healthFile, 'utf8')).final, true);
  assert.doesNotMatch(output(), /UNEXPECTED_NETWORK_IN_FAST_STARTUP_FIXTURE/);
});
