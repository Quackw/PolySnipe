import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { randomBytes } from 'node:crypto';
import { spawn } from 'node:child_process';
import { DatabaseSync } from 'node:sqlite';
import { createLocalFastIngress } from '../snipe-dual-rtds-ingress/ingress.mjs';
import { scopeFingerprint } from '../snipe-dual-rtds-ingress/scope.mjs';
import { createRemoteB } from './b-receiver.mjs';
import { prepareBSSH } from './b-ssh-client.mjs';
import { startFastBridge } from '../../../snipe-v3-combined/fast-bridge-server.mjs';
import { openBCaptureStore } from './b-capture-store.mjs';

const BASE = 1800000000000;
const scope = Array.from({ length: 14 }, (_, n) => ({
  wallet: '0x' + (n + 1).toString(16).padStart(40, '0'),
  recipient: 1,
  notify_from_ts: BASE / 1000
}));
const fingerprint = scopeFingerprint(scope);
const runId = 'b-channel-recovery-copy';
const temp = (prefix) => fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), prefix));
const pause = () => new Promise((resolve) => setTimeout(resolve, 10));
async function until(read, expected) {
  for (let i = 0; i < 200; i++) {
    if (read() === expected) return;
    await pause();
  }
  assert.equal(read(), expected);
}
const envelope = (n, at, wallet = scope[0].wallet) => ({
  topic: 'activity',
  type: 'trades',
  payload: {
    proxyWallet: wallet,
    transactionHash: '0x' + n.toString(16).padStart(64, '0'),
    asset: 'fixture',
    side: 'BUY',
    size: 1,
    price: 0.5,
    timestamp: Math.floor(at / 1000)
  }
});
function captureRows(directory) {
  const db = new DatabaseSync(path.join(directory, 'channel-capture.sqlite'), { readOnly: true });
  try {
    return db.prepare('SELECT state, reason, envelope_json FROM capture_events ORDER BY id').all();
  } finally {
    db.close();
  }
}

// Child stdio -> real Unix socket and authenticated bridge; only the external
// SSH link and market socket are substituted. No IP sockets or sender.
const childProgram = String.raw`
import importlib.util, os, sys, types
spec = importlib.util.spec_from_file_location('bridge',sys.argv[1])
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)
bridge.DIRECTORY = sys.argv[2]
bridge.SOCKET = os.path.join(bridge.DIRECTORY,'rtds-b-ingress.sock')
bridge.pwd.getpwnam = lambda _: types.SimpleNamespace(pw_uid=os.geteuid())
sys.argv = ['bridge']
sys.exit(bridge.run())
`;

test('separate child channel loss preserves B, an unconfirmed ACK and offline events on a copied FAST queue', async () => {
  const seedDir = temp('fast-dual-local-seed-'),
    copyDir = temp('fast-dual-local-copy-'),
    bDir = temp('b-cap-');
  const bridgeDir = fs.mkdtempSync('/tmp/b-link-');
  const key = randomBytes(32);
  const file = 'local-fast-dual.sqlite';
  const makeStore = () =>
    openBCaptureStore({ directory: bDir, runId, scopeFingerprint: fingerprint, minFreeBytes: 0 });
  let m = 0,
    gateExpired = false,
    monoReads = 0,
    ownerReason,
    status = {},
    server,
    source,
    capture,
    owner,
    seed,
    options;
  let sockets = 0,
    closes = 0,
    attempts = 0,
    intercepted = false,
    cutAck = false,
    child,
    relay;
  const children = new Set(),
    results = [],
    mode = { scope: true };
  const ownerConfig = {
    scope,
    startMs: BASE,
    now: () => BASE + m,
    mono: () => (gateExpired && monoReads++ > 0 ? m + 2001 : m)
  };
  const packet = (n) => envelope(n, BASE + m);
  const tick = (elapsed = 0) => {
    m += elapsed;
    source.tick();
  };
  const fire = (e) =>
    options.onActivity(e, { received_mono_ms: m, received_wall_ms: BASE + m, generation: 1 });
  const startBridge = async () => {
    server = startFastBridge({
      directory: bridgeDir,
      keyPath: path.join(bridgeDir, 'bridge.key'),
      runId,
      scopeFingerprint: fingerprint,
      onState: (s) => {
        status.channel = s;
      },
      getWallets: mode.scope ? () => [scope[0].wallet] : null,
      owner: {
        ...owner,
        submit(frame) {
          const result = owner.submit(frame);
          ownerReason = result.reason;
          if (cutAck) {
            cutAck = false;
            intercepted = true;
            server.close();
          }
          return result;
        }
      }
    });
    await server.ready;
  };
  try {
    seed = createLocalFastIngress({ ...ownerConfig, directory: seedDir });
    const initial = seed.openSource('A');
    assert.equal(
      seed.submit({ ...initial, sequence: 1, message_id: 'seed', envelope: packet(1) }).status,
      'ACCEPTED'
    );
    assert.equal(
      seed.finishSimulation(seed.claimForSimulation(), { outcome: 'AMBIGUOUS' }).status,
      'AMBIGUOUS'
    );
    seed.close();
    seed = null;
    fs.copyFileSync(path.join(seedDir, file), path.join(copyDir, file));
    const seedBytes = fs.readFileSync(path.join(seedDir, file));
    owner = createLocalFastIngress({ ...ownerConfig, directory: copyDir, reopen: true });
    const a = owner.openSource('A');
    let seq = 0;
    const addA = (e) => owner.submit({ ...a, sequence: ++seq, message_id: 'A-' + seq, envelope: e });

    for (const name of ['identity', 'known_hosts'])
      fs.writeFileSync(path.join(bDir, name), 'fixture', { mode: 0o600 });
    fs.writeFileSync(path.join(bridgeDir, 'bridge.key'), key, { mode: 0o600 });
    capture = makeStore();
    const ssh = prepareBSSH({
      host: 'fixture.invalid',
      user: 'fixture',
      runId,
      scopeFingerprint: fingerprint,
      key,
      wallets: scope.map((p) => p.wallet),
      identityFile: path.join(bDir, 'identity'),
      knownHostsFile: path.join(bDir, 'known_hosts'),
      spawn(exe, args, spawnOptions) {
        assert.equal(exe, '/usr/bin/ssh');
        assert.ok(args.includes('StrictHostKeyChecking=yes'));
        assert.equal(children.size, 0, 'only one child may be running');
        attempts++;
        child = spawn(
          'python3',
          [
            '-IB',
            '-c',
            childProgram,
            path.resolve('components/snipe-v3-combined/bridge-stdio.py'),
            bridgeDir
          ],
          spawnOptions
        );
        children.add(child);
        child.once('exit', () => children.delete(child));
        return child;
      }
    });
    key.fill(0); // Entrypoint clears its source key as well.
    class Peer {
      constructor(o) {
        options = o;
        sockets++;
      }
      start() {
        options.onOpen();
      }
      tick() {}
      stop() {
        closes++;
      }
      health() {
        return { phase: 'OPEN', generation: 1 };
      }
    }
    source = createRemoteB({
      scope,
      ssh: {
        start(args) {
          relay = ssh.start(args);
          return relay;
        },
        close: ssh.close
      },
      capture,
      mono: () => m,
      wall: () => BASE + m,
      Peer,
      socketFactory: () => assert.fail('external socket'),
      onDiagnostic: (row) => results.push(row)
    });

    // Startup without FAST must not prevent capture. It must also survive two
    // subsequent authenticated sessions, one of which lacks a scope snapshot.
    await source.start();
    fire(packet(2));
    await until(() => children.size, 0);
    tick();
    assert.equal(source.status().state, 'RUNNING');
    assert.equal(captureRows(bDir)[0].state, 'CAPTURED_ONLY');

    mode.scope = false;
    await startBridge();
    tick(250);
    await until(() => source.status().channel, 'WAIT_SCOPE');
    fire(packet(3));
    assert.equal(owner.summary().queue_rows, 1);
    server.close();
    await until(() => children.size, 0);
    tick();
    mode.scope = true;
    await startBridge();
    tick(500);
    await until(() => ['READY', 'CLOSED'].includes(source.status().channel), true);
    assert.equal(
      source.status().channel,
      'READY',
      JSON.stringify({ channel: relay.status(), attempts, m, status })
    );
    assert.equal(owner.summary().queue_rows, 1, 'offline rows were never replayed');

    cutAck = true;
    fire(packet(4));
    await until(() => intercepted, true);
    await until(() => capture.snapshot().counts.ACK_UNCERTAIN, 1);
    assert.equal(owner.summary().queue_rows, 2, 'FAST committed although the ACK did not arrive');
    await until(() => children.size, 0);
    tick();
    fire(packet(5));
    assert.equal(addA(packet(6)).status, 'ACCEPTED');
    await startBridge();
    tick(250);
    await until(() => ['READY', 'CLOSED'].includes(source.status().channel), true);
    assert.equal(
      source.status().channel,
      'READY',
      JSON.stringify({ channel: relay.status(), attempts, m, status })
    );
    assert.equal(attempts, 4);
    assert.equal(sockets, 1);
    assert.equal(closes, 0);
    assert.equal(source.status().peer.generation, 1);
    assert.equal(status.channel, 'B_AUTHENTICATED');
    assert.equal(owner.summary().queue_rows, 3);

    fire(packet(6)); // A/B duplicate
    fire(packet(1)); // Previously UNKNOWN
    fire(packet(7)); // New B-only action
    fire(envelope(8, BASE + m, scope[1].wallet)); // Removed in signed snapshot
    await until(() => owner.summary().queue_rows, 4);
    await until(() => capture.snapshot().counts.PENDING_ACK, undefined);
    assert.equal(owner.summary().delivery_states.DELIVERY_UNKNOWN, 1);
    assert.equal(owner.summary().action_recipient_reservations, 4);
    assert.deepEqual(capture.snapshot().counts, { ACK_UNCERTAIN: 1, CAPTURED_ONLY: 3, OWNER_ATTENTION: 1 });
    assert.equal(results.filter((x) => x.kind === 'B_FORWARD_REQUESTED').length, 4);

    gateExpired = true;
    fire(packet(9));
    await until(() => capture.snapshot().counts.ACK_UNCERTAIN, 2);
    assert.equal(ownerReason, 'STATE_TOO_STALE');
    assert.equal(owner.summary().queue_rows, 4, '2001 ms delivery state did not create a queue entry');
    assert.equal(owner.summary().telegram_sends, 0);
    assert.equal(owner.summary().REST_requests, 0);
    assert.ok(fs.readFileSync(path.join(seedDir, file)).equals(seedBytes));
    source.stop();
    capture.close();
    capture = makeStore();
    assert.deepEqual(capture.snapshot().counts, { ACK_UNCERTAIN: 2, CAPTURED_ONLY: 3, OWNER_ATTENTION: 1 });
  } finally {
    source?.stop();
    server?.close();
    for (const p of children) p.kill('SIGKILL');
    await until(() => children.size, 0);
    capture?.close();
    owner?.close();
    seed?.close();
    for (const dir of [seedDir, copyDir, bDir, bridgeDir]) fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('bounded capture reports unavailable storage and keeps prior rows and pending evidence on reopen', async () => {
  const directory = temp('b-cap-limit-');
  const config = { directory, runId, scopeFingerprint: fingerprint, maxEvents: 1, minFreeBytes: 0 };
  let capture, source, options;
  try {
    capture = openBCaptureStore(config);
    const evidence = {
      received_wall_ms: BASE,
      scope_version: 0,
      connection_id: null,
      generation: 1,
      frame_seq: 1,
      action_key: 'fixture'
    };
    const e = envelope(2, BASE);
    capture.save(e, evidence, true, null);
    assert.equal(fs.statSync(path.join(directory, 'channel-capture.sqlite')).mode & 0o777, 0o600);
    capture.close();
    capture = openBCaptureStore(config);
    assert.equal(capture.snapshot().interrupted, 1);
    const diagnostics = [];
    class Peer {
      constructor(o) {
        options = o;
      }
      start() {
        options.onOpen();
      }
      stop() {}
      health() {
        return { phase: 'OPEN' };
      }
    }
    source = createRemoteB({
      scope,
      ssh: {
        start() {
          throw Error('FIXTURE_LINK_DOWN');
        }
      },
      capture,
      socketFactory: () => assert.fail('external socket'),
      Peer,
      mono: () => 0,
      wall: () => BASE,
      onDiagnostic: (row) => diagnostics.push(row)
    });
    await source.start();
    options.onActivity(envelope(3, BASE));
    assert.equal(source.status().peer.phase, 'OPEN');
    assert.equal(source.status().capture.failures, 1);
    assert.ok(
      diagnostics.some(
        (x) =>
          x.kind === 'B_CAPTURE_RESULT' && x.state === 'CAPTURE_INCOMPLETE' && x.reason === 'B_CAPTURE_FULL'
      )
    );
    const rows = captureRows(directory);
    assert.equal(rows.length, 1);
    assert.equal(rows[0].state, 'ACK_UNCERTAIN');
    assert.equal(rows[0].reason, 'B_PROCESS_INTERRUPTED');
    assert.deepEqual(JSON.parse(rows[0].envelope_json), e);
  } finally {
    source?.stop();
    capture?.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
