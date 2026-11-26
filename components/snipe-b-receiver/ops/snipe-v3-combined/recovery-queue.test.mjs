// Real private SQLite + authenticated in-memory B stream. No sender or network.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Duplex } from 'node:stream';
import { RtdsPeer } from '../snipe-rtds-resilient/connection.mjs';
import { createLocalFastIngress, MAX_STATE_AGE_MS } from '../snipe-dual-rtds-ingress/ingress.mjs';
import { scopeFingerprint } from '../snipe-dual-rtds-ingress/scope.mjs';
import { createBBridgeReceiver, createBBridgeProducer } from '../snipe-v3-readiness/b-bridge.mjs';
import { createBRelay, serveBChannel } from '../snipe-v3-readiness/bridge-stream.mjs';
import { createRemoteB } from './b-receiver.mjs';
import { createBJournal } from './b-diagnostics.mjs';
const settle = () => new Promise((r) => setImmediate(r)),
  BASE = 1800000000000;
function pair() {
  let a, b;
  const side = (get) =>
    new Duplex({
      read() {},
      write(bytes, _encoding, cb) {
        if (!get().destroyed) get().push(Buffer.from(bytes));
        cb();
      }
    });
  a = side(() => b);
  b = side(() => a);
  return [a, b];
}
class Socket {
  constructor() {
    this.listeners = new Map();
    this.confirm = true;
    this.exited = false;
  }
  addEventListener(k, f) {
    if (!this.listeners.has(k)) this.listeners.set(k, new Set());
    this.listeners.get(k).add(f);
  }
  removeEventListener(k, f) {
    this.listeners.get(k)?.delete(f);
  }
  emit(k, e = {}) {
    for (const f of [...(this.listeners.get(k) ?? [])]) f(e);
  }
  send() {}
  open() {
    this.emit('open');
  }
  frame(e) {
    this.emit('message', { data: JSON.stringify(e) });
  }
  close() {
    if (this.confirm) {
      this.exited = true;
      this.emit('close', { code: 1000 });
    }
  }
}
test('A hard-close recovery, B health and journal retain one queue writer, UNKNOWN and source independence', async () => {
  const directory = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'fast-dual-local-recovery-'));
  const scope = Array.from({ length: 14 }, (_, i) => ({
    wallet: '0x' + String(i + 1).padStart(40, '0'),
    recipient: 42,
    notify_from_ts: BASE / 1000 - 1
  }));
  let m = 0;
  const owner = createLocalFastIngress({
    directory,
    scope,
    startMs: BASE,
    mono: () => m,
    now: () => BASE + m
  });
  const key = Buffer.alloc(32, 8),
    runId = 'offline-recovery-queue',
    fingerprint = scopeFingerprint(scope);
  const journal = createBJournal({ directory: path.join(directory, 'journal'), runId, minFreeBytes: 0 });
  await journal.start();
  let sourceB,
    sessionA,
    sequenceA = 0;
  const socketsA = [],
    socketsB = [];
  const receiver = createBBridgeReceiver({ runId, scopeFingerprint: fingerprint, key, owner, mono: () => m }),
    producer = createBBridgeProducer({ runId, scopeFingerprint: fingerprint, key });
  let applyScope;
  const [left, right] = pair(),
    relay = createBRelay({
      stream: right,
      producer,
      wallets: scope.map((p) => p.wallet),
      onScope: (values) => applyScope(values),
      getHealth: () => ({
        ...(sourceB?.status().peer ?? {
          phase: 'PREPARED',
          generation: 0,
          activity_frames: 0,
          last_valid_activity_age_ms: null
        }),
        journal_ok: true
      })
    }),
    server = serveBChannel({
      stream: left,
      receiver,
      healthEnabled: true,
      getWallets: () => scope.map((p) => p.wallet)
    });
  const A = new RtdsPeer({
    source: 'A',
    continuous: true,
    mono: () => m,
    wall: () => BASE + m,
    socketFactory: () => {
      const s = new Socket();
      socketsA.push(s);
      return s;
    },
    onOpen: () => {
      sessionA = owner.openSource('A');
      sequenceA = 0;
    },
    onDisconnect: () => {
      if (sessionA) owner.disconnectSource(sessionA);
      sessionA = null;
    },
    onActivity: (envelope) =>
      owner.submit({ ...sessionA, sequence: ++sequenceA, message_id: 'A-' + sequenceA, envelope })
  });
  sourceB = createRemoteB({
    scope,
    continuous: true,
    ssh: {
      start: ({ onScope }) => {
        applyScope = onScope;
        return relay;
      },
      close() {}
    },
    socketFactory: () => {
      const s = new Socket();
      socketsB.push(s);
      return s;
    },
    mono: () => m,
    wall: () => BASE + m,
    onDiagnostic: journal.record
  });
  const packet = (n) => ({
    topic: 'activity',
    type: 'trades',
    payload: {
      proxyWallet: scope[0].wallet,
      transactionHash: '0x' + String(n).padStart(64, '0'),
      asset: 'fixture',
      side: 'BUY',
      size: 1,
      price: 0.5,
      timestamp: Math.floor((BASE + m) / 1000)
    }
  });
  const drain = async () => {
    for (let i = 0; i < 100 && relay.status().pending; i++) await settle();
    await settle();
  };
  try {
    A.start();
    socketsA[0].open();
    await sourceB.start();
    await settle();
    assert.equal(sourceB.status().channel, 'READY');
    socketsB[0].open();
    const first = packet(1);
    socketsA[0].frame(first);
    socketsB[0].frame(first);
    await drain();
    const claim = owner.claimForSimulation();
    assert.equal(owner.finishSimulation(claim, { outcome: 'AMBIGUOUS' }).status, 'AMBIGUOUS');
    const old = socketsA[0],
      late = [...old.listeners.get('message')][0];
    let confirm;
    old.confirm = false;
    old.terminate = () =>
      new Promise(
        (resolve) =>
          (confirm = () => {
            old.exited = true;
            resolve(true);
          })
      );
    A.reconnect('TEST_HUNG_CLOSE');
    m = 2000;
    A.tick();
    await settle();
    assert.equal(A.phase, 'WAIT_ABORT');
    const second = packet(2);
    socketsB[0].frame(second);
    socketsB[0].frame(packet(1));
    await drain();
    assert.equal(owner.summary().queue_rows, 2);
    assert.equal(owner.summary().delivery_states.DELIVERY_UNKNOWN, 1);
    confirm();
    await settle();
    A.tick();
    assert.equal(old.exited, true);
    assert.equal(socketsA.length, 2);
    socketsA[1].open();
    socketsA[1].frame(second);
    socketsA[1].frame(packet(1));
    late({ data: JSON.stringify(packet(3)) });
    assert.equal(owner.summary().queue_rows, 2);
    assert.equal(owner.summary().action_recipient_reservations, 2);
    relay.close();
    sourceB.tick();
    await settle();
    assert.equal(sourceB.status().channel, 'RECONNECTING');
    socketsA[1].frame(packet(3));
    assert.equal(owner.summary().queue_rows, 3);
    assert.equal(owner.summary().delivery_states.DELIVERY_UNKNOWN, 1);
    assert.equal(owner.summary().telegram_sends, 0);
    assert.equal(owner.summary().REST_requests, 0);
    assert.equal(MAX_STATE_AGE_MS, 2000);
    await journal.flush();
    assert.equal(journal.snapshot().errors, 0);
  } finally {
    A.stop();
    sourceB.stop();
    relay.close();
    server.close();
    receiver.close();
    await journal.close();
    owner.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
