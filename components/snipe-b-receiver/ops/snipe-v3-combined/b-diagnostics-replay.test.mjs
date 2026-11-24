// Recorded A/B receipt layout, synthetic payload metadata, real private SQLite
// and authenticated in-memory transport. No network or Telegram implementation.
import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Duplex } from 'node:stream';
import { createHash } from 'node:crypto';
import { createLocalFastIngress } from '../snipe-dual-rtds-ingress/ingress.mjs';
import { scopeFingerprint } from '../snipe-dual-rtds-ingress/scope.mjs';
import { createBBridgeReceiver, createBBridgeProducer } from '../snipe-v3-readiness/b-bridge.mjs';
import { createBRelay, serveBChannel } from '../snipe-v3-readiness/bridge-stream.mjs';
import { createRemoteB } from './b-receiver.mjs';
import { createBJournal, eventEvidence } from './b-diagnostics.mjs';
const BASE = 1800000000000,
  w = (n) => '0x' + n.toString(16).padStart(40, '0');
function pair() {
  let a, b;
  const side = (get) =>
    new Duplex({
      read() {},
      write(chunk, _encoding, cb) {
        get().push(Buffer.from(chunk));
        cb();
      },
      final(cb) {
        get().push(null);
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
    this.sent = [];
  }
  addEventListener(k, f) {
    if (!this.listeners.has(k)) this.listeners.set(k, new Set());
    this.listeners.get(k).add(f);
  }
  removeEventListener(k, f) {
    this.listeners.get(k)?.delete(f);
  }
  emit(k, v = {}) {
    for (const f of this.listeners.get(k) ?? []) f(v);
  }
  send(s) {
    this.sent.push(s);
  }
  close() {
    this.emit('close', { code: 1000 });
  }
}
const settle = () => new Promise((r) => setImmediate(r));
test('94 recorded action identities: full B diagnostics preserve common A/B dedup, UNKNOWN fencing and A independence', async () => {
  const fixture = JSON.parse(
    fs.readFileSync(new URL('./fixtures/replay94-fixture.json', import.meta.url), 'utf8')
  );
  const directory = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), 'fast-dual-local-B-diagnostics-'));
  const scope = Array.from({ length: 14 }, (_, i) => ({
      wallet: w(i + 1),
      recipient: 42,
      notify_from_ts: BASE / 1000 - 1
    })),
    owner = createLocalFastIngress({
      directory,
      scope,
      startMs: BASE,
      now: () => BASE + 1000,
      mono: () => 0
    });
  const fp = scopeFingerprint(scope),
    runId = 'B-diagnostics-replay'.toLowerCase(),
    key = Buffer.alloc(32, 9),
    bridge = createBBridgeReceiver({ runId, scopeFingerprint: fp, key, owner, mono: () => 0 }),
    producer = createBBridgeProducer({ runId, scopeFingerprint: fp, key });
  const journal = createBJournal({ directory: path.join(directory, 'journal'), runId, minFreeBytes: 0 }),
    sockets = [],
    A = owner.openSource('A');
  let sequence = 0;
  await journal.start();
  let applyScope;
  const [left, right] = pair(),
    relay = createBRelay({
      stream: right,
      producer,
      wallets: scope.map((p) => p.wallet),
      onScope: (values) => applyScope(values),
      maxQueue: 128
    }),
    server = serveBChannel({ stream: left, receiver: bridge, getWallets: () => scope.map((p) => p.wallet) });
  const source = createRemoteB({
    scope,
    ssh: {
      start: ({ onScope }) => {
        applyScope = onScope;
        return relay;
      },
      close() {}
    },
    socketFactory: () => {
      const s = new Socket();
      sockets.push(s);
      return s;
    },
    mono: () => 0,
    wall: () => BASE + 1000,
    onDiagnostic: journal.record
  });
  try {
    await source.start();
    await settle();
    assert.equal(source.status().channel, 'READY');
    sockets[0].emit('open');
    const aliases = new Map(),
      byKey = new Map();
    for (const receipt of fixture.receipts) {
      if (!aliases.has(receipt.wallet_alias)) aliases.set(receipt.wallet_alias, w(aliases.size + 1));
      const payload = {
        proxyWallet: aliases.get(receipt.wallet_alias),
        transactionHash: '0x' + createHash('sha256').update(receipt.action_id).digest('hex'),
        asset: 'fixture-asset',
        side: 'BUY',
        size: receipt.source === 'B' ? 2 : 1,
        price: 0.5,
        timestamp: BASE / 1000 + 1
      };
      const envelope = { topic: 'activity', type: receipt.type, payload };
      byKey.set(eventEvidence(envelope).action_key, envelope);
      if (receipt.source === 'A')
        owner.submit({ ...A, sequence: ++sequence, message_id: 'A-' + sequence, envelope });
      else sockets[0].emit('message', { data: JSON.stringify(envelope) });
    }
    for (let i = 0; i < 1000 && relay.status().pending; i++) await settle();
    await settle();
    await journal.flush();
    assert.equal(owner.summary().unique_actions, 94);
    assert.equal(owner.summary().queue_rows, 94);
    assert.equal(owner.summary().action_recipient_reservations, 94);
    assert.equal(source.status().matched, 125);
    assert.equal(source.status().gaps, 0);
    const claim = owner.claimForSimulation();
    assert.equal(owner.finishSimulation(claim, { outcome: 'AMBIGUOUS' }).status, 'AMBIGUOUS');
    sockets[0].emit('message', { data: JSON.stringify(byKey.get(claim.action_key)) });
    for (let i = 0; i < 100 && relay.status().pending; i++) await settle();
    await settle();
    assert.equal(owner.summary().delivery_states.DELIVERY_UNKNOWN, 1);
    let simulated = 0;
    for (;;) {
      const c = owner.claimForSimulation();
      if (c.status === 'EMPTY') break;
      assert.equal(owner.finishSimulation(c).status, 'WOULD_SEND');
      simulated++;
    }
    assert.equal(simulated, 93);
    relay.close();
    source.tick();
    await settle();
    assert.equal(source.status().channel, 'RECONNECTING');
    const fresh = {
      ...byKey.values().next().value,
      payload: { ...byKey.values().next().value.payload, transactionHash: '0x' + 'f'.repeat(64) }
    };
    assert.equal(
      owner.submit({ ...A, sequence: ++sequence, message_id: 'A-' + sequence, envelope: fresh }).status,
      'ACCEPTED'
    );
    await journal.close();
    const rows = fs.readdirSync(path.join(directory, 'journal')).flatMap((n) =>
      fs
        .readFileSync(path.join(directory, 'journal', n), 'utf8')
        .trim()
        .split('\n')
        .map(JSON.parse)
    );
    assert.equal(rows.filter((x) => x.kind === 'B_EVENT_RECEIVED').length, 126);
    assert.equal(rows.filter((x) => x.kind === 'B_FORWARD_RESULT').length, 126);
    assert.ok(rows.some((x) => x.kind === 'B_FORWARD_RESULT' && x.owner_status === 'DUPLICATE'));
    assert.ok(rows.some((x) => x.kind === 'B_FORWARD_RESULT' && x.blocked_pairs === true));
    assert.equal(journal.snapshot().coverage_complete, true);
    assert.equal(owner.summary().telegram_sends, 0);
    assert.equal(owner.summary().REST_requests, 0);
  } finally {
    source.stop();
    relay.close();
    server.close();
    bridge.close();
    await journal.close();
    owner.close();
    fs.rmSync(directory, { recursive: true, force: true });
  }
});
