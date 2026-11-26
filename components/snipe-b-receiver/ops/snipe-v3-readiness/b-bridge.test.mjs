import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createHmac, createHash } from 'node:crypto';
import { createLocalFastIngress } from '../snipe-dual-rtds-ingress/ingress.mjs';
import { normalizePairs, scopeFingerprint } from '../snipe-dual-rtds-ingress/scope.mjs';
import { createBBridgeReceiver, createBBridgeProducer } from './b-bridge.mjs';
const at = 1800000000000,
  runId = 'bridge-test-current-24',
  key = Buffer.alloc(32, 23),
  w = (n) => '0x' + n.toString(16).padStart(40, '0');
const scope = normalizePairs(
  [
    ...Array.from({ length: 21 }, (_, i) => ({
      wallet: w(i + 1),
      recipient: i < 14 ? 42 : 44,
      notify_from_ts: 100
    })),
    ...[2, 5, 11].map((n) => ({ wallet: w(n), recipient: 43, notify_from_ts: 200 }))
  ],
  { exact14: false }
);
const fp = scopeFingerprint(scope, { exact14: false }),
  approval = {
    kind: 'FROZEN_CURRENT_SCOPE_SHADOW_ONLY',
    scope_pairs: 24,
    scope_wallets: 21,
    scope_fingerprint: fp,
    scope_sha256: 'a'.repeat(64),
    captured_at_ms: at - 1000,
    telegram_enabled: false,
    production_changes: false
  };
function setup({ snapshot = [] } = {}) {
  const directory = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'fast-dual-local-bridge-'))),
    owner = createLocalFastIngress({
      directory,
      scope,
      scopeApproval: approval,
      startMs: at,
      now: () => at + 1000,
      mono: () => 0
    });
  if (snapshot.length) assert.equal(owner.importLocalSnapshot(snapshot).status, 'LOCAL_SNAPSHOT_IMPORTED');
  let mono = 0;
  const receiver = createBBridgeReceiver({ runId, scopeFingerprint: fp, key, owner, mono: () => mono }),
    a = owner.openSource('A');
  let seq = 0;
  return {
    owner,
    receiver,
    a,
    setMono(x) {
      mono = x;
    },
    sendA(e) {
      return owner.submit({ ...a, message_id: 'a-' + ++seq, sequence: seq, envelope: e });
    },
    producer() {
      return createBBridgeProducer({ runId, scopeFingerprint: fp, key });
    },
    finish() {
      receiver.close();
      owner.close();
      fs.rmSync(directory, { recursive: true, force: true });
    }
  };
}
const envelope = (wallet, serial = 1, size = 1) => ({
  topic: 'activity',
  type: 'trades',
  payload: {
    proxyWallet: wallet,
    transactionHash: '0x' + serial.toString(16).padStart(64, '0'),
    asset: 'fixture-' + serial,
    side: 'BUY',
    size,
    price: 0.5,
    timestamp: at / 1000
  }
});
function connect(f) {
  const b = f.producer(),
    offer = f.receiver.challenge();
  assert.equal(f.receiver.authenticate(b.auth(offer)).status, 'CONNECTED');
  return b;
}
function signFrame(challenge, seq, id, payload) {
  const h = createHash('sha256').update(payload).digest('hex');
  return createHmac('sha256', key)
    .update(JSON.stringify(['B_BRIDGE_FRAME_V1', runId, fp, challenge, seq, id, h]))
    .digest('hex');
}
test('24 subscriptions share one owner, suppress actual A/B races and never send', () => {
  const f = setup();
  try {
    const b = connect(f);
    for (const wallet of new Set(scope.map((p) => p.wallet))) {
      const e = envelope(wallet);
      const frame = b.frame(e);
      assert.equal(f.sendA(e).status, 'ACCEPTED');
      const ack = f.receiver.receive(frame);
      assert.equal(ack.status, 'DUPLICATE');
      assert.equal(b.acknowledge(ack).ready, true);
    }
    assert.equal(f.owner.summary().queue_rows, 24);
    assert.equal(f.owner.summary().action_recipient_reservations, 24);
    let n = 0;
    for (;;) {
      const c = f.owner.claimForSimulation();
      if (c.status === 'EMPTY') break;
      assert.equal(f.owner.finishSimulation(c).status, 'WOULD_SEND');
      n++;
    }
    assert.equal(n, 24);
    assert.equal(f.owner.summary().telegram_sends, 0);
    b.close();
  } finally {
    f.finish();
  }
});
test('lost ACK retransmits exactly the same frame and creates no second reservation', () => {
  const f = setup();
  try {
    const b = connect(f),
      frame = b.frame(envelope(w(1)));
    assert.throws(() => b.frame(envelope(w(3))), /PENDING_ACK/);
    assert.equal(f.receiver.receive(frame).status, 'ACCEPTED');
    const retry = f.receiver.receive(b.retransmit());
    assert.equal(retry.status, 'ALREADY_COMMITTED');
    assert.equal(b.acknowledge(retry).ready, true);
    assert.equal(f.owner.summary().queue_rows, 1);
    b.close();
  } finally {
    f.finish();
  }
});
test('invalid key, expired challenge, forged ACK or mutated payload fail closed', () => {
  const f = setup();
  try {
    const evil = createBBridgeProducer({ runId, scopeFingerprint: fp, key: Buffer.alloc(32, 9) }),
      offer = f.receiver.challenge();
    assert.equal(f.receiver.authenticate(evil.auth(offer)).reason, 'BRIDGE_AUTH_INVALID');
    evil.close();
    const b = f.producer(),
      late = f.receiver.challenge(),
      auth = b.auth(late);
    f.setMono(2000.01);
    assert.equal(f.receiver.authenticate(auth).reason, 'BRIDGE_AUTH_INVALID');
    b.close();
    f.setMono(0);
    const good = connect(f),
      frame = good.frame(envelope(w(1)));
    const valid = f.receiver.receive(frame);
    assert.throws(() => good.acknowledge({ ...valid, status: 'DUPLICATE' }), /ACK_INVALID/);
    assert.equal(good.acknowledge(valid).ready, true);
    const second = good.frame(envelope(w(3)));
    assert.equal(
      f.receiver.receive({ ...second, payload_json: second.payload_json + ' ' }).reason,
      'BRIDGE_FRAME_INVALID'
    );
    assert.equal(f.sendA(envelope(w(4))).status, 'ACCEPTED');
    good.close();
  } finally {
    f.finish();
  }
});
test('authenticated sequence gaps and changed same-sequence replay disconnect B only', () => {
  const f = setup();
  try {
    let b = f.producer(),
      original;
    // Generate an authenticated but out-of-order sequence.
    const offer = f.receiver.challenge();
    assert.equal(f.receiver.authenticate(b.auth(offer)).status, 'CONNECTED');
    original = b.frame(envelope(w(1)));
    const gap = { ...original, sequence: 2 };
    gap.mac = signFrame(offer.nonce, 2, gap.message_id, gap.payload_json);
    assert.equal(f.receiver.receive(gap).reason, 'BRIDGE_SEQUENCE_GAP');
    assert.equal(f.sendA(envelope(w(3))).status, 'ACCEPTED');
    b.close();
    b = f.producer();
    const second = f.receiver.challenge();
    assert.equal(f.receiver.authenticate(b.auth(second)).status, 'CONNECTED');
    const first = b.frame(envelope(w(4)));
    assert.equal(f.receiver.receive(first).status, 'ACCEPTED');
    const changed = { ...first, payload_json: JSON.stringify(envelope(w(5))) };
    changed.mac = signFrame(second.nonce, changed.sequence, changed.message_id, changed.payload_json);
    assert.equal(f.receiver.receive(changed).reason, 'BRIDGE_REPLAY_CONFLICT');
    b.close();
  } finally {
    f.finish();
  }
});
test('loss of B cannot make stale packets eligible; A keeps receiving after disconnect', () => {
  const f = setup();
  try {
    const b = connect(f),
      packet = b.frame(envelope(w(1)));
    f.receiver.disconnect();
    assert.equal(f.receiver.receive(packet).reason, 'BRIDGE_NOT_CONNECTED');
    assert.equal(f.sendA(envelope(w(4))).status, 'ACCEPTED');
    b.close();
    const resumed = connect(f),
      fresh = resumed.frame(envelope(w(4)));
    assert.equal(f.receiver.receive(fresh).status, 'DUPLICATE');
    resumed.close();
    assert.equal(f.owner.summary().queue_rows, 1);
  } finally {
    f.finish();
  }
});
test('an existing UNKNOWN recipient is held without blocking unrelated B actions', () => {
  const target = envelope(w(2));
  const f = setup({
    snapshot: [{ payload: target.payload, recipient: 43, state: 'DELIVERY_UNKNOWN', originVerified: true }]
  });
  try {
    const b = connect(f),
      frame = b.frame(target),
      ack = f.receiver.receive(frame);
    assert.equal(ack.status, 'ACCEPTED');
    assert.equal(ack.blocked_pairs, true);
    assert.deepEqual(b.acknowledge(ack), {
      status: 'PROCESSED_WITH_ATTENTION',
      ready: true,
      attention: true
    });
    assert.equal(f.owner.summary().delivery_states.DELIVERY_UNKNOWN, 1);
    const next = b.frame(envelope(w(3), 2));
    assert.equal(f.receiver.receive(next).status, 'ACCEPTED');
    b.close();
  } finally {
    f.finish();
  }
});
