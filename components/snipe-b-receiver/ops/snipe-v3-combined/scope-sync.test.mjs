import test from 'node:test';
import assert from 'node:assert/strict';
import { createScopePublisher, createScopeSubscriber } from './scope-sync.mjs';
const key = Buffer.alloc(32, 7),
  runId = 'v3-test-session',
  nonce = 'a'.repeat(64),
  wallet = (n) => '0x' + n.toString(16).padStart(40, '0');
function fixture() {
  let applied = [];
  const p = createScopePublisher({ runId, nonce, key }),
    s = createScopeSubscriber({
      runId,
      nonce,
      key,
      apply: (w) => {
        applied = w;
      }
    });
  return { p, s, get: () => applied };
}
test('adding and removing a wallet applies a canonical authenticated set', () => {
  const f = fixture();
  f.s.receive(f.p.update([wallet(2), wallet(1)]));
  assert.deepEqual(f.get(), [wallet(1), wallet(2)]);
  f.s.receive(f.p.update([wallet(2), wallet(3)]));
  assert.deepEqual(f.get(), [wallet(2), wallet(3)]);
  f.s.receive(f.p.update([]));
  assert.deepEqual(f.get(), []);
});
test('replayed, reordered, modified or other-session sets never apply', () => {
  const f = fixture(),
    first = f.p.update([wallet(1)]);
  f.s.receive(first);
  assert.throws(() => f.s.receive(first));
  const next = f.p.update([wallet(2)]);
  assert.throws(() => f.s.receive({ ...next, wallets: [wallet(3)] }));
  const wrong = createScopeSubscriber({ runId, nonce: 'b'.repeat(64), key, apply: () => assert.fail() });
  assert.throws(() => wrong.receive(first));
  assert.deepEqual(f.get(), [wallet(1)]);
  f.s.receive(next);
  assert.deepEqual(f.get(), [wallet(2)]);
});
test('invalid wallet sets, duplicates and skipped updates fail closed', () => {
  const f = fixture();
  assert.throws(() => f.p.update([wallet(1), wallet(1)]));
  assert.throws(() => f.p.update(['not-a-wallet']));
  const first = f.p.update([wallet(1)]),
    second = f.p.update([wallet(2)]);
  assert.throws(() => f.s.receive(second));
  assert.deepEqual(f.get(), []);
  f.s.receive(first);
  f.s.receive(second);
});
test('an application failure cannot advance scope revision', () => {
  let fail = true;
  const p = createScopePublisher({ runId, nonce, key }),
    s = createScopeSubscriber({
      runId,
      nonce,
      key,
      apply: () => {
        if (fail) throw Error('APPLY_FAILED');
      }
    }),
    frame = p.update([wallet(1)]);
  assert.throws(() => s.receive(frame));
  fail = false;
  assert.equal(s.receive(frame).revision, 1);
});
test('closed sessions reject updates', () => {
  const f = fixture();
  f.p.close();
  assert.throws(() => f.p.update([]));
  f.s.close();
  assert.throws(() => f.s.receive({}));
});
