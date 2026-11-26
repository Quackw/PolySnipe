import assert from 'node:assert/strict';
import test from 'node:test';
import { createTelegramCircuit } from './telegram-circuit.mjs';
import { telegramRequest } from './telegram-transport.mjs';

test('destructured observation and snapshot work with missing and invalid results', () => {
  const { observe, ready, snapshot } = createTelegramCircuit({ now: () => 0 });
  assert.equal(ready(), true);
  const degraded = observe({ kind: 'AMBIGUOUS' });
  assert.equal(degraded.state, 'DEGRADED');
  assert.equal(degraded.consecutive_unknown, 1);
  for (const result of [null, undefined, 429, 'AMBIGUOUS', {}]) {
    assert.deepEqual(observe(result), degraded);
  }
  assert.deepEqual(snapshot(), degraded);
});

test('a Bot API 429 pauses the whole sender and later responses cannot shorten the pause', async () => {
  const result = await telegramRequest(
    'FIXTURE',
    'sendMessage',
    {},
    {
      fetchImpl: async () =>
        new Response(JSON.stringify({ ok: false, error_code: 429, parameters: { retry_after: 90 } }))
    }
  );
  let at = 0;
  const circuit = createTelegramCircuit({ now: () => at });
  assert.equal(circuit.observe(result).cooldown_remaining_ms, 90_000);
  assert.equal(circuit.ready(), false);

  at = 1000;
  assert.equal(
    circuit.observe({ kind: 'RETRYABLE', status: 429, retryAfterMs: 2000 }).cooldown_remaining_ms,
    89_000
  );
  assert.equal(circuit.observe({ kind: 'OK' }).cooldown_remaining_ms, 89_000);
  assert.equal(
    circuit.observe({ kind: 'RETRYABLE', status: 429, retryAfterMs: 95_000 }).cooldown_remaining_ms,
    95_000
  );
});

test('recipient-specific rejection does not open the circuit', () => {
  const circuit = createTelegramCircuit({ now: () => 0 });
  circuit.observe({ kind: 'RETRYABLE', status: 503 });
  circuit.observe({ kind: 'AMBIGUOUS', reason: 'ECONNRESET' });
  const health = circuit.observe({ kind: 'FINAL', status: 403 });
  assert.equal(health.state, 'READY');
  assert.equal(health.consecutive_failures, 0);
  assert.equal(health.consecutive_unknown, 0);
  assert.equal(health.total_unknown, 1);
  assert.equal(circuit.ready(), true);
});

test('one half-open probe at a time with release for a tick that cannot send', () => {
  let at = 0;
  const { observe, ready, snapshot, releaseProbe } = createTelegramCircuit({
    now: () => at,
    failureThreshold: 2,
    baseCooldownMs: 20,
    maxCooldownMs: 30
  });
  assert.equal(observe({ kind: 'RETRYABLE', status: 503 }).state, 'DEGRADED');
  assert.equal(observe({ kind: 'AMBIGUOUS', reason: 'TRANSPORT_TIMEOUT' }).cooldown_remaining_ms, 20);
  assert.equal(ready(), false);

  at = 20;
  assert.equal(snapshot().state, 'HALF_OPEN');
  assert.equal(ready(), true);
  assert.equal(ready(), false);
  assert.equal(observe(null).state, 'HALF_OPEN');
  assert.equal(ready(), false);
  releaseProbe();
  assert.equal(ready(), true);
  assert.equal(ready(), false);

  assert.equal(observe({ kind: 'RETRYABLE', status: 408 }).cooldown_remaining_ms, 30);
  assert.equal(ready(), false);
  at = 50;
  assert.equal(ready(), true);
  assert.equal(ready(), false);
  assert.equal(observe({ kind: 'OK' }).state, 'READY');
  assert.equal(snapshot().unknown_replay, false);
  assert.equal(ready(), true);
});
