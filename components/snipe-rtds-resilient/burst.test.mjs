import test from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as pause } from 'node:timers/promises';
import { BufferedRtdsSocket } from './buffered-socket.mjs';
import { ManagedRtdsSocket } from './managed-socket.mjs';
import { RtdsPeer, RTDS_URL } from './connection.mjs';
const url = new URL('./burst-worker-fixture.mjs', import.meta.url),
  command = (x) => JSON.stringify(x);
async function until(fn, reason, ms = 5000) {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (fn()) return;
    await pause(2);
  }
  throw Error(reason);
}
async function socket(options = {}) {
  const diagnostics = [],
    dispositions = [],
    s = new BufferedRtdsSocket(RTDS_URL, {
      workerURL: url,
      onDiagnostic: (r) => diagnostics.push(r),
      onDisposition: (r) => dispositions.push(r),
      ...options
    });
  await until(() => s.readyState === 1, 'OPEN_TIMEOUT');
  return { s, diagnostics, dispositions };
}
for (const count of [64, 65, 128, 512])
  test(`actual old/new worker comparison: ${count} watched BUY/SELL frames`, async () => {
    const old = new ManagedRtdsSocket(RTDS_URL, { workerURL: url });
    let oldReceived = 0,
      oldErrors = 0;
    old.addEventListener('message', () => oldReceived++);
    old.addEventListener('error', () => oldErrors++);
    await until(() => old.readyState === 1, 'OLD_OPEN_TIMEOUT');
    const f = await socket();
    const values = [];
    f.s.addEventListener('message', (e) => {
      values.push(JSON.parse(e.data));
      e.complete('PIPELINE_COMPLETED', 'COMMON_PIPELINE_RETURNED');
    });
    try {
      old.send(command({ fixture: 'BURST', count }));
      f.s.send(command({ fixture: 'BURST', count }));
      await until(() => f.s.snapshot().pipeline_completed === count, 'NEW_BURST_LOST');
      await until(() => oldReceived === Math.min(64, count), 'OLD_PREFIX_MISSING');
      assert.equal(oldErrors, count > 64 ? 1 : 0);
      assert.equal(values.length, count);
      assert.equal(new Set(values.map((x) => x.payload.transactionHash)).size, count);
      assert.ok(values.some((x) => x.payload.side === 'BUY'));
      assert.ok(values.some((x) => x.payload.side === 'SELL'));
      assert.equal(f.s.snapshot().discarded, 0);
      assert.equal(f.s.snapshot().unknown, 0);
      assert.equal(f.s.snapshot().incomplete_intervals, 0);
      assert.equal(f.s.readyState, 1);
      assert.equal(f.dispositions.length, count);
      assert.equal(f.s.snapshot().accepted_accounted, true);
    } finally {
      old.close();
      f.s.close();
      await Promise.all([old.terminate(), f.s.terminate()]);
    }
  });
test('delayed receiver keeps a 512-frame burst until explicit completion, without closing the socket', async () => {
  const f = await socket();
  let n = 0;
  f.s.addEventListener('message', (e) => {
    n++;
    if (n === 1) setTimeout(() => e.complete('PIPELINE_COMPLETED', 'COMMON_PIPELINE_RETURNED'), 100);
    else e.complete('PIPELINE_COMPLETED', 'COMMON_PIPELINE_RETURNED');
  });
  try {
    f.s.send(command({ fixture: 'BURST', count: 512 }));
    await until(() => f.s.snapshot().pipeline_completed === 512, 'DELAYED_BURST_LOST');
    await until(() => f.s.snapshot().worker?.accepted === 512, 'STATS_MISSING');
    const s = f.s.snapshot();
    assert.equal(s.discarded + s.unknown + s.rejected, 0);
    assert.ok(s.max_queue_wait_ms >= 80);
    assert.ok(s.max_queue_wait_ms < 2000);
    assert.equal(s.worker.max_buffer_frames, 512);
    assert.ok(s.worker.max_buffer_bytes <= 2 * 1024 ** 2);
    assert.equal(f.s.readyState, 1);
  } finally {
    f.s.close();
    await f.s.terminate();
  }
});
test('no consumer receipt is not acknowledged in finally and becomes an explicit timeout', async () => {
  const f = await socket({ limits: { ackTimeoutMs: 75 } });
  f.s.addEventListener('message', () => {});
  try {
    f.s.send(command({ fixture: 'BURST', count: 1 }));
    await f.s.exit;
    assert.ok(f.diagnostics.some((x) => x.reason === 'FRAME_RECEIPT_TIMEOUT'));
    assert.equal(f.s.snapshot().pipeline_completed, 0);
    assert.equal(f.s.snapshot().unknown, 1);
    assert.equal(f.s.snapshot().accepted_accounted, true);
  } finally {
    await f.s.terminate();
  }
});
test('prolonged overload classifies every announced frame and exposes its fenced input tail', async () => {
  const f = await socket({ limits: { maxFrames: 32 } });
  f.s.addEventListener('message', (e) =>
    setTimeout(() => e.complete('PIPELINE_COMPLETED', 'COMMON_PIPELINE_RETURNED'), 100)
  );
  try {
    f.s.send(command({ fixture: 'BURST', count: 1024 }));
    await f.s.exit;
    const s = f.s.snapshot();
    assert.equal(s.accepted, 32);
    assert.equal(s.rejected, 1);
    assert.equal(s.accepted_accounted, true);
    assert.equal(s.pipeline_completed + s.discarded + s.unknown, 32);
    assert.equal(f.dispositions.length, 33);
    assert.equal(s.worker.tail_after_fence, 991);
    assert.ok(f.diagnostics.some((x) => x.reason === 'FRAME_QUEUE_COUNT'));
    assert.ok(s.worker.max_buffer_bytes <= 2 * 1024 ** 2);
  } finally {
    await f.s.terminate();
  }
});
test('native close drains already accepted frames instead of fencing them prematurely', async () => {
  const f = await socket();
  f.s.addEventListener('message', (e) => e.complete('PIPELINE_COMPLETED', 'COMMON_PIPELINE_RETURNED'));
  try {
    f.s.send(command({ fixture: 'BURST', count: 128 }));
    f.s.send(command({ fixture: 'NATIVE_CLOSE' }));
    await f.s.exit;
    assert.equal(f.s.snapshot().pipeline_completed, 128);
    assert.equal(f.s.snapshot().discarded + f.s.snapshot().unknown, 0);
    assert.equal(f.s.snapshot().accepted_accounted, true);
  } finally {
    await f.s.terminate();
  }
});
test('native error drains accepted payloads before its classified reconnect', async () => {
  const f = await socket();
  f.s.addEventListener('message', (e) => e.complete('PIPELINE_COMPLETED', 'COMMON_PIPELINE_RETURNED'));
  try {
    f.s.send(command({ fixture: 'BURST', count: 128 }));
    f.s.send(command({ fixture: 'NATIVE_ERROR' }));
    await f.s.exit;
    assert.equal(f.s.snapshot().pipeline_completed, 128);
    assert.equal(f.s.snapshot().discarded + f.s.snapshot().unknown, 0);
    assert.equal(f.s.snapshot().last_error, 'NATIVE_SOCKET_ERROR');
  } finally {
    await f.s.terminate();
  }
});
test('default byte budget bounds sustained large bursts and reports every accepted frame outcome', async () => {
  const f = await socket();
  f.s.addEventListener('message', (e) =>
    setTimeout(() => e.complete('PIPELINE_COMPLETED', 'COMMON_PIPELINE_RETURNED'), 150)
  );
  try {
    f.s.send(command({ fixture: 'BURST', count: 10000, pad: 512 }));
    await f.s.exit;
    const s = f.s.snapshot();
    assert.equal(s.last_error, 'FRAME_QUEUE_BYTES');
    assert.ok(s.worker.max_buffer_bytes <= 2 * 1024 ** 2);
    assert.ok(s.max_pending_frames <= 4096);
    assert.equal(s.accepted_accounted, true);
    assert.equal(s.worker.native_frames, 10000);
    assert.equal(s.worker.accepted + s.worker.rejected + s.worker.tail_after_fence, 10000);
    assert.ok(s.worker.tail_after_fence > 0);
    assert.equal(f.dispositions.length, s.accepted + s.rejected);
  } finally {
    await f.s.terminate();
  }
});
test('pipeline exceptions are reported without false receipts or private exception text', async () => {
  const f = await socket();
  f.s.addEventListener('message', () => {
    throw Error('private-token-or-address');
  });
  try {
    f.s.send(command({ fixture: 'BURST', count: 65 }));
    await f.s.exit;
    assert.equal(f.s.snapshot().pipeline_completed, 0);
    assert.equal(f.s.snapshot().accepted_accounted, true);
    assert.ok(f.diagnostics.some((x) => x.reason === 'PIPELINE_HANDLER_THROW'));
    assert.ok(!JSON.stringify(f.diagnostics).includes('private-token-or-address'));
  } finally {
    await f.s.terminate();
  }
});
for (const [fixture, reason] of [
  ['LARGE', 'FRAME_TOO_LARGE'],
  ['NON_TEXT', 'FRAME_NON_TEXT'],
  ['NATIVE_ERROR', 'NATIVE_SOCKET_ERROR'],
  ['SEND_ERROR', 'NATIVE_SEND_ERROR']
])
  test(`worker fault is classified: ${reason}`, async () => {
    const f = await socket();
    f.s.addEventListener('message', (e) => e.complete('PIPELINE_COMPLETED', 'COMMON_PIPELINE_RETURNED'));
    try {
      f.s.send(command({ fixture }));
      await f.s.exit;
      assert.ok(f.diagnostics.some((x) => x.reason === reason) || f.s.snapshot().last_error === reason);
      assert.ok(!JSON.stringify(f.diagnostics).includes('private-token-or-address'));
      assert.equal(f.s.snapshot().accepted_accounted, true);
    } finally {
      await f.s.terminate();
    }
  });
test('worker crash accounts announced pending frames without inventing delivery success', async () => {
  const f = await socket();
  f.s.addEventListener('message', () => {});
  try {
    f.s.send(command({ fixture: 'BURST', count: 512 }));
    await until(() => f.s.snapshot().accepted === 512, 'ANNOUNCEMENTS_MISSING');
    f.s.worker.postMessage({ kind: 'FIXTURE_WORKER_CRASH' });
    await f.s.exit;
    const s = f.s.snapshot();
    assert.equal(s.pipeline_completed, 0);
    assert.equal(s.discarded + s.unknown, 512);
    assert.equal(s.accepted_accounted, true);
    assert.equal(s.last_error, 'WORKER_ERROR');
    assert.ok(f.diagnostics.some((x) => x.tail_unknown));
    assert.ok(!JSON.stringify(f.diagnostics).includes('private-token-or-address'));
  } finally {
    await f.s.terminate();
  }
});
test('unexpected clean worker exit is a classified incomplete interval', async () => {
  const f = await socket();
  try {
    f.s.worker.postMessage({ kind: 'FIXTURE_WORKER_EXIT' });
    await f.s.exit;
    assert.equal(f.s.snapshot().last_error, 'WORKER_EXIT');
    assert.ok(f.diagnostics.some((x) => x.reason === 'WORKER_EXIT' && x.tail_unknown));
  } finally {
    await f.s.terminate();
  }
});
test('A uses classified worker recovery, confirmed hard-close and fences old generation callbacks', async () => {
  let at = 0;
  const sockets = [],
    rows = [],
    received = [];
  const peer = new RtdsPeer({
    source: 'A',
    continuous: true,
    mono: () => at,
    sink: (r) => rows.push(r),
    onActivity: (e) => received.push(e),
    socketFactory: (target) => {
      const s = new BufferedRtdsSocket(target, { workerURL: url });
      sockets.push(s);
      return s;
    }
  });
  peer.start();
  try {
    await until(() => peer.phase === 'OPEN', 'PEER_NOT_OPEN');
    const old = sockets[0],
      late = [...old.listeners.get('message')][0];
    old.send(command({ fixture: 'HANG_CLOSE' }));
    old.send(command({ fixture: 'BURST', count: 65 }));
    await until(() => received.length === 65, 'PEER_BURST_LOST');
    peer.reconnect('OFFLINE_HUNG_CLOSE');
    at = 2000;
    peer.tick();
    await old.exit;
    await pause(5);
    peer.tick();
    await until(() => sockets[1]?.readyState === 1, 'PEER_NOT_RECOVERED');
    assert.equal(old.exited, true);
    const packet = command({ topic: 'activity', type: 'trades', payload: { fixture: true } });
    let lateStatus;
    late({
      data: packet,
      complete(status) {
        lateStatus = status;
      }
    });
    assert.equal(lateStatus, 'DISCARDED');
    assert.equal(received.length, 65);
    sockets[1].worker.postMessage({ kind: 'FIXTURE_WORKER_CRASH' });
    await sockets[1].exit;
    await pause(5);
    assert.ok(rows.some((x) => x.kind === 'SOCKET_ERROR' && x.reason === 'WORKER_ERROR'));
    at += 1000;
    peer.tick();
    await until(() => sockets[2]?.readyState === 1, 'WORKER_FAILURE_NOT_RECOVERED');
    sockets[2].send(command({ fixture: 'BURST', count: 1, base: 5000 }));
    await until(() => received.length === 66, 'POST_RECOVERY_FRAME_LOST');
    assert.equal(peer.phase, 'OPEN');
  } finally {
    peer.stop();
    await Promise.all(sockets.map((s) => s.terminate()));
  }
});
for (const delay of [2100, 4000])
  test(`real worker retains burst during ${delay}ms receipt delay, including duplicate completion`, async () => {
    const f = await socket();
    let count = 0;
    f.s.addEventListener('message', (e) => {
      count++;
      const done = () => {
        e.complete('PIPELINE_COMPLETED', 'COMMON_PIPELINE_RETURNED');
        e.complete('PIPELINE_COMPLETED', 'COMMON_PIPELINE_RETURNED');
      };
      if (count === 1) setTimeout(done, delay);
      else done();
    });
    try {
      f.s.send(command({ fixture: 'BURST', count: 512 }));
      await until(() => f.s.snapshot().pipeline_completed === 512, 'DELAYED_RECEIPT_LOSS', 8000);
      await until(() => f.s.snapshot().worker?.receipted === 512, 'RECEIPT_STATS', 2000);
      const s = f.s.snapshot();
      assert.equal(count, 512);
      assert.equal(s.discarded + s.unknown + s.rejected, 0);
      assert.equal(s.incomplete_intervals, 0);
      assert.equal(s.accepted_accounted, true);
      assert.equal(f.s.readyState, 1);
      assert.equal(f.dispositions.length, 512);
      assert.ok(f.diagnostics.some((r) => r.kind === 'A_RECEIPT_STALLED'));
      assert.ok(f.diagnostics.some((r) => r.kind === 'A_RECEIPT_RECOVERED'));
    } finally {
      f.s.close();
      await f.s.terminate();
    }
  });
