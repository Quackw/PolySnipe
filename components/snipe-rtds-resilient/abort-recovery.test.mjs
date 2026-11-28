import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import test from 'node:test';
import { setImmediate as settle } from 'node:timers/promises';
import { BufferedRtdsSocket } from './buffered-socket.mjs';
import { RtdsPeer } from './connection.mjs';
import { frameMono } from './frame-buffer.mjs';

class DeferredExitWorker extends EventEmitter {
  termination = Promise.withResolvers();

  postMessage() {}

  terminate() {
    return this.termination.promise;
  }

  exit() {
    this.emit('exit', 0);
  }
}

async function abortingPeer({ continuous = true, maxConnections } = {}) {
  let monoMs = 0;
  const sockets = [];
  const rows = [];
  const frames = [];
  const peer = new RtdsPeer({
    source: 'A',
    continuous,
    maxConnections,
    mono: () => monoMs,
    random: () => 1,
    sink: (row) => rows.push(row),
    onActivity: (activity) => frames.push(activity),
    socketFactory(url) {
      assert.ok(
        sockets.every((socket) => socket.exited),
        'the previous worker must have exited'
      );
      const socket = new BufferedRtdsSocket(url, { WorkerClass: DeferredExitWorker });
      sockets.push(socket);
      return socket;
    }
  });
  function step(ms) {
    monoMs += ms;
    peer.tick();
  }

  peer.start();
  const old = sockets[0];
  old.worker.emit('message', { kind: 'open' });
  old.worker.emit('message', { kind: 'transport_fault', reason: 'NATIVE_SOCKET_ERROR' });
  step(2000);
  await settle();
  assert.equal(peer.phase, 'WAIT_ABORT');
  return { peer, old, sockets, rows, frames, step };
}

test('a delayed worker exit unblocks exactly one replacement that can process a frame', async () => {
  const { peer, old, sockets, rows, frames, step } = await abortingPeer();
  step(2000);
  old.worker.emit('message', { kind: 'socket_close', code: 1006 });
  step(30000);
  assert.equal(peer.phase, 'WAIT_ABORT');
  assert.equal(peer.health().abort_pending_reason, 'ABORT_NOT_CONFIRMED');
  assert.equal(peer.health().next_connect_in_ms, null);
  assert.equal(rows.filter((row) => row.kind === 'ABORT_NOT_CONFIRMED').length, 1);
  assert.equal(rows.filter((row) => row.kind === 'CONNECT_ATTEMPT').length, 1);
  assert.equal(sockets.length, 1);
  assert.equal(old.exited, false);

  old.worker.exit();
  await settle();
  assert.equal(peer.phase, 'BACKOFF');
  assert.equal(peer.session, null);
  assert.equal(peer.health().abort_pending_reason, null);
  assert.equal(sockets.length, 1);
  step(0);
  assert.equal(sockets.length, 2);
  const replacement = sockets[1];
  replacement.worker.emit('message', { kind: 'open' });
  const trade = { topic: 'activity', type: 'trades', payload: { side: 'BUY' } };
  const data = JSON.stringify(trade);
  replacement.worker.emit('message', {
    kind: 'frame_observed',
    seq: 1,
    bytes: Buffer.byteLength(data),
    received_mono_ms: frameMono(),
    received_wall_ms: Date.now(),
    accepted: true,
    frame_sha256: null
  });
  replacement.worker.emit('message', { kind: 'frame', seq: 1, data });
  assert.deepEqual(frames, [trade]);
  assert.equal(replacement.snapshot().pipeline_completed, 1);
  assert.ok(rows.some((row) => row.kind === 'ABORT_CONFIRMED' && row.late === true));
  old.worker.termination.resolve(1);
  peer.stop();
});

test('an explicit stop while waiting for a late exit is terminal', async () => {
  const { peer, old, sockets, rows, step } = await abortingPeer();
  step(2000);
  assert.equal(peer.health().abort_pending_reason, 'ABORT_NOT_CONFIRMED');
  peer.stop('OPERATOR_STOP');
  old.worker.exit();
  old.worker.termination.resolve(1);
  await settle();
  step(600000);
  assert.equal(peer.phase, 'STOPPED');
  assert.equal(peer.health().stop_reason, 'OPERATOR_STOP');
  assert.equal(sockets.length, 1);
  assert.equal(rows.filter((row) => row.kind === 'ABORT_CONFIRMED').length, 0);
});

test('duplicate close and termination confirmations cannot schedule another replacement', async () => {
  const { peer, old, sockets, rows, step } = await abortingPeer();
  const [lateClose] = old.listeners.get('close');
  step(2000);
  old.worker.exit();
  old.worker.termination.resolve(1);
  await settle();
  lateClose({ code: 1006 });
  step(0);
  assert.equal(sockets.length, 2);
  sockets[1].worker.emit('message', { kind: 'open' });
  lateClose({ code: 1006 });
  await settle();
  step(0);
  assert.equal(sockets.length, 2);
  assert.equal(rows.filter((row) => row.kind === 'ABORT_CONFIRMED').length, 1);
  assert.equal(rows.filter((row) => row.kind === 'RECONNECT_SCHEDULED').length, 1);
  peer.stop();
});

test('an exhausted finite peer cannot resume after a late worker exit', async () => {
  const { peer, old, sockets, step } = await abortingPeer({ continuous: false, maxConnections: 1 });
  step(2000);
  assert.equal(peer.phase, 'STOPPED');
  assert.equal(peer.health().stop_reason, 'ABORT_NOT_CONFIRMED');
  old.worker.exit();
  old.worker.termination.resolve(1);
  await settle();
  step(600000);
  assert.equal(peer.phase, 'STOPPED');
  assert.equal(sockets.length, 1);
});
