// Execute the actual worker with a deterministic native-socket substitute.
// close() deliberately hangs; no real socket, HTTP or DNS is constructed.
import net from 'node:net';
import tls from 'node:tls';
import { channel } from 'node:diagnostics_channel';
import { parentPort, workerData } from 'node:worker_threads';
const forbidden = () => {
  throw Error('OFFLINE_NETWORK_FORBIDDEN');
};
net.Socket.prototype.connect = forbidden;
tls.connect = forbidden;
globalThis.fetch = forbidden;
const diagnosticChannels = ['ping', 'pong', 'socket_error'].map((name) =>
  channel('undici:websocket:' + name)
);
const reportSubscribers = (stage) =>
  parentPort.postMessage({
    kind: 'fixture_subscribers',
    stage,
    active: diagnosticChannels.filter((c) => c.hasSubscribers).length
  });
class OfflineSocket {
  constructor() {
    if (workerData.fixture === 'CONSTRUCT_ERROR') {
      reportSubscribers('constructor');
      throw Error('FIXTURE_CONSTRUCT_ERROR');
    }
    this.listeners = new Map();
    queueMicrotask(() => this.emit('open'));
  }
  addEventListener(kind, fn) {
    if (!this.listeners.has(kind)) this.listeners.set(kind, new Set());
    this.listeners.get(kind).add(fn);
  }
  emit(kind, value = {}) {
    for (const fn of this.listeners.get(kind) ?? []) fn(value);
  }
  close() {
    if (workerData.fixture === 'CLOSE_ERROR')
      throw Object.assign(new TypeError('private close detail'), { code: 'ECONNRESET' });
  }
  send(value) {
    if (value === 'fixture-frame')
      this.emit('message', {
        data: JSON.stringify({ topic: 'activity', type: 'trades', payload: { fixture: true } })
      });
    if (value === 'fixture-overflow')
      for (let i = 0; i < 1000; i++) this.emit('message', { data: 'fixture' });
    if (value === 'fixture-large') this.emit('message', { data: 'x'.repeat(262145) });
    if (value === 'fixture-native-error') this.emit('error');
  }
}
globalThis.WebSocket = OfflineSocket;
try {
  await import('./socket-worker.mjs');
} catch (error) {
  reportSubscribers('after_failure');
  throw error;
}
