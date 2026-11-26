// Execute the actual worker with a deterministic native-socket substitute.
// close() deliberately hangs; no real socket, HTTP or DNS is constructed.
import net from 'node:net';
import tls from 'node:tls';
const forbidden = () => {
  throw Error('OFFLINE_NETWORK_FORBIDDEN');
};
net.Socket.prototype.connect = forbidden;
tls.connect = forbidden;
globalThis.fetch = forbidden;
class OfflineSocket {
  constructor() {
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
  close() {}
  send(value) {
    if (value === 'fixture-frame')
      this.emit('message', {
        data: JSON.stringify({ topic: 'activity', type: 'trades', payload: { fixture: true } })
      });
    if (value === 'fixture-overflow')
      for (let i = 0; i < 1000; i++) this.emit('message', { data: 'fixture' });
    if (value === 'fixture-large') this.emit('message', { data: 'x'.repeat(262145) });
  }
}
globalThis.WebSocket = OfflineSocket;
await import('./socket-worker.mjs');
