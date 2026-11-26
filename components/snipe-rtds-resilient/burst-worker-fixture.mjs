// Execute either exact worker with offline native input. No external IO exists.
import net from 'node:net';
import tls from 'node:tls';
import dns from 'node:dns';
import dgram from 'node:dgram';
import { parentPort, workerData } from 'node:worker_threads';
import { syncBuiltinESMExports } from 'node:module';
const forbidden = () => {
  throw Error('OFFLINE_NETWORK_FORBIDDEN');
};
net.Socket.prototype.connect = forbidden;
tls.connect = dns.lookup = dgram.createSocket = forbidden;
globalThis.fetch = forbidden;
syncBuiltinESMExports();
export function fixtureTrade(index = 1, pad = 0) {
  return {
    topic: 'activity',
    type: index % 2 ? 'trades' : 'orders_matched',
    payload: {
      proxyWallet: '0x' + '1'.repeat(40),
      transactionHash: '0x' + index.toString(16).padStart(64, '0'),
      asset: 'fixture-token',
      side: index % 2 ? 'BUY' : 'SELL',
      size: 1,
      price: 0.5,
      timestamp: Math.floor(Date.now() / 1000),
      title: 'x'.repeat(pad)
    }
  };
}
class OfflineSocket {
  constructor() {
    this.listeners = new Map();
    this.hang = false;
    this.throwOnClose = false;
    queueMicrotask(() => this.emit('open'));
  }
  addEventListener(kind, fn) {
    if (!this.listeners.has(kind)) this.listeners.set(kind, new Set());
    this.listeners.get(kind).add(fn);
  }
  emit(kind, row = {}) {
    for (const fn of this.listeners.get(kind) ?? []) fn(row);
  }
  close() {
    if (this.throwOnClose) throw Object.assign(new TypeError('private close detail'), { code: 'ECONNRESET' });
    if (!this.hang) queueMicrotask(() => this.emit('close', { code: 1000, reason: '' }));
  }
  send(text) {
    if (text === 'ping') {
      this.emit('message', { data: 'pong' });
      return;
    }
    let x;
    try {
      x = JSON.parse(text);
    } catch {
      return;
    }
    if (x.fixture === 'BURST')
      for (let i = 0; i < x.count; i++)
        this.emit('message', { data: JSON.stringify(fixtureTrade((x.base ?? 1) + i, x.pad ?? 0)) });
    if (x.fixture === 'LARGE') this.emit('message', { data: 'x'.repeat(262145) });
    if (x.fixture === 'NON_TEXT') this.emit('message', { data: new Uint8Array(4) });
    if (x.fixture === 'HANG_CLOSE') this.hang = true;
    if (x.fixture === 'THROW_ON_CLOSE') this.throwOnClose = true;
    if (x.fixture === 'NATIVE_CLOSE') this.close();
    if (x.fixture === 'NATIVE_ERROR' || x.fixture === 'NATIVE_ERROR_WITHOUT_CLOSE') {
      if (x.fixture === 'NATIVE_ERROR_WITHOUT_CLOSE') this.hang = true;
      this.emit('error', {
        error: Object.assign(new TypeError('private-token-or-address'), { code: 'ECONNRESET' })
      });
      if (x.fixture === 'NATIVE_ERROR') this.close();
    }
    if (x.fixture === 'SEND_ERROR') throw Error('private-token-or-address');
  }
}
globalThis.WebSocket = OfflineSocket;
parentPort.on('message', (x) => {
  if (x?.kind === 'FIXTURE_WORKER_CRASH') throw Error('private-token-or-address');
  if (x?.kind === 'FIXTURE_WORKER_EXIT') process.exit(0);
});
await import(workerData.protocol === 'A_BUFFER_V1' ? './buffered-worker.mjs' : './socket-worker.mjs');
