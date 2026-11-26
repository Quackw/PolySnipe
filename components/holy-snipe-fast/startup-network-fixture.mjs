import http from 'node:http';
import https from 'node:https';
import net from 'node:net';
import tls from 'node:tls';
import threads from 'node:worker_threads';
import { syncBuiltinESMExports } from 'node:module';
import { Readable, Writable } from 'node:stream';
import { fileURLToPath } from 'node:url';

function unexpectedNetwork() {
  console.error('UNEXPECTED_NETWORK_IN_FAST_STARTUP_FIXTURE');
  throw Error('UNEXPECTED_NETWORK_IN_FAST_STARTUP_FIXTURE');
}

net.Socket.prototype.connect = unexpectedNetwork;
tls.connect = unexpectedNetwork;
http.request = unexpectedNetwork;
globalThis.fetch = unexpectedNetwork;

const telegram = {
  getMe: { id: 1000, is_bot: true, first_name: 'Fixture', username: 'fixture_bot' },
  getWebhookInfo: { url: '', pending_update_count: 0 },
  deleteWebhook: true,
  getUpdates: []
};

let pollingSeen = false;
https.request = (resource, options, onResponse) => {
  const route = resource instanceof URL ? resource.pathname : resource.path;
  const method = /^\/botfixture\/([a-zA-Z]+)$/.exec(route)?.[1];
  if (resource.hostname !== 'api.telegram.org' || !Object.hasOwn(telegram, method)) unexpectedNetwork();

  const request = new Writable({
    autoDestroy: false,
    write(_data, _encoding, done) {
      done();
    }
  });
  request.abort = () => request.destroy();
  if (typeof onResponse === 'function') request.once('response', onResponse);
  request.once('finish', () => {
    const respond = () => {
      if (request.destroyed) return;
      const data = Buffer.from(JSON.stringify({ ok: true, result: telegram[method] }));
      const response = Readable.from([data]);
      response.statusCode = 200;
      response.headers = { 'content-type': 'application/json', 'content-length': String(data.length) };
      request.emit('response', response);
    };
    if (method === 'getUpdates') {
      if (!pollingSeen) console.log('FIXTURE_TELEGRAM_GET_UPDATES');
      pollingSeen = true;
      setTimeout(respond, 50);
    } else {
      respond();
    }
  });
  return request;
};

globalThis.WebSocket = class FixtureSocket extends EventTarget {
  constructor(url) {
    super();
    if (url !== 'wss://ws-live-data.polymarket.com/') unexpectedNetwork();
    queueMicrotask(() => this.dispatchEvent(new Event('open')));
  }

  send(data) {
    if (data === 'PING' || data === 'ping') {
      this.dispatchEvent(new MessageEvent('message', { data: 'PONG' }));
    } else if (JSON.parse(data).action === 'subscribe') {
      this.dispatchEvent(
        new MessageEvent('message', {
          data: JSON.stringify({ topic: 'activity', type: 'trades', payload: {} })
        })
      );
    }
  }

  close() {
    this.dispatchEvent(Object.assign(new Event('close'), { code: 1000, reason: '' }));
  }
};

if (threads.isMainThread) {
  const RealWorker = threads.Worker;
  const preload = fileURLToPath(import.meta.url);
  // FAST starts the real RTDS worker with an empty execArgv.
  threads.Worker = class OfflineWorker extends RealWorker {
    constructor(filename, options) {
      super(filename, { ...options, execArgv: ['--import', preload] });
    }
  };
}
syncBuiltinESMExports();
