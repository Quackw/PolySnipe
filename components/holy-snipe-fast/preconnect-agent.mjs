import https from 'node:https';
import tls from 'node:tls';
import { performance } from 'node:perf_hooks';
import { recordTelegramConnection } from './telegram-diagnostics.mjs';

// Only TLS handshakes are retried. The HTTP request is attached after verified
// secureConnect; an error after that point is handled by the existing sender.
const transient = new Set([
  'ETIMEDOUT',
  'ECONNRESET',
  'ECONNREFUSED',
  'EAI_AGAIN',
  'ENETUNREACH',
  'EHOSTUNREACH'
]);
export function createPreconnectAgent({
  connect = tls.connect,
  attemptTimeoutMs = 2000,
  attempts = 2,
  onAttempt = () => {}
} = {}) {
  class Agent extends https.Agent {
    createConnection(options, callback) {
      let finished = false,
        n = 0;
      const started = performance.now();
      const finish = (err, socket) => {
        if (finished) {
          socket?.destroy();
          return;
        }
        finished = true;
        callback(err, socket);
      };
      const start = () => {
        n++;
        let socket,
          done = false,
          timer;
        recordTelegramConnection({ preconnect_attempts: n });
        const fail = (err) => {
          if (done || finished) return;
          done = true;
          clearTimeout(timer);
          socket?.destroy();
          recordTelegramConnection({ preconnect_failures: n });
          try {
            onAttempt({ attempt: n, result: 'failed', code: err.code || 'OTHER' });
          } catch {}
          if (n < attempts && transient.has(err.code)) start();
          else finish(err);
        };
        try {
          socket = connect({ ...options, rejectUnauthorized: true });
          socket.once('lookup', () => recordTelegramConnection({ dns_ms: performance.now() - started }));
          socket.once('connect', () => recordTelegramConnection({ tcp_ms: performance.now() - started }));
          socket.once('error', fail);
          socket.once('secureConnect', () => {
            if (done || finished) return;
            done = true;
            clearTimeout(timer);
            socket.removeListener('error', fail);
            recordTelegramConnection({
              tls_ms: performance.now() - started,
              address_family:
                socket.remoteFamily === 'IPv6' ? 6 : socket.remoteFamily === 'IPv4' ? 4 : undefined
            });
            try {
              onAttempt({ attempt: n, result: 'ready' });
            } catch {}
            finish(null, socket);
          });
          timer = setTimeout(
            () => fail(Object.assign(new Error('TLS_CONNECT_TIMEOUT'), { code: 'ETIMEDOUT' })),
            attemptTimeoutMs
          );
          timer.unref();
        } catch (err) {
          fail(err);
        }
      };
      start();
    }
  }
  return new Agent({
    keepAlive: true,
    maxSockets: 1,
    maxFreeSockets: 1,
    scheduling: 'lifo',
    autoSelectFamily: true,
    autoSelectFamilyAttemptTimeout: 1000
  });
}
