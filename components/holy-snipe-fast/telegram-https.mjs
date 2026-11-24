// Dedicated Telegram pool. No HTTP replay, redirects, proxy or global networking changes.
import https from 'node:https';
import { performance } from 'node:perf_hooks';
import { recordTelegramConnection } from './telegram-diagnostics.mjs';
import { createPreconnectAgent } from './preconnect-agent.mjs';
export function createTelegramHTTPSFetch({
  requestImpl = https.request,
  agent = createPreconnectAgent()
} = {}) {
  return function telegramHTTPSFetch(url, { method, headers, body, signal } = {}) {
    const target = new URL(url);
    if (
      target.protocol !== 'https:' ||
      target.hostname !== 'api.telegram.org' ||
      target.port ||
      target.username ||
      target.password ||
      target.search ||
      !/^\/bot[^/]+\/[A-Za-z]+$/.test(target.pathname)
    )
      throw Error('TELEGRAM_TARGET_REJECTED');
    const start = performance.now();
    recordTelegramConnection({ family_attempt_ms: 1000 });
    return new Promise((resolve, reject) => {
      let settled = false,
        request;
      const fail = (error) => {
        if (settled) return;
        settled = true;
        reject(error);
      };
      try {
        request = requestImpl(target, { method, headers, agent, signal }, (response) => {
          const chunks = [];
          let bytes = 0;
          response.on('data', (chunk) => {
            bytes += chunk.length;
            if (bytes > 65536) {
              response.destroy(
                Object.assign(Error('TELEGRAM_RESPONSE_LIMIT'), { code: 'TELEGRAM_RESPONSE_LIMIT' })
              );
              return;
            }
            chunks.push(chunk);
          });
          response.on('error', fail);
          response.on('aborted', () =>
            fail(Object.assign(Error('TELEGRAM_RESPONSE_ABORTED'), { code: 'ECONNRESET' }))
          );
          response.on('end', () => {
            if (settled) return;
            settled = true;
            recordTelegramConnection({ response_end_ms: performance.now() - start });
            const text = Buffer.concat(chunks).toString('utf8');
            resolve({
              status: response.statusCode,
              json: async () => JSON.parse(text),
              body: { cancel: async () => {} }
            });
          });
        });
        request.on('error', fail);
        request.on('socket', (socket) => {
          recordTelegramConnection({ reused_socket: request.reusedSocket === true });
          if (request.reusedSocket) return;
          socket.once('lookup', () => recordTelegramConnection({ dns_ms: performance.now() - start }));
          socket.once('connect', () => recordTelegramConnection({ tcp_ms: performance.now() - start }));
          socket.once('secureConnect', () =>
            recordTelegramConnection({
              tls_ms: performance.now() - start,
              address_family:
                socket.remoteFamily === 'IPv6' ? 6 : socket.remoteFamily === 'IPv4' ? 4 : undefined
            })
          );
        });
        request.on('finish', () =>
          recordTelegramConnection({ request_finish_ms: performance.now() - start })
        );
        request.end(body);
      } catch (error) {
        request?.destroy();
        fail(error);
      }
    });
  };
}
export const telegramHTTPSFetch = createTelegramHTTPSFetch();
