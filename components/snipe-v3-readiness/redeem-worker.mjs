// Dedicated GET-only worker. No Telegram code, credential env, retries or logs.
import { parentPort } from 'node:worker_threads';
if (!parentPort) throw Error('REDEEM_WORKER_ONLY');
let busy = false;
parentPort.on('message', async (message) => {
  if (busy || message?.kind !== 'REDEEM_FETCH' || typeof message.url !== 'string') return;
  busy = true;
  try {
    const url = new URL(message.url);
    if (
      url.protocol !== 'https:' ||
      url.hostname !== 'data-api.polymarket.com' ||
      url.port ||
      url.pathname !== '/v2/activity' ||
      url.username ||
      url.password ||
      url.hash
    )
      throw Error('REDEEM_ENDPOINT_INVALID');
    const allowed = new Set(['user', 'type', 'start', 'end', 'sort_by', 'sort_direction', 'limit', 'cursor']);
    if (
      [...url.searchParams.keys()].some((k) => !allowed.has(k)) ||
      url.searchParams.get('type') !== 'REDEEM'
    )
      throw Error('REDEEM_QUERY_INVALID');
    const response = await fetch(url, {
      method: 'GET',
      redirect: 'error',
      signal: AbortSignal.timeout(3000)
    });
    let data = null;
    if (response.ok) {
      if (!response.body) throw Error('REDEEM_BODY_MISSING');
      const reader = response.body.getReader(),
        chunks = [];
      let bytes = 0;
      try {
        for (;;) {
          const r = await reader.read();
          if (r.done) break;
          bytes += r.value.byteLength;
          if (bytes > 2 * 1024 * 1024) throw Error('REDEEM_BODY_BUDGET');
          chunks.push(Buffer.from(r.value));
        }
      } finally {
        reader.releaseLock();
      }
      data = JSON.parse(Buffer.concat(chunks).toString('utf8'));
    } else {
      try {
        await response.body?.cancel();
      } catch {}
    }
    parentPort.postMessage({ kind: 'REDEEM_RESPONSE', ok: response.ok, status: response.status, data });
  } catch {
    parentPort.postMessage({ kind: 'REDEEM_WORKER_ERROR' });
  }
});
