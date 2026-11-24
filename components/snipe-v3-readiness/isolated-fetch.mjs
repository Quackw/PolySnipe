// One worker per bounded REDEEM request. Importing this module creates no worker
// and opens no socket. A timed-out worker must exit before another can start.
import { Worker } from 'node:worker_threads';
export function createIsolatedFetch({
  workerFactory = () =>
    new Worker(new URL('./redeem-worker.mjs', import.meta.url), {
      env: {},
      resourceLimits: { maxOldGenerationSizeMb: 64, maxYoungGenerationSizeMb: 16 }
    })
} = {}) {
  let active = null,
    closed = false;
  const fetchPage = (url, { signal } = {}) => {
    if (closed || active) throw Error('REDEEM_WORKER_UNAVAILABLE');
    if (!signal || signal.aborted) throw Error('REDEEM_REQUEST_FENCED');
    const target = new URL(String(url));
    if (
      target.protocol !== 'https:' ||
      target.hostname !== 'data-api.polymarket.com' ||
      target.port ||
      target.pathname !== '/v2/activity' ||
      target.username ||
      target.password ||
      target.hash
    )
      throw Error('REDEEM_ENDPOINT_INVALID');
    const worker = workerFactory(),
      item = { worker, finished: false, terminating: false, outcome: null };
    active = item;
    return new Promise((resolve, reject) => {
      const finish = (error, value) => {
        if (item.finished) return;
        item.finished = true;
        signal.removeEventListener('abort', abort);
        if (active === item) active = null;
        worker.removeAllListeners();
        if (error) reject(error);
        else resolve({ ok: value.ok, status: value.status, json: async () => value.data });
      };
      const stop = (error) => {
        if (item.terminating || item.finished) return;
        item.terminating = true;
        item.outcome = { error };
        // Keep active set until termination is confirmed. A hung termination is
        // quarantined by the outer request fence; it never permits a second worker.
        Promise.resolve()
          .then(() => worker.terminate())
          .then(
            () => finish(item.outcome.error, item.outcome.value),
            () => {
              item.terminationFailed = true;
            }
          );
      };
      item.stop = stop;
      const abort = () => {
        if (item.terminating) {
          item.outcome = { error: Error('REDEEM_REQUEST_FENCED') };
          return;
        }
        stop(Error('REDEEM_REQUEST_FENCED'));
      };
      signal.addEventListener('abort', abort, { once: true });
      worker.once('message', (msg) => {
        if (item.terminating) return;
        if (
          !msg ||
          msg.kind !== 'REDEEM_RESPONSE' ||
          typeof msg.ok !== 'boolean' ||
          !Number.isInteger(msg.status)
        ) {
          stop(Error('REDEEM_WORKER_PROTOCOL'));
          return;
        }
        const value = { ok: msg.ok, status: msg.status, data: msg.data };
        // No second GET may start until this worker has exited, even on success.
        item.terminating = true;
        item.outcome = { error: null, value };
        Promise.resolve()
          .then(() => worker.terminate())
          .then(
            () => finish(item.outcome.error, item.outcome.value),
            () => {
              item.terminationFailed = true;
            }
          );
      });
      worker.once('error', () => stop(Error('REDEEM_WORKER_FAILED')));
      worker.once('exit', (code) => {
        if (item.finished) return;
        if (item.terminating) finish(item.outcome.error, item.outcome.value);
        else finish(Error(code === 0 ? 'REDEEM_WORKER_EARLY_EXIT' : 'REDEEM_WORKER_FAILED'));
      });
      try {
        worker.postMessage({ kind: 'REDEEM_FETCH', url: target.toString() });
      } catch {
        stop(Error('REDEEM_WORKER_START_FAILED'));
      }
      if (signal.aborted) abort();
    });
  };
  fetchPage.close = () => {
    closed = true;
    if (active && !active.terminating) active.stop(Error('REDEEM_WORKER_CLOSED'));
  };
  return fetchPage;
}
