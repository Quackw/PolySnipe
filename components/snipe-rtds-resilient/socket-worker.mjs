// One native socket per disposable worker. Inert when imported by the main
// thread; no credentials, databases, REST client or sender enter this worker.
import { isMainThread, parentPort, workerData } from 'node:worker_threads';
import { attachNativeDiagnostics, safeSocketError } from '../snipe-rtds-passive-ab/diagnostics.mjs';
if (!isMainThread) {
  if (workerData?.url !== 'wss://ws-live-data.polymarket.com/') throw Error('RTDS_TARGET_REQUIRED');
  const post = (value) => parentPort.postMessage(value);
  let socket,
    credits = 64,
    closing = false,
    disposed = false;
  const releaseDiagnostics = attachNativeDiagnostics((value) => post({ kind: 'diagnostic', value }));
  const dispose = () => {
    if (disposed) return;
    disposed = true;
    releaseDiagnostics();
  };
  try {
    socket = new WebSocket(workerData.url);
  } catch (error) {
    dispose();
    throw error;
  }
  const finish = (code, reason) => {
    if (disposed) return;
    closing = true;
    dispose();
    post({ kind: 'socket_close', code, reason });
    parentPort.close();
  };
  const closeSocket = () => {
    if (closing) return;
    closing = true;
    try {
      socket.close();
    } catch (error) {
      post({ kind: 'diagnostic', value: { kind: 'NATIVE_CLOSE_ERROR', ...safeSocketError(error) } });
      // The parent treats the worker exit as the hard-close witness.
      finish(1006, '');
    }
  };
  socket.addEventListener('open', () => post({ kind: 'open' }));
  socket.addEventListener('message', (event) => {
    if (closing) return;
    if (typeof event.data !== 'string' || Buffer.byteLength(event.data) > 262144 || --credits < 0) {
      post({ kind: 'error' });
      closeSocket();
      return;
    }
    post({ kind: 'message', data: event.data });
  });
  socket.addEventListener('error', () => {
    if (closing) return;
    post({ kind: 'error' });
    closeSocket();
  });
  socket.addEventListener('close', (event) =>
    finish(
      Number.isInteger(event.code) ? event.code : 1006,
      typeof event.reason === 'string' ? event.reason : ''
    )
  );
  parentPort.on('message', (value) => {
    if (value?.kind === 'consumed') {
      credits = Math.min(64, credits + 1);
      return;
    }
    if (value?.kind === 'close') {
      closeSocket();
      return;
    }
    if (
      value?.kind === 'send' &&
      !closing &&
      typeof value.data === 'string' &&
      Buffer.byteLength(value.data) <= 16384
    ) {
      try {
        socket.send(value.data);
      } catch {
        post({ kind: 'error' });
      }
    }
  });
}
