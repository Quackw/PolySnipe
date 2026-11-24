// Construction is inert. RtdsPeer starts this wrapper only through its factory.
// A worker exit is the hard-close witness; a close() request alone is not one.
import { Worker } from 'node:worker_threads';
export class ManagedRtdsSocket {
  constructor(
    url,
    {
      WorkerClass = Worker,
      workerURL = new URL('./socket-worker.mjs', import.meta.url),
      onDiagnostic = () => {}
    } = {}
  ) {
    if (url !== 'wss://ws-live-data.polymarket.com/') throw Error('RTDS_TARGET_REQUIRED');
    this.listeners = new Map();
    this.readyState = 0;
    this.bufferedAmount = 0;
    this.exited = false;
    this.closed = null;
    this.termination = null;
    this.worker = new WorkerClass(workerURL, {
      workerData: { url },
      execArgv: [],
      env: {},
      resourceLimits: { maxOldGenerationSizeMb: 64 }
    });
    this.worker.on('message', (value) => {
      if (this.exited) return;
      if (value?.kind === 'open') {
        this.readyState = 1;
        this.emit('open');
      } else if (value?.kind === 'message') {
        try {
          this.emit('message', { data: value.data });
        } finally {
          if (!this.exited) this.worker.postMessage({ kind: 'consumed' });
        }
      } else if (value?.kind === 'socket_close') {
        this.closed = { code: value.code, reason: value.reason };
      } else if (value?.kind === 'error') this.emit('error');
      else if (value?.kind === 'diagnostic')
        try {
          onDiagnostic(value.value);
        } catch {
          /* Evidence is never admission authority. */
        }
    });
    this.worker.on('error', () => this.emit('error'));
    this.exit = new Promise((resolve) =>
      this.worker.once('exit', () => {
        this.exited = true;
        this.readyState = 3;
        this.emit('close', this.closed ?? { code: 1006, reason: '' });
        resolve(true);
      })
    );
  }
  addEventListener(kind, fn) {
    if (!this.listeners.has(kind)) this.listeners.set(kind, new Set());
    this.listeners.get(kind).add(fn);
  }
  removeEventListener(kind, fn) {
    this.listeners.get(kind)?.delete(fn);
  }
  emit(kind, value = {}) {
    for (const fn of [...(this.listeners.get(kind) ?? [])]) fn(value);
  }
  send(data) {
    if (this.readyState !== 1 || typeof data !== 'string' || Buffer.byteLength(data) > 16384)
      throw Error('RTDS_SEND_NOT_OPEN');
    this.worker.postMessage({ kind: 'send', data });
  }
  close() {
    if (this.exited || this.readyState === 2) return;
    this.readyState = 2;
    this.worker.postMessage({ kind: 'close' });
  }
  terminate() {
    if (!this.termination)
      this.termination = (async () => {
        if (!this.exited) await this.worker.terminate();
        await this.exit;
        return this.exited;
      })();
    return this.termination;
  }
}
