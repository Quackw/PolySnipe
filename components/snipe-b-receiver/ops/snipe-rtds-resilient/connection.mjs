// Inert dependency-free RTDS lifecycle. No sender, REST, credentials or DB access.
import { randomUUID, createHash } from 'node:crypto';
export const RTDS_URL = 'wss://ws-live-data.polymarket.com/';
export const SUBSCRIPTIONS = Object.freeze(
  ['trades', 'orders_matched'].map((type) => Object.freeze({ topic: 'activity', type }))
);
const sha = (x) => createHash('sha256').update(x).digest('hex');
export const GAP_BOUNDS_MS = [
  1,
  2,
  5,
  10,
  20,
  50,
  100,
  250,
  500,
  1000,
  2000,
  5000,
  8000,
  15000,
  30000,
  60000,
  Infinity
];
export class RtdsPeer {
  constructor({
    source,
    socketFactory,
    mono,
    wall = Date.now,
    sink = () => {},
    onOpen = () => {},
    onDisconnect = () => {},
    onActivity = () => {},
    observeFrame = null,
    staleAfterMs = 8000,
    random = Math.random,
    maxConnections = 120,
    closeGraceMs = 2000,
    continuous = false,
    abortGraceMs = 2000
  }) {
    if (
      !['A', 'B'].includes(source) ||
      typeof socketFactory !== 'function' ||
      typeof mono !== 'function' ||
      !Number.isFinite(staleAfterMs) ||
      staleAfterMs < 1000 ||
      staleAfterMs > 60000
    )
      throw Error('INVALID_PEER_CONFIGURATION');
    if (observeFrame !== null && typeof observeFrame !== 'function') throw Error('INVALID_PEER_OBSERVER');
    if (
      typeof continuous !== 'boolean' ||
      !Number.isFinite(abortGraceMs) ||
      abortGraceMs < 1 ||
      abortGraceMs > 2000
    )
      throw Error('INVALID_RECOVERY_CONFIGURATION');
    Object.assign(this, {
      source,
      socketFactory,
      mono,
      wall,
      sink,
      onOpen,
      onDisconnect,
      onActivity,
      observeFrame,
      staleAfterMs,
      random,
      maxConnections,
      closeGraceMs,
      continuous,
      abortGraceMs
    });
    this.observationErrors = 0;
    this.phase = 'PREPARED';
    this.generation = 0;
    this.sessions = [];
    this.gapHistogram = Array(GAP_BOUNDS_MS.length).fill(0);
    this.gapMaxMs = 0;
    this.gapCount = 0;
    this.totalRaw = 0;
    this.totalActivity = 0;
    this.failures = 0;
    this.attempts = [];
    this.events = [];
  }
  emit(kind, data = {}) {
    const row = {
      kind,
      source: this.source,
      mono_ms: this.mono(),
      wall_ms: this.wall(),
      generation: this.generation,
      ...data
    };
    this.sink(row);
    return row;
  }
  start() {
    if (this.phase !== 'PREPARED') throw Error('NO_RESUME');
    this.phase = 'BACKOFF';
    this.nextConnect = this.mono();
    this.tick();
  }
  connect() {
    const m = this.mono();
    this.attempts = this.attempts.filter((x) => m - x < 600000);
    if ((!this.continuous && this.generation >= this.maxConnections) || this.attempts.length >= 20) {
      if (!this.continuous) {
        this.stop('CONNECTION_BUDGET');
        return;
      }
      this.phase = 'COOLDOWN';
      this.nextConnect = this.attempts[0] + 600000;
      this.emit('CONNECTION_COOLDOWN', { backoff_ms: Math.max(0, this.nextConnect - m) });
      return;
    }
    this.attempts.push(m);
    const s = {
      id: randomUUID(),
      generation: ++this.generation,
      start: m,
      open: null,
      subscribe: null,
      first: null,
      lastRaw: null,
      lastData: null,
      lastControl: null,
      lastPong: null,
      nextPing: m + 5000,
      raw: 0,
      activity: 0,
      topics: { trades: 0, orders_matched: 0 },
      closeRequest: null,
      close: null,
      errors: 0,
      listeners: [],
      closed: false
    };
    s.frameSeq = 0;
    this.session = s;
    this.sessions.push(s);
    if (this.sessions.length > 64) this.sessions.shift();
    this.phase = 'CONNECTING';
    this.emit('CONNECT_ATTEMPT', { connection_id: s.id });
    let socket;
    try {
      socket = this.socketFactory(RTDS_URL);
    } catch {
      this.schedule('CONSTRUCT_FAILED');
      return;
    }
    s.socket = socket;
    const current = () => this.session === s && !s.closeRequest && !s.closed && this.phase !== 'STOPPED';
    const add = (kind, fn) => {
      socket.addEventListener(kind, fn);
      s.listeners.push([kind, fn]);
    };
    add('open', () => {
      if (!current()) return;
      s.open = this.mono();
      this.phase = 'OPEN';
      this.emit('SOCKET_OPEN', { connection_id: s.id, connect_ms: s.open - s.start });
      try {
        socket.send(JSON.stringify({ action: 'subscribe', subscriptions: SUBSCRIPTIONS }));
        s.subscribe = this.mono();
        this.emit('SUBSCRIBE_SENT', { ack: 'NOT_MEASURED', topics: SUBSCRIPTIONS });
        this.onOpen({ source: this.source, generation: s.generation, opened_wall_ms: this.wall() });
      } catch {
        this.reconnect('SUBSCRIBE_OR_ADAPTER_FAILED');
      }
    });
    add('message', (event) => {
      const frameSeq = ++s.frameSeq;
      // Optional metadata-only observer. It cannot authorize a frame, throw into
      // the receiver, change liveness, or retain an unbounded input payload.
      const observed = (decision, extra = {}) => {
        if (!this.observeFrame) return;
        const raw = event.data,
          text = typeof raw === 'string',
          bounded = text && raw.length <= 262144 && Buffer.byteLength(raw) <= 262144;
        try {
          this.observeFrame({
            source: this.source,
            generation: s.generation,
            connection_id: s.id,
            frame_seq: frameSeq,
            frame_id: s.id + ':' + frameSeq,
            received_wall_ms: this.wall(),
            received_mono_ms: this.mono(),
            decision,
            raw_bytes: text ? Buffer.byteLength(raw) : null,
            raw_sha256: bounded ? sha(raw) : null,
            ...extra
          });
        } catch {
          this.observationErrors++;
        }
      };
      if (!current()) {
        observed('OBSOLETE_CONNECTION_GENERATION');
        return;
      }
      const m = this.mono();
      s.lastRaw = m;
      s.raw++;
      this.totalRaw++;
      const raw = event.data;
      if (typeof raw !== 'string' || Buffer.byteLength(raw) > 262144) {
        const reason = typeof raw !== 'string' ? 'NON_TEXT' : 'FRAME_BUDGET';
        observed(reason);
        this.emit('FRAME_REJECTED', { reason });
        return;
      }
      if (raw.toLowerCase() === 'pong' || raw.toLowerCase() === 'ping') {
        s.lastControl = m;
        if (raw.toLowerCase() === 'pong') s.lastPong = m;
        observed('APPLICATION_CONTROL');
        this.emit('APPLICATION_CONTROL', {
          type: raw.toUpperCase(),
          rtt_ms: null,
          correlation: 'NOT_MEASURED'
        });
        return;
      }
      let e;
      try {
        e = JSON.parse(raw);
      } catch {
        observed(raw ? 'INVALID_JSON' : 'EMPTY_FRAME');
        if (raw) this.emit('FRAME_REJECTED', { reason: 'INVALID_JSON' });
        return;
      }
      if (
        e?.topic !== 'activity' ||
        !['trades', 'orders_matched'].includes(e.type) ||
        !e.payload ||
        typeof e.payload !== 'object' ||
        Array.isArray(e.payload)
      ) {
        observed(
          e?.topic !== 'activity'
            ? 'UNSUBSCRIBED_TOPIC'
            : !['trades', 'orders_matched'].includes(e.type)
              ? 'UNSUBSCRIBED_TYPE'
              : 'INVALID_PAYLOAD_ENVELOPE'
        );
        return;
      }
      if (s.lastData !== null) {
        const gap = m - s.lastData;
        if (gap >= 0 && Number.isFinite(gap)) {
          this.gapHistogram[GAP_BOUNDS_MS.findIndex((x) => gap <= x)]++;
          this.gapMaxMs = Math.max(this.gapMaxMs, gap);
          this.gapCount++;
        }
      }
      const first = s.first === null;
      s.first ??= m;
      s.lastData = m;
      s.activity++;
      s.topics[e.type]++;
      this.totalActivity++;
      this.failures = 0;
      if (first)
        this.emit('FIRST_VALID_FRAME', {
          since_connect_ms: m - s.start,
          since_subscribe_ms: s.subscribe === null ? null : m - s.subscribe
        });
      observed('ACTIVITY');
      try {
        this.onActivity(
          e,
          {
            source: this.source,
            generation: s.generation,
            connection_id: s.id,
            frame_seq: frameSeq,
            frame_id: s.id + ':' + frameSeq,
            received_mono_ms: m,
            received_wall_ms: this.wall(),
            raw_bytes: Buffer.byteLength(raw),
            raw_sha256: sha(raw)
          },
          raw
        );
      } catch {
        this.reconnect('ACTIVITY_ADAPTER_FAILED');
      }
    });
    add('error', () => {
      if (!current()) return;
      s.errors++;
      this.emit('SOCKET_ERROR', { error_class: 'NOT_EXPOSED' });
      this.reconnect('SOCKET_ERROR');
    });
    add('close', (event) => {
      if (s.closed) return;
      s.closed = true;
      s.close = this.mono();
      this.emit('SOCKET_CLOSE', {
        connection_generation: s.generation,
        connection_id: s.id,
        code: Number.isInteger(event.code) ? event.code : null,
        reason_sha256: typeof event.reason === 'string' && event.reason ? sha(event.reason) : null,
        initiator: s.closeRequest ? 'LOCAL' : 'REMOTE_OR_TRANSPORT_UNKNOWN',
        local_trigger: s.closeRequest?.reason ?? null,
        connection_age_ms: s.close - s.start
      });
      this.detach(s);
      if (this.session === s && this.phase !== 'STOPPED') {
        if (!s.closeRequest) {
          this.onDisconnect({ source: this.source, generation: s.generation });
          this.schedule('REMOTE_OR_TRANSPORT_CLOSE');
        } else if (!s.abortRequested) this.phase = 'BACKOFF';
      }
    });
  }
  detach(s) {
    for (const [kind, fn] of s.listeners.splice(0)) s.socket?.removeEventListener(kind, fn);
  }
  schedule(reason) {
    const delay = Math.min(5000, 500 * 2 ** Math.min(this.failures++, 4)) * (0.75 + 0.25 * this.random());
    this.nextConnect = this.mono() + delay;
    this.phase = 'BACKOFF';
    this.emit('RECONNECT_SCHEDULED', { reason, backoff_ms: delay });
  }
  reconnect(reason) {
    const s = this.session;
    if (!s || s.closeRequest || this.phase === 'STOPPED') return;
    // Set the fence and recovery deadline before close(), including synchronous closes.
    s.closeRequest = { at: this.mono(), reason };
    this.onDisconnect({ source: this.source, generation: s.generation });
    this.schedule(reason);
    this.phase = 'WAIT_CLOSE';
    this.emit('SOCKET_CLOSE_REQUEST', {
      reason,
      connection_age_ms: this.mono() - s.start,
      last_raw_age_ms: s.lastRaw === null ? null : this.mono() - s.lastRaw,
      last_data_age_ms: s.lastData === null ? null : this.mono() - s.lastData
    });
    try {
      s.socket?.close();
    } catch {
      this.stop('CLOSE_CALL_FAILED');
    }
  }
  tick() {
    if (['PREPARED', 'STOPPED'].includes(this.phase)) return;
    const m = this.mono(),
      s = this.session;
    if (this.phase === 'WAIT_ABORT') {
      if (m - s.abortRequested >= this.abortGraceMs) this.stop('ABORT_NOT_CONFIRMED');
      return;
    }
    if (this.phase === 'WAIT_CLOSE') {
      if (m - s.closeRequest.at >= this.closeGraceMs) {
        this.emit('CLOSE_NOT_CONFIRMED', {
          abort:
            typeof s.socket?.terminate === 'function'
              ? 'WORKER_TERMINATION'
              : 'NOT_AVAILABLE_NATIVE_WEBSOCKET'
        });
        if (typeof s.socket?.terminate !== 'function') {
          this.stop('CLOSE_NOT_CONFIRMED');
          return;
        }
        s.abortRequested = m;
        this.phase = 'WAIT_ABORT';
        this.emit('ABORT_REQUESTED', { connection_id: s.id });
        Promise.resolve()
          .then(() => s.socket.terminate())
          .then((confirmed) => {
            if (this.session !== s || this.phase !== 'WAIT_ABORT') return;
            if (confirmed !== true) {
              this.stop('ABORT_NOT_CONFIRMED');
              return;
            }
            s.closed = true;
            this.detach(s);
            this.emit('ABORT_CONFIRMED', { connection_id: s.id });
            this.phase = 'BACKOFF';
          })
          .catch(() => {
            if (this.session === s && this.phase === 'WAIT_ABORT') this.stop('ABORT_FAILED');
          });
      }
      return;
    }
    if (this.phase === 'BACKOFF' || this.phase === 'COOLDOWN') {
      if (m >= this.nextConnect) this.connect();
      return;
    }
    if (this.phase === 'CONNECTING') {
      if (m - s.start >= 15000) this.reconnect('CONNECT_TIMEOUT');
      return;
    }
    if (this.phase === 'OPEN') {
      if (m - (s.lastData ?? s.open) >= this.staleAfterMs) {
        this.emit('DATA_PLANE_STALE', {
          silence_ms: m - (s.lastData ?? s.open),
          transport: this.health().transport
        });
        this.reconnect('DATA_PLANE_STALE');
        return;
      }
      if (m >= s.nextPing) {
        s.nextPing = m + 5000;
        try {
          s.socket.send('ping');
          this.emit('PING_SENT', {
            meaning: 'ACCEPTED_BY_API_NOT_DELIVERY_PROOF',
            buffered_bytes: Number.isFinite(s.socket.bufferedAmount) ? s.socket.bufferedAmount : null
          });
        } catch {
          this.reconnect('PING_SEND_FAILED');
        }
      }
    }
  }
  health() {
    const s = this.session,
      m = this.mono(),
      age = (x) => (x === null || x === undefined ? null : m - x);
    const rawAge = age(s?.lastRaw),
      dataAge = age(s?.lastData),
      pongAge = age(s?.lastPong),
      transportFresh =
        (rawAge !== null && rawAge < this.staleAfterMs) || (pongAge !== null && pongAge < 10000);
    return {
      source: this.source,
      phase: this.phase,
      stop_reason: this.reason ?? null,
      recovery_mode: this.continuous ? 'CONTINUOUS_BOUNDED' : 'FINITE',
      next_connect_in_ms: ['BACKOFF', 'COOLDOWN'].includes(this.phase)
        ? Math.max(0, this.nextConnect - m)
        : null,
      generation: this.generation,
      connection_id: s?.id ?? null,
      connection_age_ms: s ? m - s.start : null,
      last_raw_age_ms: rawAge,
      last_valid_activity_age_ms: dataAge,
      last_control_age_ms: age(s?.lastControl),
      transport: transportFresh ? 'TRANSPORT_ALIVE' : 'NOT_MEASURED',
      data_plane:
        dataAge !== null && dataAge < this.staleAfterMs ? 'APPLICATION_DATA_ALIVE' : 'DATA_PLANE_STALE',
      subscription_health: 'SUBSCRIPTION_HEALTH_UNKNOWN',
      subscription_topics: s?.topics ?? {},
      protocol_pong: 'NOT_MEASURED',
      rtt_ms: null,
      buffered_bytes: Number.isFinite(s?.socket?.bufferedAmount) ? s.socket.bufferedAmount : null,
      raw_frames: this.totalRaw,
      activity_frames: this.totalActivity,
      gap_histogram: this.gapHistogram,
      gap_count: this.gapCount,
      gap_max_ms: this.gapMaxMs,
      stale_after_ms: this.staleAfterMs
    };
  }
  stop(reason = 'DURATION_COMPLETED') {
    if (this.phase === 'STOPPED') return;
    const s = this.session;
    this.phase = 'STOPPED';
    this.reason = reason;
    if (s && !s.closeRequest && !s.closed) {
      s.closeRequest = { at: this.mono(), reason };
      this.onDisconnect({ source: this.source, generation: s.generation });
      try {
        s.socket?.close();
      } catch {}
    }
    if (s) this.detach(s);
    this.emit('STOP', { reason });
    if (typeof s?.socket?.terminate === 'function')
      Promise.resolve()
        .then(() => s.socket.terminate())
        .catch(() => {});
  }
}
