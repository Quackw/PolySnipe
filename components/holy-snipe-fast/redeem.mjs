import { createHash, randomUUID } from 'node:crypto';
import { performance } from 'node:perf_hooks';
// Review-only candidate. No network or sender is opened by importing this file.
import { normalizeAddress, escapeHtml } from './identity.mjs';
import { createIsolatedFetch } from '../snipe-v3-readiness/isolated-fetch.mjs';
import { createRedeemPacer } from '../snipe-v3-readiness/redeem-pacer.mjs';

// Separate source/ledger: never promotes REST trades into the RTDS/recovery path.
export function canonicalRedeem(row, wallet, startMs, now = Date.now()) {
  if (row?.type !== 'REDEEM' || normalizeAddress(row.proxyWallet) !== wallet) return null;
  const timestamp = Number(row.timestamp),
    size = Number(row.size),
    amount = Number(row.usdcSize);
  const hash = String(row.transactionHash || '').toLowerCase();
  const condition = String(row.conditionId || '').toLowerCase();
  const asset = String(row.asset || '');
  const outcomeIndex = Number(row.outcomeIndex);
  if (!/^0x[0-9a-f]{64}$/.test(hash) || !/^0x[0-9a-f]{64}$/.test(condition)) return null;
  if (!Number.isSafeInteger(timestamp) || timestamp * 1000 < startMs || timestamp * 1000 > now + 5000)
    return null;
  if (!Number.isFinite(size) || size <= 0 || !Number.isFinite(amount) || amount < 0) return null;
  if (asset.length > 256 || (!asset && (!Number.isSafeInteger(outcomeIndex) || outcomeIndex < 0)))
    return null;
  const key = createHash('sha256')
    .update(
      JSON.stringify([
        'REDEEM',
        wallet,
        hash,
        condition,
        asset,
        Number.isSafeInteger(outcomeIndex) ? outcomeIndex : null
      ])
    )
    .digest('hex');
  return {
    key,
    wallet,
    timestamp,
    size,
    amount,
    title: String(row.title || '').slice(0, 300),
    outcome: String(row.outcome || '').slice(0, 100)
  };
}
// Normalize only the documented v2 names into the existing identity contract.
// Keep the v1 canonicalizer/key stable for the persisted delivery ledger.
export function canonicalRedeemV2(row, wallet, startMs, now = Date.now()) {
  if (!row || typeof row !== 'object' || Array.isArray(row)) return null;
  if (
    [row.timestamp, row.size, row.usdc_size].some(
      (v) => !(typeof v === 'number' || (typeof v === 'string' && v.trim() !== ''))
    )
  )
    return null;
  if (row.token_id != null && typeof row.token_id !== 'string') return null;
  if (!row.token_id && row.outcome_index == null) return null;
  if (
    row.outcome_index != null &&
    (!(
      typeof row.outcome_index === 'number' ||
      (typeof row.outcome_index === 'string' && row.outcome_index.trim() !== '')
    ) ||
      !Number.isSafeInteger(Number(row.outcome_index)) ||
      Number(row.outcome_index) < 0)
  )
    return null;
  return canonicalRedeem(
    {
      type: row.type,
      proxyWallet: row.proxy_wallet,
      transactionHash: row.transaction_hash,
      conditionId: row.condition_id,
      asset: row.token_id ?? '',
      outcomeIndex: row.outcome_index,
      timestamp: row.timestamp,
      size: row.size,
      usdcSize: row.usdc_size,
      title: row.title,
      outcome: row.outcome
    },
    wallet,
    startMs,
    now
  );
}
export function formatRedeem(row, label = {}) {
  const name = label.profileSlug
    ? '@' + label.profileSlug
    : label.name || label.pseudonym || 'Tracked wallet';
  return `${escapeHtml(String(name).slice(0, 120))}\n\n${escapeHtml(row.title || 'Market unavailable')} | <b>REDEEM</b>, outcome: <b>${escapeHtml(row.outcome || 'Unavailable')}</b>, shares: <b>${row.size.toLocaleString('en-US', { useGrouping: false, maximumFractionDigits: 2 })}</b>, payout: <b>$${row.amount.toFixed(2)}</b>`;
}
export function createRedeem(
  db,
  {
    config,
    getRecipients,
    getLabel,
    enabled,
    telegram,
    fetchImpl = null,
    now = Date.now,
    pause = (ms) => new Promise((r) => setTimeout(r, ms)),
    stageTimeoutMs = 3000,
    freshEligibility = null,
    mono = () => performance.now(),
    requestPacer = null,
    onFatal = () => {}
  }
) {
  if (
    config?.version !== 1 ||
    !Number.isSafeInteger(config.start_ms) ||
    !Array.isArray(config.subscriptions) ||
    !config.subscriptions.length
  )
    throw Error('redeem_config_invalid');
  if (!Number.isSafeInteger(stageTimeoutMs) || stageTimeoutMs < 10 || stageTimeoutMs > 3000)
    throw Error('redeem_timeout_invalid');
  const request = fetchImpl ?? createIsolatedFetch();
  if (typeof onFatal !== 'function') throw Error('redeem_pacer_invalid');
  const scope = new Map();
  for (const s of config.subscriptions) {
    const wallet = normalizeAddress(s.wallet);
    if (!wallet || !Number.isSafeInteger(s.recipient) || s.recipient <= 0)
      throw Error('redeem_scope_invalid');
    if (!scope.has(wallet)) scope.set(wallet, new Set());
    scope.get(wallet).add(s.recipient);
  }
  db.exec(`CREATE TABLE IF NOT EXISTS redeem_deliveries (
    event_key TEXT NOT NULL,chat_id INTEGER NOT NULL,wallet TEXT NOT NULL,
    source_timestamp INTEGER NOT NULL,message_html TEXT NOT NULL,state TEXT NOT NULL,
    created_at_ms INTEGER NOT NULL,attempts INTEGER NOT NULL DEFAULT 0,
    http_status INTEGER,error_code INTEGER,reason TEXT,ack_valid INTEGER,
    finished_at_ms INTEGER,PRIMARY KEY(event_key,chat_id));
    CREATE TABLE IF NOT EXISTS redeem_checkpoint(wallet TEXT PRIMARY KEY,through_ts INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS redeem_runtime_guard(id INTEGER PRIMARY KEY CHECK(id=1),reason TEXT NOT NULL,created_at_ms INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS redeem_request_clock(id INTEGER PRIMARY KEY CHECK(id=1),last_start_ms INTEGER NOT NULL);`);
  // Nullable/additive: older code can still read existing rows. The token fences
  // the claim through its final fresh check and completion.
  if (
    !db
      .prepare('PRAGMA table_info(redeem_deliveries)')
      .all()
      .some((r) => r.name === 'claim_token')
  )
    db.exec('ALTER TABLE redeem_deliveries ADD COLUMN claim_token TEXT');
  db.prepare(
    "UPDATE redeem_deliveries SET state='DELIVERY_UNKNOWN',reason='PROCESS_INTERRUPTED' WHERE state='SENDING'"
  ).run();
  const instanceStart = now();
  let lastRequestWall =
    db.prepare('SELECT last_start_ms FROM redeem_request_clock WHERE id=1').get()?.last_start_ms ?? null;
  const backwardClock =
    lastRequestWall !== null &&
    (!Number.isSafeInteger(lastRequestWall) || lastRequestWall < 0 || instanceStart < lastRequestWall);
  const initialWait = backwardClock
    ? 0
    : lastRequestWall === null
      ? 0
      : Math.max(0, 3100 + stageTimeoutMs - (instanceStart - lastRequestWall));
  const pacing = requestPacer ?? createRedeemPacer({ mono, initialWaitMs: initialWait });
  if (typeof pacing?.take !== 'function') throw Error('redeem_pacer_invalid');
  // A fresh explicit boundary is mandatory: restarting never backfills the
  // downtime or sends pending rows from an earlier process instance.
  const liveStart =
    Number.isSafeInteger(config.live_only_after_ms) && config.live_only_after_ms >= config.start_ms
      ? Math.max(config.live_only_after_ms, instanceStart)
      : null;
  const stats = {
    start_ms: config.start_ms,
    scope_pairs: config.subscriptions.length,
    http_requests: 0,
    poll_errors: 0,
    invalid_rows: 0,
    truncated_cycles: 0,
    last_poll_ms: null,
    live_boundary_ready: liveStart !== null,
    send_guard_ready: typeof freshEligibility === 'function',
    stage_timeouts: 0,
    last_failure_code: null,
    last_progress_ms: null,
    state_guard_stops: 0,
    state_age_max_ms: 0
  };
  const insert = db.prepare(
    "INSERT OR IGNORE INTO redeem_deliveries(event_key,chat_id,wallet,source_timestamp,message_html,state,created_at_ms) VALUES(?,?,?,?,?,'PENDING',?)"
  );
  const checkpoint = db.prepare('SELECT through_ts FROM redeem_checkpoint WHERE wallet=?');
  const advance = db.prepare(
    'INSERT INTO redeem_checkpoint VALUES(?,?) ON CONFLICT(wallet) DO UPDATE SET through_ts=excluded.through_ts'
  );
  const savedRateGuard = db.prepare('SELECT reason FROM redeem_runtime_guard WHERE id=1').get();
  let polling = false,
    stopped = false,
    pending = null,
    attempt = 0,
    guardHalted = false,
    rateHalted = !!savedRateGuard,
    rateGuardReason = savedRateGuard?.reason ?? null;
  function haltRate(reason = 'RATE_LIMIT_429') {
    rateHalted = true;
    rateGuardReason = reason;
    stats.last_failure_code = 'REDEEM_' + reason;
    try {
      db.prepare('INSERT OR IGNORE INTO redeem_runtime_guard(id,reason,created_at_ms) VALUES(1,?,?)').run(
        reason,
        now()
      );
    } catch {
      guardHalted = true;
      try {
        onFatal('REDEEM_RATE_GUARD_PERSISTENCE_FAILED');
      } catch {}
    }
  }
  if (backwardClock) haltRate('CLOCK_ROLLBACK');
  else if (rateHalted) stats.last_failure_code = 'REDEEM_RATE_GUARD_ACTIVE';
  const stateAge = (start) => {
    const age = mono() - start;
    if (!Number.isFinite(age) || age < 0 || age > 2000) throw Error('REDEEM_STATE_TOO_STALE');
    stats.state_age_max_ms = Math.max(stats.state_age_max_ms, age);
    return age;
  };
  // A timeout fences the whole fetch + body parse. Until the actual operation
  // settles, no new work may overlap it. A late result can never reach SQLite.
  async function bounded(stage, operation, controller = null, deadlineMs = stageTimeoutMs) {
    if (pending) throw Error('REDEEM_OPERATION_UNSETTLED');
    const item = { id: ++attempt, stage, timedOut: false };
    pending = item;
    let timer;
    const work = Promise.resolve()
      .then(operation)
      .then(
        (value) => {
          if (pending === item) pending = null;
          return { ok: true, value };
        },
        (error) => {
          if (pending === item) pending = null;
          return { ok: false, error };
        }
      );
    const timeout = new Promise((resolve) => {
      timer = setTimeout(() => resolve({ timeout: true }), deadlineMs);
    });
    const outcome = await Promise.race([work, timeout]);
    clearTimeout(timer);
    if (outcome.timeout) {
      item.timedOut = true;
      stats.stage_timeouts++;
      stats.last_failure_code = 'REDEEM_' + stage + '_TIMEOUT';
      controller?.abort();
      throw Error(stats.last_failure_code);
    }
    if (!outcome.ok) throw outcome.error;
    stats.last_progress_ms = now();
    return outcome.value;
  }
  function allowed(wallet, id, timestamp) {
    return (
      scope.get(wallet)?.has(id) &&
      getRecipients()
        .get(wallet)
        ?.some((r) => r.chat_id === id && timestamp >= r.notify_from_ts)
    );
  }
  async function poll() {
    if (
      polling ||
      stopped ||
      guardHalted ||
      rateHalted ||
      !enabled() ||
      liveStart === null ||
      typeof freshEligibility !== 'function' ||
      now() < liveStart ||
      pending
    )
      return;
    polling = true;
    try {
      for (const wallet of scope.keys()) {
        if (stopped) break;
        if (
          !getRecipients()
            .get(wallet)
            ?.some((r) => scope.get(wallet).has(r.chat_id))
        )
          continue;
        const cut = now(),
          prior = checkpoint.get(wallet)?.through_ts ?? 0;
        if (prior > Math.floor(cut / 1000)) {
          stats.poll_errors++;
          stats.last_failure_code = 'REDEEM_CLOCK_OR_CHECKPOINT_AHEAD';
          break;
        }
        const start = Math.max(Math.ceil(config.start_ms / 1000), Math.ceil(liveStart / 1000), prior - 300);
        if (start > Math.floor(cut / 1000)) continue;
        const rows = [],
          seenCursors = new Set();
        let complete = false,
          cursor;
        try {
          for (let page = 0; page < 5; page++) {
            if (stopped) break;
            const url = new URL('https://data-api.polymarket.com/v2/activity');
            for (const [k, v] of Object.entries({
              user: wallet,
              type: 'REDEEM',
              start,
              end: Math.floor(cut / 1000),
              sort_by: 'TIMESTAMP',
              sort_direction: 'ASC'
            }))
              url.searchParams.set(k, String(v));
            if (cursor === undefined) url.searchParams.set('limit', '100');
            else url.searchParams.set('cursor', cursor);
            await bounded('REQUEST_PACE', () => pacing.take(), null, stageTimeoutMs + 3600);
            if (stopped || rateHalted || guardHalted) break;
            const controller = new AbortController(),
              networkStarted = mono();
            const networkAge = () => {
              const age = mono() - networkStarted;
              if (!Number.isFinite(age) || age < 0 || age > stageTimeoutMs) {
                controller.abort();
                stats.stage_timeouts++;
                throw Error('REDEEM_FETCH_AND_BODY_TIMEOUT');
              }
            };
            const data = await bounded(
              'FETCH_AND_BODY',
              async () => {
                if (controller.signal.aborted || stopped || rateHalted || guardHalted)
                  throw Error('REDEEM_REQUEST_FENCED');
                const startWall = now();
                if (
                  !Number.isSafeInteger(startWall) ||
                  (lastRequestWall !== null && startWall < lastRequestWall)
                ) {
                  haltRate('CLOCK_ROLLBACK');
                  throw Error('REDEEM_CLOCK_ROLLBACK');
                }
                try {
                  db.prepare(
                    'INSERT INTO redeem_request_clock(id,last_start_ms) VALUES(1,?) ON CONFLICT(id) DO UPDATE SET last_start_ms=excluded.last_start_ms'
                  ).run(startWall);
                } catch {
                  guardHalted = true;
                  stats.last_failure_code = 'REDEEM_REQUEST_CLOCK_PERSISTENCE_FAILED';
                  try {
                    onFatal('REDEEM_RATE_GUARD_PERSISTENCE_FAILED');
                  } catch {}
                  throw Error('REDEEM_REQUEST_CLOCK_PERSISTENCE_FAILED');
                }
                lastRequestWall = startWall;
                networkAge();
                try {
                  pacing.markStart?.();
                } catch {
                  haltRate('CLOCK_ROLLBACK');
                  throw Error('REDEEM_CLOCK_ROLLBACK');
                }
                stats.http_requests++;
                const response = await request(url, { signal: controller.signal });
                if (response?.status === 429) {
                  haltRate();
                  throw Error('REDEEM_RATE_LIMIT_429');
                }
                networkAge();
                if (controller.signal.aborted) {
                  try {
                    void Promise.resolve(response?.body?.cancel?.()).catch(() => {});
                  } catch {}
                  throw Error('REDEEM_REQUEST_FENCED');
                }
                if (!response.ok) throw Error('ACTIVITY_HTTP');
                const value = await response.json();
                networkAge();
                if (controller.signal.aborted) throw Error('REDEEM_REQUEST_FENCED');
                return value;
              },
              controller
            );
            if (
              !data ||
              !Array.isArray(data.data) ||
              data.data.length > 100 ||
              !data.pagination ||
              !Object.hasOwn(data.pagination, 'next_cursor')
            )
              throw Error('ACTIVITY_SCHEMA');
            const next = data.pagination.next_cursor;
            if (
              next !== null &&
              (typeof next !== 'string' || next.trim().length === 0 || seenCursors.has(next))
            )
              throw Error('ACTIVITY_CURSOR');
            if (data.pagination.has_more !== (next !== null)) throw Error('ACTIVITY_PAGINATION');
            rows.push(...data.data);
            // A short or empty page may still have more results. Only null ends coverage.
            if (next === null) {
              complete = true;
              break;
            }
            seenCursors.add(next);
            cursor = next;
            await bounded('PAGINATION_PAUSE', () => pause(1000));
          }
          if (stopped || rateHalted || guardHalted) break;
          if (!complete) {
            stats.truncated_cycles++;
            continue;
          }
          // Validate the whole response before admitting anything or advancing coverage.
          const normalized = rows.map((r) => canonicalRedeemV2(r, wallet, config.start_ms, cut));
          const invalid = normalized.filter(
            (r, i) =>
              !r ||
              r.timestamp < start ||
              r.timestamp > Math.floor(cut / 1000) ||
              (i > 0 && r.timestamp < normalized[i - 1]?.timestamp)
          );
          if (invalid.length) {
            stats.invalid_rows += invalid.length;
            continue;
          }
          db.exec('BEGIN IMMEDIATE');
          try {
            for (const row of normalized)
              for (const id of scope.get(wallet))
                if (allowed(wallet, id, row.timestamp))
                  insert.run(row.key, id, wallet, row.timestamp, formatRedeem(row, getLabel(wallet)), now());
            advance.run(wallet, Math.floor(cut / 1000));
            db.exec('COMMIT');
          } catch (e) {
            db.exec('ROLLBACK');
            throw e;
          }
          stats.last_poll_ms = now();
          stats.last_failure_code = null;
        } catch (error) {
          stats.poll_errors++;
          if (!/^REDEEM_[A-Z_]+_TIMEOUT$/.test(error?.message ?? ''))
            stats.last_failure_code = /^[A-Z0-9_]+$/.test(error?.message ?? '')
              ? error.message
              : 'REDEEM_POLL_ERROR';
          if (pending || rateHalted || guardHalted) break;
        }
        if (!stopped)
          try {
            await bounded('WALLET_PAUSE', () => pause(1000));
          } catch {
            stats.poll_errors++;
            break;
          }
      }
    } catch {
      stats.poll_errors++;
      stats.last_failure_code = 'REDEEM_CYCLE_FAILURE';
    } finally {
      polling = false;
    }
  }
  async function sendDue() {
    if (
      stopped ||
      guardHalted ||
      !enabled() ||
      liveStart === null ||
      typeof freshEligibility !== 'function' ||
      now() < liveStart
    )
      return;
    const started = mono(),
      token = randomUUID();
    let row,
      begun = false;
    try {
      db.exec('BEGIN IMMEDIATE');
      begun = true;
      stateAge(started);
      row = db
        .prepare(
          "SELECT * FROM redeem_deliveries WHERE state='PENDING' AND source_timestamp>=? AND created_at_ms>=? ORDER BY created_at_ms LIMIT 1"
        )
        .get(Math.ceil(liveStart / 1000), instanceStart);
      if (!row) {
        db.exec('COMMIT');
        begun = false;
        return;
      }
      const eligible = freshEligibility(row.wallet, row.chat_id, row.source_timestamp);
      if (eligible !== true && eligible !== false) throw Error('REDEEM_ELIGIBILITY_UNKNOWN');
      if (eligible === false) {
        db.prepare(
          "UPDATE redeem_deliveries SET state='CANCELLED_SUBSCRIPTION' WHERE event_key=? AND chat_id=? AND state='PENDING'"
        ).run(row.event_key, row.chat_id);
        stateAge(started);
        db.exec('COMMIT');
        begun = false;
        stateAge(started);
        return;
      }
      if (!allowed(row.wallet, row.chat_id, row.source_timestamp)) throw Error('REDEEM_CACHED_SCOPE_DRIFT');
      const claimed = db
        .prepare(
          "UPDATE redeem_deliveries SET state='SENDING',attempts=1,claim_token=? WHERE event_key=? AND chat_id=? AND state='PENDING'"
        )
        .run(token, row.event_key, row.chat_id);
      if (claimed.changes !== 1) throw Error('REDEEM_CLAIM_CONFLICT');
      stateAge(started);
      db.exec('COMMIT');
      begun = false;
      stateAge(started);
      // Recheck the same claim and live subscription immediately before send;
      // the original 2000ms monotonic budget is never restarted.
      db.exec('BEGIN IMMEDIATE');
      begun = true;
      stateAge(started);
      if (
        !enabled() ||
        !allowed(row.wallet, row.chat_id, row.source_timestamp) ||
        freshEligibility(row.wallet, row.chat_id, row.source_timestamp) !== true
      )
        throw Error('REDEEM_FINAL_STATE_CHANGED');
      const current = db
        .prepare(
          'SELECT wallet,source_timestamp,state,claim_token FROM redeem_deliveries WHERE event_key=? AND chat_id=?'
        )
        .get(row.event_key, row.chat_id);
      if (
        !current ||
        current.wallet !== row.wallet ||
        current.source_timestamp !== row.source_timestamp ||
        current.state !== 'SENDING' ||
        current.claim_token !== token
      )
        throw Error('REDEEM_FINAL_STATE_CHANGED');
      stateAge(started);
      db.exec('COMMIT');
      begun = false;
      stateAge(started);
    } catch (error) {
      if (begun)
        try {
          db.exec('ROLLBACK');
        } catch {}
      // A committed claim remains reserved; never send or retry blindly.
      guardHalted = true;
      stats.state_guard_stops++;
      stats.last_failure_code = /^[A-Z0-9_]+$/.test(error?.message ?? '')
        ? error.message
        : 'REDEEM_STATE_GUARD_FAILURE';
      return;
    }
    let result;
    try {
      result = await telegram(
        'sendMessage',
        {
          chat_id: String(row.chat_id),
          text: row.message_html,
          parse_mode: 'HTML',
          disable_web_page_preview: true
        },
        8000
      );
    } catch {
      result = { kind: 'AMBIGUOUS', reason: 'SEND_EXCEPTION', status: null };
    }
    const valid =
      result.kind === 'OK' &&
      result.status === 200 &&
      result.value?.ok === true &&
      Number.isSafeInteger(result.value?.result?.message_id) &&
      result.value.result.message_id > 0 &&
      String(result.value.result.chat?.id) === String(row.chat_id);
    const state = valid
      ? 'DELIVERED'
      : result.kind === 'AMBIGUOUS' || result.kind === 'OK'
        ? 'DELIVERY_UNKNOWN'
        : 'FAILED_FINAL';
    try {
      const finished = db
        .prepare(
          "UPDATE redeem_deliveries SET state=?,http_status=?,error_code=?,reason=?,ack_valid=?,finished_at_ms=?,claim_token=NULL WHERE event_key=? AND chat_id=? AND state='SENDING' AND claim_token=?"
        )
        .run(
          state,
          result.status ?? null,
          result.error_code ?? null,
          valid ? null : (result.reason ?? 'ACK_RECIPIENT_NOT_CONFIRMED'),
          valid ? 1 : 0,
          now(),
          row.event_key,
          row.chat_id,
          token
        );
      if (finished.changes !== 1) throw Error('REDEEM_FINAL_CLAIM_LOST');
    } catch {
      guardHalted = true;
      stats.state_guard_stops++;
      stats.last_failure_code = 'REDEEM_FINAL_CLAIM_LOST';
    }
  }
  return {
    poll,
    sendDue,
    async stop() {
      stopped = true;
      try {
        request.close?.();
      } catch {}
      while (polling) await new Promise((r) => setTimeout(r, 50));
    },
    snapshot() {
      const times = db.prepare('SELECT through_ts FROM redeem_checkpoint').all();
      return {
        ...stats,
        polling,
        guard_halted: guardHalted,
        rate_halted: rateHalted,
        rate_guard_reason: rateGuardReason,
        request_start_interval_ms: 3100,
        operation_unsettled: !!pending,
        operation_stage: pending?.stage ?? null,
        checkpoint_wallets: times.length,
        max_checkpoint_age_s: times.length
          ? Math.max(...times.map((r) => now() / 1000 - r.through_ts))
          : null,
        states: Object.fromEntries(
          db
            .prepare('SELECT state,COUNT(*) n FROM redeem_deliveries GROUP BY state')
            .all()
            .map((r) => [r.state, r.n])
        )
      };
    }
  };
}
