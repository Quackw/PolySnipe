import fs from 'node:fs';
import path from 'node:path';
import { normalizeAddress, userActionIdentity, userActionKey } from './identity.mjs';
import { isRetryableSqliteLock } from './sqlite-lock.mjs';

export const MAX_TRACKED_ACK_IDS = 4096;

export function createSendOnce(
  db,
  { root, ownerIds, recipientMap, insert, now = Date.now, dynamicProtected = false }
) {
  const file = path.join(root, 'send-once-canary.json');
  const rollbackFile = path.join(root, 'SEND_ONCE_ROLLBACK');
  const config = JSON.parse(fs.readFileSync(file, 'utf8'));
  const expanded = config.version === 2;
  if (dynamicProtected && !expanded) throw Error('send_once_dynamic_requires_expanded_scope');
  if (
    !expanded &&
    (config.version !== 1 ||
      !Number.isSafeInteger(config.start_ms) ||
      config.start_ms <= 0 ||
      !Number.isSafeInteger(config.recipient) ||
      !ownerIds.includes(config.recipient) ||
      !Array.isArray(config.wallets) ||
      config.wallets.length !== 6 ||
      new Set(config.wallets).size !== config.wallets.length ||
      config.wallets.some((w) => normalizeAddress(w) !== w))
  )
    throw Error('send_once_canary_config_invalid');
  if (
    expanded &&
    (!Number.isSafeInteger(config.start_ms) ||
      config.start_ms <= 0 ||
      !Array.isArray(config.subscriptions) ||
      config.subscriptions.length === 0 ||
      config.subscriptions.some(
        (s) => normalizeAddress(s.wallet) !== s.wallet || !Number.isSafeInteger(s.recipient)
      ))
  )
    throw Error('send_once_scope_config_invalid');
  const pairs = new Set((config.subscriptions || []).map((s) => s.wallet + '|' + s.recipient));
  if (expanded && !dynamicProtected) {
    if (pairs.size !== config.subscriptions.length) throw Error('send_once_scope_missing');
    const missing = config.subscriptions.filter(
      (s) => !recipientMap.get(s.wallet)?.some((r) => r.chat_id === s.recipient)
    );
    if (missing.length) {
      // Deleted subscriptions remain protected by the frozen pairs/ledger if
      // re-added. They cannot receive alerts through FAST's active recipient map.
      // A stored subscription missing from that map is still a startup failure.
      const stored = db.prepare('SELECT 1 FROM subscriptions WHERE proxy_address=? AND telegram_chat_id=?');
      if (missing.length === pairs.size || missing.some((s) => stored.get(s.wallet, s.recipient)))
        throw Error('send_once_scope_missing');
    }
  }
  const wallets = new Set(expanded ? config.subscriptions.map((s) => s.wallet) : config.wallets);
  const actual = [...recipientMap]
    .filter(([wallet, rows]) => wallets.has(wallet) && rows.some((r) => r.chat_id === config.recipient))
    .map(([wallet]) => wallet);
  if (!expanded && actual.length !== wallets.size) throw Error('send_once_canary_scope_missing');
  db.exec(`CREATE TABLE IF NOT EXISTS send_once_actions (
    action_key TEXT NOT NULL, chat_id INTEGER NOT NULL, identity_json TEXT NOT NULL,
    first_event_key TEXT NOT NULL, first_trace_id TEXT, created_at_ms INTEGER NOT NULL,
    PRIMARY KEY(action_key,chat_id)
  );
  CREATE INDEX IF NOT EXISTS idx_send_once_first_event
    ON send_once_actions(first_event_key,chat_id);`);
  const get = db.prepare(
    'SELECT identity_json,first_event_key,first_trace_id FROM send_once_actions WHERE action_key=? AND chat_id=?'
  );
  const add = db.prepare(
    'INSERT INTO send_once_actions(action_key,chat_id,identity_json,first_event_key,first_trace_id,created_at_ms) VALUES(?,?,?,?,?,?)'
  );
  const counts = db.prepare(`SELECT COUNT(*) AS n,
    SUM(CASE WHEN d.state='DELIVERED' THEN 1 ELSE 0 END) AS acked,
    SUM(CASE WHEN d.state='DELIVERY_UNKNOWN' THEN 1 ELSE 0 END) AS unknown
    FROM send_once_actions a JOIN deliveries d
      ON d.event_key=a.first_event_key AND d.chat_id=a.chat_id
    WHERE a.created_at_ms>=?`);
  const latencies = db.prepare(`SELECT d.delivered_at_ms-d.source_timestamp*1000 AS ms
    FROM send_once_actions a JOIN deliveries d
      ON d.event_key=a.first_event_key AND d.chat_id=a.chat_id
    WHERE a.created_at_ms>=? AND d.state='DELIVERED'
    ORDER BY d.delivered_at_ms DESC LIMIT 1000`);
  const stats = {
    send_first_action: 0,
    suppress_same_action: 0,
    false_user_action_merges: 0,
    first_direct_event_dropped: 0,
    legacy_after_rollback: 0,
    admission_errors: 0,
    duplicate_telegram_alerts: 0
  };
  const ackIds = new Map();
  let rollbackMarked = fs.existsSync(rollbackFile);
  function refreshRollback() {
    // Nested admission uses the flag refreshed by the outer writer.
    if (!db.inTransaction) rollbackMarked = fs.existsSync(rollbackFile);
    return rollbackMarked;
  }
  const actionForEvent = db.prepare(
    'SELECT action_key FROM send_once_actions WHERE first_event_key=? AND chat_id=?'
  );
  const admitTx = db.transaction((trade, recipient, fillKey, traceId, html, nextAt, seenAt) => {
    const actionKey = userActionKey(trade),
      identity = userActionIdentity(trade);
    if (rollbackMarked) return { kind: 'LEGACY_AFTER_ROLLBACK', actionKey };
    const prior = get.get(actionKey, recipient);
    if (prior)
      return prior.identity_json === identity
        ? {
            kind: 'SUPPRESS_SAME_ACTION',
            actionKey,
            firstEventKey: prior.first_event_key,
            firstTraceId: prior.first_trace_id
          }
        : { kind: 'FALSE_USER_ACTION_MERGE', actionKey };
    const inserted = insert.run(fillKey, recipient, trade.wallet, trade.timestamp, seenAt, html, nextAt);
    if (inserted.changes !== 1) return { kind: 'FIRST_DIRECT_EVENT_DROPPED', actionKey };
    add.run(actionKey, recipient, identity, fillKey, traceId, seenAt);
    return { kind: 'SEND_FIRST_ACTION', actionKey };
  });
  function rollback(reason) {
    try {
      fs.writeFileSync(rollbackFile, `${new Date(now()).toISOString()} ${reason}\n`, {
        flag: 'wx',
        mode: 0o600
      });
    } catch (e) {
      if (e.code !== 'EEXIST') throw e;
    }
    rollbackMarked = true;
  }
  function applies(wallet, recipient) {
    return dynamicProtected
      ? typeof wallet === 'string' &&
          normalizeAddress(wallet) === wallet &&
          Number.isSafeInteger(recipient) &&
          recipient > 0
      : expanded
        ? pairs.has(wallet + '|' + recipient)
        : recipient === config.recipient && wallets.has(wallet);
  }
  function observeAck(row, result) {
    if (result.kind !== 'OK' || !applies(row.wallet, row.chat_id) || refreshRollback()) return;
    const action = actionForEvent.get(row.event_key, row.chat_id);
    if (!action) return;
    const key = action.action_key + '|' + row.chat_id,
      id = result.value?.result?.message_id;
    if (!Number.isSafeInteger(id) || id <= 0) return;
    if (ackIds.has(key) && ackIds.get(key) !== id) {
      stats.duplicate_telegram_alerts++;
      rollback('DUPLICATE_TELEGRAM_ALERT');
    } else {
      // Refresh the recency of repeated acknowledgements before eviction.
      ackIds.delete(key);
      ackIds.set(key, id);
      if (ackIds.size > MAX_TRACKED_ACK_IDS) ackIds.delete(ackIds.keys().next().value);
    }
  }
  function admit(trade, recipient, fillKey, traceId, html, nextAt, seenAt) {
    let result;
    refreshRollback();
    try {
      result = admitTx.immediate(trade, recipient, fillKey, traceId, html, nextAt, seenAt);
    } catch (e) {
      stats.admission_errors++;
      if (!isRetryableSqliteLock(e)) rollback('ACTION_ADMISSION_ERROR');
      throw e;
    }
    if (result.kind === 'SEND_FIRST_ACTION') stats.send_first_action++;
    if (result.kind === 'SUPPRESS_SAME_ACTION') stats.suppress_same_action++;
    if (result.kind === 'FALSE_USER_ACTION_MERGE') {
      stats.false_user_action_merges++;
      rollback('FALSE_USER_ACTION_MERGE');
    }
    if (result.kind === 'FIRST_DIRECT_EVENT_DROPPED') {
      stats.first_direct_event_dropped++;
      rollback('FIRST_DIRECT_EVENT_DROPPED');
    }
    if (result.kind === 'LEGACY_AFTER_ROLLBACK') stats.legacy_after_rollback++;
    return result;
  }
  function snapshot() {
    const rows = latencies
      .all(config.start_ms)
      .map((r) => Math.max(0, r.ms))
      .sort((a, b) => a - b);
    const n = counts.get(config.start_ms);
    return {
      start_ms: config.start_ms,
      scope_wallets: wallets.size,
      scope_subscriptions: expanded ? pairs.size : wallets.size,
      scope_mode: dynamicProtected
        ? 'DYNAMIC_PROTECTED'
        : expanded
          ? 'FROZEN_CURRENT_SUBSCRIPTIONS'
          : 'OWNER_SIX',
      rollback: refreshRollback(),
      user_actions_seen: n.n,
      send_first_action: stats.send_first_action,
      suppress_same_action: stats.suppress_same_action,
      false_user_action_merges: stats.false_user_action_merges,
      first_direct_event_dropped: stats.first_direct_event_dropped,
      acked: n.acked || 0,
      delivery_unknown: n.unknown || 0,
      latency_ms: {
        n: rows.length,
        median: rows[Math.floor((rows.length - 1) * 0.5)] ?? null,
        p90: rows[Math.floor((rows.length - 1) * 0.9)] ?? null,
        max: rows.at(-1) ?? null
      },
      duplicate_telegram_alerts: stats.duplicate_telegram_alerts,
      admission_errors: stats.admission_errors,
      legacy_after_rollback: stats.legacy_after_rollback,
      tracked_ack_ids: ackIds.size
    };
  }
  function isOriginallyProtected(wallet, recipient) {
    return expanded
      ? pairs.has(wallet + '|' + recipient)
      : recipient === config.recipient && wallets.has(wallet);
  }
  return {
    applies,
    isOriginallyProtected,
    admit,
    rollback,
    refreshRollback,
    observeAck,
    snapshot,
    config: { start_ms: config.start_ms, scope_wallets: wallets.size }
  };
}
