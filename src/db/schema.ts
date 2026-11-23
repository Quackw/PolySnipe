import type Database from "better-sqlite3";

function ensureOptionalColumn(db: Database.Database, tableName: string, columnName: string, columnSql: string): void {
  const rows = db.prepare(`PRAGMA table_info(${tableName})`).all() as Array<{ name: string }>;
  if (rows.some((row) => row.name === columnName)) {
    return;
  }

  db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${columnSql}`);
}

export function ensureSchema(db: Database.Database): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS alert_outbox (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      subscription_id INTEGER NOT NULL,
      proxy_address TEXT NOT NULL,
      chat_id INTEGER NOT NULL,
      text TEXT NOT NULL,
      context_json TEXT NOT NULL,
      status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK(status IN ('PENDING','SENDING','DELIVERED','UNKNOWN','REJECTED','CANCELLED')),
      next_attempt_ms INTEGER NOT NULL DEFAULT 0,
      attempts INTEGER NOT NULL DEFAULT 0,
      last_error TEXT,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );
    CREATE INDEX IF NOT EXISTS idx_alert_outbox_pending ON alert_outbox(status,next_attempt_ms);
    CREATE INDEX IF NOT EXISTS idx_alert_outbox_chat ON alert_outbox(chat_id,status,id);
    CREATE TABLE IF NOT EXISTS subscribers (
      id                INTEGER PRIMARY KEY AUTOINCREMENT,
      telegram_user_id  INTEGER NOT NULL,
      telegram_chat_id  INTEGER NOT NULL,
      chat_type         TEXT NOT NULL,
      created_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      updated_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      UNIQUE (telegram_user_id, telegram_chat_id)
    );

    CREATE TABLE IF NOT EXISTS chat_sessions (
      telegram_chat_id  INTEGER PRIMARY KEY,
      mode              TEXT NOT NULL CHECK (mode IN ('idle', 'awaiting_subscribe')),
      awaiting_user_id  INTEGER,
      expires_at_ms     INTEGER,
      updated_at        TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );

    CREATE TABLE IF NOT EXISTS tracked_accounts (
      proxy_address          TEXT PRIMARY KEY,
      name                   TEXT,
      pseudonym              TEXT,
      profile_slug           TEXT,
      last_seen_timestamp    INTEGER NOT NULL DEFAULT 0,
      last_seen_keys_json    TEXT NOT NULL DEFAULT '[]',
      last_polled_at_ms      INTEGER,
      next_poll_at_ms        INTEGER NOT NULL DEFAULT 0,
      consecutive_errors     INTEGER NOT NULL DEFAULT 0,
      inflight               INTEGER NOT NULL DEFAULT 0 CHECK (inflight IN (0, 1)),
      inflight_started_at_ms INTEGER,
      created_at             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      updated_at             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );

    CREATE TABLE IF NOT EXISTS subscriptions (
      id                  INTEGER PRIMARY KEY AUTOINCREMENT,
      subscriber_id       INTEGER NOT NULL,
      telegram_chat_id    INTEGER NOT NULL,
      proxy_address       TEXT NOT NULL,
      created_by_user_id  INTEGER NOT NULL,
      notify_from_ts      INTEGER NOT NULL,
      created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
      FOREIGN KEY (subscriber_id) REFERENCES subscribers(id) ON DELETE CASCADE,
      FOREIGN KEY (proxy_address) REFERENCES tracked_accounts(proxy_address) ON DELETE CASCADE,
      UNIQUE (telegram_chat_id, proxy_address)
    );

    CREATE TABLE IF NOT EXISTS poll_runs (
      id                     INTEGER PRIMARY KEY AUTOINCREMENT,
      kind                   TEXT NOT NULL CHECK (kind IN ('account', 'heartbeat')),
      proxy_address          TEXT,
      status                 TEXT NOT NULL CHECK (status IN ('ok', 'bootstrap', 'error')),
      duration_ms            INTEGER NOT NULL DEFAULT 0,
      api_calls              INTEGER NOT NULL DEFAULT 0,
      retries                INTEGER NOT NULL DEFAULT 0,
      fetched_rows           INTEGER NOT NULL DEFAULT 0,
      new_rows               INTEGER NOT NULL DEFAULT 0,
      alerts_sent            INTEGER NOT NULL DEFAULT 0,
      queue_depth            INTEGER NOT NULL DEFAULT 0,
      tracked_accounts       INTEGER NOT NULL DEFAULT 0,
      inflight_accounts      INTEGER NOT NULL DEFAULT 0,
      effective_interval_ms  INTEGER NOT NULL DEFAULT 0,
      throttle_wait_ms       INTEGER NOT NULL DEFAULT 0,
      note                   TEXT,
      created_at             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );

    CREATE TABLE IF NOT EXISTS alert_delay_samples (
      id                  INTEGER PRIMARY KEY AUTOINCREMENT,
      telegram_chat_id    INTEGER NOT NULL,
      proxy_address       TEXT NOT NULL,
      activity_timestamp  INTEGER NOT NULL,
      poll_started_at_ms  INTEGER,
      fetch_completed_at_ms INTEGER,
      send_started_at_ms  INTEGER NOT NULL,
      send_completed_at_ms INTEGER,
      delay_ms            INTEGER NOT NULL,
      delay_reason_code   TEXT,
      created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    );

    CREATE INDEX IF NOT EXISTS idx_subscriptions_chat
      ON subscriptions(telegram_chat_id);

    CREATE INDEX IF NOT EXISTS idx_subscriptions_proxy
      ON subscriptions(proxy_address);

    CREATE INDEX IF NOT EXISTS idx_tracked_accounts_due
      ON tracked_accounts(next_poll_at_ms, inflight);

    CREATE INDEX IF NOT EXISTS idx_tracked_accounts_inflight
      ON tracked_accounts(inflight, inflight_started_at_ms);

    CREATE INDEX IF NOT EXISTS idx_poll_runs_created
      ON poll_runs(created_at DESC);

    CREATE INDEX IF NOT EXISTS idx_alert_delay_samples_created
      ON alert_delay_samples(created_at DESC);

    CREATE INDEX IF NOT EXISTS idx_alert_delay_samples_send_started
      ON alert_delay_samples(send_started_at_ms DESC);
  `);

  ensureOptionalColumn(db, "alert_delay_samples", "poll_started_at_ms", "INTEGER");
  ensureOptionalColumn(db, "alert_delay_samples", "fetch_completed_at_ms", "INTEGER");
  ensureOptionalColumn(db, "alert_delay_samples", "send_completed_at_ms", "INTEGER");
  ensureOptionalColumn(db, "alert_delay_samples", "delay_reason_code", "TEXT");
}
