import type Database from "better-sqlite3";
import { ensureSchema } from "./schema.js";
import type {
  AdminPerformanceReport,
  AdminProfileStat,
  AdminSubscriberStats,
  AdminSubscriptionsReport,
  AdminUsersReport,
  AlertDelaySampleInsert,
  AccountSubscriptionRecord,
  ChatSessionRecord,
  ChatSubscriptionView,
  DelayBucketCount,
  DelayBucketLabel,
  DelayReasonCode,
  DelayReasonCount,
  FreshnessStats,
  PollRunRecord,
  TrackedAccountRecord
} from "../types.js";

export const MAX_ALERT_ATTEMPTS = 10;
const ATTEMPTS_EXHAUSTED = "Delivery attempt limit reached";

type AlertStatus = "DELIVERED" | "UNKNOWN" | "REJECTED" | "PENDING";

type AlertCompletion = {
  status: AlertStatus;
  error?: string | null;
  nextAttemptMs?: number;
  pruneAfterPermanentRejects?: number;
};

export type OutboxInsert = { chatId: number; text: string; contextJson: string };
export type OutboxRow = {
  id: number;
  chat_id: number;
  proxy_address: string;
  text: string;
  context_json: string;
  attempts: number;
};

type ChatSessionRow = {
  telegram_chat_id: number;
  mode: ChatSessionRecord["mode"];
  awaiting_user_id: number | null;
  expires_at_ms: number | null;
};

type TrackedAccountRow = {
  proxy_address: string;
  name: string | null;
  pseudonym: string | null;
  profile_slug: string | null;
  last_seen_timestamp: number;
  last_seen_keys_json: string;
  skip_activity_through_timestamp: number;
  last_polled_at_ms: number | null;
  next_poll_at_ms: number;
  consecutive_errors: number;
  consecutive_page_budget_exhaustions: number;
  inflight: number;
  inflight_started_at_ms: number | null;
};

type ChatSubscriptionRow = {
  proxyAddress: string;
  name: string | null;
  pseudonym: string | null;
  profileSlug: string | null;
  notifyFromTs: number;
};

type AccountSubscriptionRow = {
  telegramChatId: number;
  notifyFromTs: number;
};

type AdminSubscriberStatsRow = {
  telegramUserId: number;
  telegramChatId: number;
  subscriptionsCount: number;
};

type AdminUsersReportRow = {
  totalUsers: number;
  lastUserJoinedAt: string | null;
};

type AdminSubscriptionsReportRow = {
  totalSubscriptions: number;
  totalUsers: number;
  lastSubscriptionCreatedAt: string | null;
  lastAlertSentAtMs: number | null;
};

type AdminProfileStatRow = {
  proxyAddress: string;
  subscriptionsCount: number;
};

type AlertDelaySampleReportRow = {
  activity_timestamp: number;
  poll_started_at_ms: number | null;
  fetch_completed_at_ms: number | null;
  send_started_at_ms: number;
  send_completed_at_ms: number | null;
  delay_ms: number;
  delay_reason_code: DelayReasonCode | null;
};

const ALERT_DELAY_RETENTION_MS = 24 * 60 * 60 * 1000;
const ALERT_DELAY_MAX_ROWS = 25_000;
const SECOND_MS = 1000;

const DELAY_BUCKET_DEFINITIONS: Array<{ label: DelayBucketLabel; minMs: number; maxMs?: number }> = [
  { label: "<5s", minMs: 0, maxMs: 5 * SECOND_MS },
  { label: "5-10s", minMs: 5 * SECOND_MS, maxMs: 10 * SECOND_MS },
  { label: "10-30s", minMs: 10 * SECOND_MS, maxMs: 30 * SECOND_MS },
  { label: "30-60s", minMs: 30 * SECOND_MS, maxMs: 60 * SECOND_MS },
  { label: "1-2m", minMs: 60 * SECOND_MS, maxMs: 2 * 60 * SECOND_MS },
  { label: "2-5m", minMs: 2 * 60 * SECOND_MS, maxMs: 5 * 60 * SECOND_MS },
  { label: "5-10m", minMs: 5 * 60 * SECOND_MS, maxMs: 10 * 60 * SECOND_MS },
  { label: ">10m", minMs: 10 * 60 * SECOND_MS }
];

const DELAY_REASON_ORDER: readonly DelayReasonCode[] = [
  "upstream_stale",
  "poll_fetch_slow",
  "dispatch_backlog",
  "telegram_slow"
];

function parseKeysJson(value: string): string[] {
  try {
    const parsed = JSON.parse(value);
    if (Array.isArray(parsed)) {
      return parsed.filter((item): item is string => typeof item === "string");
    }
  } catch {
    return [];
  }

  return [];
}

function mapTrackedAccount(row: TrackedAccountRow): TrackedAccountRecord {
  return {
    proxyAddress: row.proxy_address,
    name: row.name ?? undefined,
    pseudonym: row.pseudonym ?? undefined,
    profileSlug: row.profile_slug ?? undefined,
    lastSeenTimestamp: row.last_seen_timestamp,
    lastSeenKeys: parseKeysJson(row.last_seen_keys_json),
    skipActivityThroughTimestamp: row.skip_activity_through_timestamp,
    lastPolledAtMs: row.last_polled_at_ms ?? undefined,
    nextPollAtMs: row.next_poll_at_ms,
    consecutiveErrors: row.consecutive_errors,
    consecutivePageBudgetExhaustions: row.consecutive_page_budget_exhaustions,
    inflight: row.inflight === 1,
    inflightStartedAtMs: row.inflight_started_at_ms ?? undefined
  };
}

function mapSession(row: ChatSessionRow): ChatSessionRecord {
  return {
    telegramChatId: row.telegram_chat_id,
    mode: row.mode,
    awaitingUserId: row.awaiting_user_id ?? undefined,
    expiresAtMs: row.expires_at_ms ?? undefined
  };
}

function toIsoUtc(ms: number): string {
  return new Date(ms).toISOString();
}

function clampDurationMs(startMs: number, endMs: number): number {
  return Math.max(0, endMs - startMs);
}

function findDelayBucketLabel(delayMs: number): DelayBucketLabel {
  const match = DELAY_BUCKET_DEFINITIONS.find((bucket) => {
    if (delayMs < bucket.minMs) {
      return false;
    }
    return typeof bucket.maxMs === "number" ? delayMs < bucket.maxMs : true;
  });

  return match?.label ?? ">10m";
}

export class AppRepository {
  private readonly listReadyAlerts: (nowMs: number, limit: number) => OutboxRow[];

  public constructor(private readonly db: Database.Database) {
    ensureSchema(this.db);
    const cancelOrphans = this.db.prepare(`UPDATE alert_outbox
      SET status='CANCELLED', next_attempt_ms=0, last_error='Subscription removed',
        updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE status='PENDING' AND NOT EXISTS
        (SELECT 1 FROM subscriptions WHERE id=alert_outbox.subscription_id)`);
    const rejectExhausted = this.db.prepare(`UPDATE alert_outbox
      SET status='REJECTED', next_attempt_ms=0, last_error=?,
        updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE status='PENDING' AND attempts>=?`);
    // Strict FIFO within each chat preserves the order of separately queued message chunks.
    const ready = this.db.prepare(`SELECT * FROM alert_outbox AS candidate
      WHERE status='PENDING' AND next_attempt_ms<=?
      AND NOT EXISTS (SELECT 1 FROM alert_outbox AS earlier WHERE earlier.chat_id=candidate.chat_id
        AND earlier.id<candidate.id AND earlier.status IN ('PENDING','SENDING'))
      ORDER BY id LIMIT ?`);
    this.listReadyAlerts = this.db.transaction((nowMs: number, limit: number) => {
      cancelOrphans.run();
      rejectExhausted.run(ATTEMPTS_EXHAUSTED, MAX_ALERT_ATTEMPTS);
      return ready.all(nowMs, limit) as OutboxRow[];
    }).immediate;
  }

  public close(): void {
    this.db.close();
  }

  public upsertSubscriber(telegramUserId: number, telegramChatId: number, chatType: string): number {
    this.db
      .prepare(
        `
          INSERT INTO subscribers (telegram_user_id, telegram_chat_id, chat_type)
          VALUES (?, ?, ?)
          ON CONFLICT (telegram_user_id, telegram_chat_id) DO UPDATE SET
            chat_type = excluded.chat_type,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        `
      )
      .run(telegramUserId, telegramChatId, chatType);

    const row = this.db
      .prepare(
        `
          SELECT id
          FROM subscribers
          WHERE telegram_user_id = ? AND telegram_chat_id = ?
          LIMIT 1
        `
      )
      .get(telegramUserId, telegramChatId) as { id: number } | undefined;

    if (!row) {
      throw new Error("Failed to read subscriber id after upsert");
    }

    return row.id;
  }

  public getChatSession(chatId: number): ChatSessionRecord | null {
    const row = this.db
      .prepare(
        `
          SELECT telegram_chat_id, mode, awaiting_user_id, expires_at_ms
          FROM chat_sessions
          WHERE telegram_chat_id = ?
          LIMIT 1
        `
      )
      .get(chatId) as ChatSessionRow | undefined;

    return row ? mapSession(row) : null;
  }

  public setChatSessionAwaiting(chatId: number, awaitingUserId: number, expiresAtMs: number): void {
    this.db
      .prepare(
        `
          INSERT INTO chat_sessions (telegram_chat_id, mode, awaiting_user_id, expires_at_ms)
          VALUES (?, 'awaiting_subscribe', ?, ?)
          ON CONFLICT (telegram_chat_id) DO UPDATE SET
            mode = 'awaiting_subscribe',
            awaiting_user_id = excluded.awaiting_user_id,
            expires_at_ms = excluded.expires_at_ms,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        `
      )
      .run(chatId, awaitingUserId, expiresAtMs);
  }

  public clearChatSession(chatId: number): void {
    this.db
      .prepare(
        `
          INSERT INTO chat_sessions (telegram_chat_id, mode, awaiting_user_id, expires_at_ms)
          VALUES (?, 'idle', NULL, NULL)
          ON CONFLICT (telegram_chat_id) DO UPDATE SET
            mode = 'idle',
            awaiting_user_id = NULL,
            expires_at_ms = NULL,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        `
      )
      .run(chatId);
  }

  public clearExpiredChatSessions(nowMs: number): number {
    const result = this.db
      .prepare(
        `
          UPDATE chat_sessions
          SET
            mode = 'idle',
            awaiting_user_id = NULL,
            expires_at_ms = NULL,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          WHERE mode = 'awaiting_subscribe'
            AND expires_at_ms IS NOT NULL
            AND expires_at_ms <= ?
        `
      )
      .run(nowMs);

    return result.changes;
  }

  public upsertTrackedAccount(account: {
    proxyAddress: string;
    name?: string | undefined;
    pseudonym?: string | undefined;
    profileSlug?: string | undefined;
    nextPollAtMs: number;
  }): void {
    this.db
      .prepare(
        `
          INSERT INTO tracked_accounts (
            proxy_address,
            name,
            pseudonym,
            profile_slug,
            next_poll_at_ms
          )
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT (proxy_address) DO UPDATE SET
            name = COALESCE(excluded.name, tracked_accounts.name),
            pseudonym = COALESCE(excluded.pseudonym, tracked_accounts.pseudonym),
            profile_slug = COALESCE(excluded.profile_slug, tracked_accounts.profile_slug),
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        `
      )
      .run(
        account.proxyAddress.toLowerCase(),
        account.name ?? null,
        account.pseudonym ?? null,
        account.profileSlug ?? null,
        account.nextPollAtMs
      );
  }

  public getTrackedAccount(proxyAddress: string): TrackedAccountRecord | null {
    const row = this.db
      .prepare(
        `
          SELECT
            proxy_address,
            name,
            pseudonym,
            profile_slug,
            last_seen_timestamp,
            last_seen_keys_json,
            skip_activity_through_timestamp,
            last_polled_at_ms,
            next_poll_at_ms,
            consecutive_errors,
            consecutive_page_budget_exhaustions,
            inflight,
            inflight_started_at_ms
          FROM tracked_accounts
          WHERE proxy_address = ?
          LIMIT 1
        `
      )
      .get(proxyAddress.toLowerCase()) as TrackedAccountRow | undefined;

    return row ? mapTrackedAccount(row) : null;
  }

  public createSubscription(
    subscriberId: number,
    chatId: number,
    proxyAddress: string,
    createdByUserId: number,
    notifyFromTs: number
  ): { created: boolean } {
    return this.db
      .transaction(() => {
        const result = this.db
          .prepare(
            `
            INSERT OR IGNORE INTO subscriptions (
              subscriber_id,
              telegram_chat_id,
              proxy_address,
              created_by_user_id,
              notify_from_ts
            )
            VALUES (?, ?, ?, ?, ?)
          `
          )
          .run(subscriberId, chatId, proxyAddress.toLowerCase(), createdByUserId, notifyFromTs);

        const created = result.changes > 0;
        if (created) this.clearChatUnavailableStreak(chatId);
        return { created };
      })
      .immediate();
  }

  public listChatSubscriptions(chatId: number): ChatSubscriptionView[] {
    const rows = this.db
      .prepare(
        `
          SELECT
            s.proxy_address AS proxyAddress,
            t.name AS name,
            t.pseudonym AS pseudonym,
            t.profile_slug AS profileSlug,
            s.notify_from_ts AS notifyFromTs
          FROM subscriptions s
          JOIN tracked_accounts t ON t.proxy_address = s.proxy_address
          WHERE s.telegram_chat_id = ?
          ORDER BY COALESCE(t.name, t.pseudonym, t.profile_slug, t.proxy_address) COLLATE NOCASE ASC
        `
      )
      .all(chatId) as ChatSubscriptionRow[];

    return rows.map((row) => ({
      proxyAddress: row.proxyAddress,
      name: row.name ?? undefined,
      pseudonym: row.pseudonym ?? undefined,
      profileSlug: row.profileSlug ?? undefined,
      notifyFromTs: row.notifyFromTs
    }));
  }

  public listSubscriptionsForAccount(proxyAddress: string): AccountSubscriptionRecord[] {
    return this.db
      .prepare(
        `
          SELECT
            telegram_chat_id AS telegramChatId,
            notify_from_ts AS notifyFromTs
          FROM subscriptions
          WHERE proxy_address = ?
          ORDER BY telegram_chat_id ASC
        `
      )
      .all(proxyAddress.toLowerCase()) as AccountSubscriptionRow[];
  }

  public getAdminSubscriberStats(): AdminSubscriberStats {
    const rows = this.db
      .prepare(
        `
          SELECT
            sub.telegram_user_id AS telegramUserId,
            sub.telegram_chat_id AS telegramChatId,
            COUNT(s.id) AS subscriptionsCount
          FROM subscribers sub
          JOIN subscriptions s ON s.subscriber_id = sub.id
          GROUP BY sub.id, sub.telegram_user_id, sub.telegram_chat_id
          ORDER BY subscriptionsCount DESC, sub.telegram_chat_id ASC, sub.telegram_user_id ASC
        `
      )
      .all() as AdminSubscriberStatsRow[];

    let totalSubscriptions = 0;
    for (const row of rows) {
      totalSubscriptions += row.subscriptionsCount;
    }

    return {
      rows,
      totalSubscribers: rows.length,
      totalSubscriptions
    };
  }

  public getAdminUsersReport(): AdminUsersReport {
    const row = this.db
      .prepare(
        `
          SELECT
            COUNT(DISTINCT telegram_user_id) AS totalUsers,
            MAX(created_at) AS lastUserJoinedAt
          FROM subscribers
          WHERE EXISTS (
            SELECT 1
            FROM subscriptions
            WHERE subscriptions.subscriber_id = subscribers.id
          )
        `
      )
      .get() as AdminUsersReportRow;

    return {
      totalUsers: row.totalUsers,
      lastUserJoinedAt: row.lastUserJoinedAt ?? undefined
    };
  }

  public getAdminSubscriptionsReport(): AdminSubscriptionsReport {
    const row = this.db
      .prepare(
        `
          SELECT
            (SELECT COUNT(*) FROM subscriptions) AS totalSubscriptions,
            (
              SELECT COUNT(DISTINCT subscribers.telegram_user_id)
              FROM subscribers
              JOIN subscriptions ON subscriptions.subscriber_id = subscribers.id
            ) AS totalUsers,
            (SELECT MAX(created_at) FROM subscriptions) AS lastSubscriptionCreatedAt,
            (SELECT MAX(send_started_at_ms) FROM alert_delay_samples) AS lastAlertSentAtMs
        `
      )
      .get() as AdminSubscriptionsReportRow;

    return {
      totalSubscriptions: row.totalSubscriptions,
      avgSubscriptionsPerUser: row.totalUsers > 0 ? row.totalSubscriptions / row.totalUsers : 0,
      lastSubscriptionCreatedAt: row.lastSubscriptionCreatedAt ?? undefined,
      lastAlertSentAt: typeof row.lastAlertSentAtMs === "number" ? toIsoUtc(row.lastAlertSentAtMs) : undefined
    };
  }

  public listAdminProfileStats(): AdminProfileStat[] {
    return this.db
      .prepare(
        `
          SELECT
            proxy_address AS proxyAddress,
            COUNT(*) AS subscriptionsCount
          FROM subscriptions
          GROUP BY proxy_address
          ORDER BY subscriptionsCount DESC, proxyAddress ASC
        `
      )
      .all() as AdminProfileStatRow[];
  }

  public insertAlertDelaySamples(samples: readonly AlertDelaySampleInsert[]): void {
    if (samples.length === 0) {
      return;
    }

    const insert = this.db.prepare(
      `
        INSERT INTO alert_delay_samples (
          telegram_chat_id,
          proxy_address,
          activity_timestamp,
          poll_started_at_ms,
          fetch_completed_at_ms,
          send_started_at_ms,
          send_completed_at_ms,
          delay_ms,
          delay_reason_code,
          created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `
    );

    const tx = this.db.transaction((rows: readonly AlertDelaySampleInsert[]) => {
      for (const row of rows) {
        insert.run(
          row.telegramChatId,
          row.proxyAddress.toLowerCase(),
          row.activityTimestamp,
          row.pollStartedAtMs ?? null,
          row.fetchCompletedAtMs ?? null,
          row.sendStartedAtMs,
          row.sendCompletedAtMs ?? null,
          row.delayMs,
          row.delayReasonCode ?? null,
          toIsoUtc(row.sendStartedAtMs)
        );
      }
    });

    tx(samples);
  }

  public getAdminPerformanceReport(nowMs: number, thresholdMs = 30 * SECOND_MS): AdminPerformanceReport {
    const bucketCounts = new Map<DelayBucketLabel, number>(
      DELAY_BUCKET_DEFINITIONS.map((bucket) => [bucket.label, 0] as const)
    );
    const reasonCounts = new Map<DelayReasonCode, number>(
      DELAY_REASON_ORDER.map((reason) => [reason, 0] as const)
    );
    const rows = this.db
      .prepare(
        `
          SELECT
            activity_timestamp,
            poll_started_at_ms,
            fetch_completed_at_ms,
            send_started_at_ms,
            send_completed_at_ms,
            delay_ms,
            delay_reason_code
          FROM alert_delay_samples
          WHERE send_started_at_ms >= ?
        `
      )
      .all(nowMs - ALERT_DELAY_RETENTION_MS) as AlertDelaySampleReportRow[];

    if (rows.length === 0) {
      return {
        thresholdMs,
        sampleCount: 0,
        classifiedSampleCount: 0,
        alertsOverThresholdCount: 0,
        upstreamStaleCount: 0,
        delayBuckets: DELAY_BUCKET_DEFINITIONS.map((bucket) => ({ label: bucket.label, count: 0 })),
        delayReasonCounts: DELAY_REASON_ORDER.map((reason) => ({ reason, count: 0 }))
      };
    }

    let minDelayMs: number | undefined;
    let maxDelayMs: number | undefined;
    let totalDelayMs = 0;
    let classifiedSampleCount = 0;
    let alertsOverThresholdCount = 0;
    let upstreamStaleCount = 0;
    let freshLocalMaxDelayMs: number | undefined;

    for (const row of rows) {
      minDelayMs = typeof minDelayMs === "number" ? Math.min(minDelayMs, row.delay_ms) : row.delay_ms;
      maxDelayMs = typeof maxDelayMs === "number" ? Math.max(maxDelayMs, row.delay_ms) : row.delay_ms;
      totalDelayMs += row.delay_ms;
      const bucketLabel = findDelayBucketLabel(row.delay_ms);
      bucketCounts.set(bucketLabel, (bucketCounts.get(bucketLabel) ?? 0) + 1);
      if (row.delay_ms >= thresholdMs) {
        alertsOverThresholdCount += 1;
      }

      const pollStartedAtMs = row.poll_started_at_ms;
      const sendCompletedAtMs = row.send_completed_at_ms;
      const activityTimestampMs = row.activity_timestamp * SECOND_MS;
      if (typeof pollStartedAtMs === "number" && typeof sendCompletedAtMs === "number") {
        const ageAtPollStartMs = clampDurationMs(activityTimestampMs, pollStartedAtMs);
        if (ageAtPollStartMs < thresholdMs) {
          const freshLocalDelayMs = clampDurationMs(pollStartedAtMs, sendCompletedAtMs);
          freshLocalMaxDelayMs =
            typeof freshLocalMaxDelayMs === "number"
              ? Math.max(freshLocalMaxDelayMs, freshLocalDelayMs)
              : freshLocalDelayMs;
        }
      }

      if (row.delay_reason_code) {
        classifiedSampleCount += 1;
        reasonCounts.set(row.delay_reason_code, (reasonCounts.get(row.delay_reason_code) ?? 0) + 1);
        if (row.delay_reason_code === "upstream_stale") {
          upstreamStaleCount += 1;
        }
      }
    }

    return {
      thresholdMs,
      sampleCount: rows.length,
      classifiedSampleCount,
      minDelayMs,
      maxDelayMs,
      avgDelayMs: totalDelayMs / rows.length,
      freshLocalMaxDelayMs,
      alertsOverThresholdCount,
      upstreamStaleCount,
      delayBuckets: DELAY_BUCKET_DEFINITIONS.map<DelayBucketCount>((bucket) => ({
        label: bucket.label,
        count: bucketCounts.get(bucket.label) ?? 0
      })),
      delayReasonCounts: DELAY_REASON_ORDER.map<DelayReasonCount>((reason) => ({
        reason,
        count: reasonCounts.get(reason) ?? 0
      }))
    };
  }

  public cleanupOldAlertDelaySamples(nowMs: number): number {
    const result = this.db
      .prepare(
        `
          DELETE FROM alert_delay_samples
          WHERE created_at < ?
        `
      )
      .run(toIsoUtc(nowMs - ALERT_DELAY_RETENTION_MS));

    return result.changes;
  }

  public cleanupAlertDelaySamplesOverflow(maxRows = ALERT_DELAY_MAX_ROWS): number {
    const result = this.db
      .prepare(
        `
          DELETE FROM alert_delay_samples
          WHERE id IN (
            SELECT id
            FROM alert_delay_samples
            ORDER BY send_started_at_ms DESC, id DESC
            LIMIT -1 OFFSET ?
          )
        `
      )
      .run(maxRows);

    return result.changes;
  }

  public cleanupOldPollRuns(nowMs: number, retentionHours: number): number {
    const retentionMs = retentionHours * 60 * 60 * 1000;
    const result = this.db
      .prepare(
        `
          DELETE FROM poll_runs
          WHERE created_at < ?
        `
      )
      .run(toIsoUtc(nowMs - retentionMs));

    return result.changes;
  }

  public removeSubscription(
    chatId: number,
    proxyAddress: string
  ): { removedSubscriptions: number; removedTrackedAccounts: number } {
    const tx = this.db.transaction((safeChatId: number, safeProxyAddress: string) => {
      this.cancelPendingSubscriptions(safeChatId, safeProxyAddress);
      const removedSubscriptions = this.db
        .prepare(
          `
            DELETE FROM subscriptions
            WHERE telegram_chat_id = ? AND proxy_address = ?
          `
        )
        .run(safeChatId, safeProxyAddress).changes;

      if (removedSubscriptions > 0) this.clearChatUnavailableStreak(safeChatId);
      const removedTrackedAccounts =
        removedSubscriptions > 0 ? this.cleanupTrackedAccountIfOrphaned(safeProxyAddress) : 0;
      return { removedSubscriptions, removedTrackedAccounts };
    });

    return tx.immediate(chatId, proxyAddress.toLowerCase());
  }

  public removeAllSubscriptions(chatId: number): {
    removedSubscriptions: number;
    removedTrackedAccounts: number;
  } {
    const tx = this.db.transaction((safeChatId: number) => {
      const proxyRows = this.db
        .prepare(
          `
            SELECT DISTINCT proxy_address
            FROM subscriptions
            WHERE telegram_chat_id = ?
          `
        )
        .all(safeChatId) as Array<{ proxy_address: string }>;

      this.cancelPendingSubscriptions(safeChatId);
      this.clearChatUnavailableStreak(safeChatId);
      const removedSubscriptions = this.db
        .prepare(
          `
            DELETE FROM subscriptions
            WHERE telegram_chat_id = ?
          `
        )
        .run(safeChatId).changes;

      let removedTrackedAccounts = 0;
      for (const row of proxyRows) {
        removedTrackedAccounts += this.cleanupTrackedAccountIfOrphaned(row.proxy_address);
      }

      return { removedSubscriptions, removedTrackedAccounts };
    });

    return tx.immediate(chatId);
  }

  private clearChatUnavailableStreak(chatId: number): void {
    this.db.prepare("DELETE FROM chat_unavailable_streaks WHERE chat_id=?").run(chatId);
  }

  private cancelPendingSubscriptions(chatId: number, proxyAddress: string | null = null): void {
    this.db
      .prepare(`UPDATE alert_outbox
      SET status='CANCELLED', next_attempt_ms=0, last_error='Subscription removed',
        updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE status='PENDING' AND subscription_id IN (
        SELECT id FROM subscriptions WHERE telegram_chat_id=? AND (? IS NULL OR proxy_address=?)
      )`)
      .run(chatId, proxyAddress, proxyAddress);
  }

  public cleanupTrackedAccountIfOrphaned(proxyAddress: string): number {
    const result = this.db
      .prepare(
        `
          DELETE FROM tracked_accounts
          WHERE proxy_address = ?
            AND NOT EXISTS (
              SELECT 1
              FROM subscriptions
              WHERE subscriptions.proxy_address = tracked_accounts.proxy_address
            )
        `
      )
      .run(proxyAddress.toLowerCase());

    return result.changes;
  }

  public cleanupOrphanTrackedAccounts(): number {
    const result = this.db
      .prepare(
        `
          DELETE FROM tracked_accounts
          WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions
            WHERE subscriptions.proxy_address = tracked_accounts.proxy_address
          )
        `
      )
      .run();

    return result.changes;
  }

  public countTrackedAccounts(): number {
    const row = this.db.prepare(`SELECT COUNT(*) AS count FROM tracked_accounts`).get() as { count: number };
    return row.count;
  }

  public countInflightTrackedAccounts(): number {
    const row = this.db
      .prepare(`SELECT COUNT(*) AS count FROM tracked_accounts WHERE inflight = 1`)
      .get() as { count: number };
    return row.count;
  }

  public countDueTrackedAccounts(nowMs: number): number {
    const row = this.db
      .prepare(
        `
          SELECT COUNT(*) AS count
          FROM tracked_accounts
          WHERE inflight = 0 AND next_poll_at_ms <= ?
        `
      )
      .get(nowMs) as { count: number };

    return row.count;
  }

  public listDueTrackedAccounts(nowMs: number, limit: number): TrackedAccountRecord[] {
    const rows = this.db
      .prepare(
        `
          SELECT
            proxy_address,
            name,
            pseudonym,
            profile_slug,
            last_seen_timestamp,
            last_seen_keys_json,
            skip_activity_through_timestamp,
            last_polled_at_ms,
            next_poll_at_ms,
            consecutive_errors,
            consecutive_page_budget_exhaustions,
            inflight,
            inflight_started_at_ms
          FROM tracked_accounts
          WHERE inflight = 0 AND next_poll_at_ms <= ?
          ORDER BY next_poll_at_ms ASC, proxy_address ASC
          LIMIT ?
        `
      )
      .all(nowMs, limit) as TrackedAccountRow[];

    return rows.map((row) => mapTrackedAccount(row));
  }

  public tryClaimTrackedAccount(proxyAddress: string, nowMs: number): boolean {
    const result = this.db
      .prepare(
        `
          UPDATE tracked_accounts
          SET
            inflight = 1,
            inflight_started_at_ms = ?,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          WHERE proxy_address = ?
            AND inflight = 0
            AND next_poll_at_ms <= ?
        `
      )
      .run(nowMs, proxyAddress.toLowerCase(), nowMs);

    return result.changes > 0;
  }

  public markPollSuccess(payload: {
    proxyAddress: string;
    lastSeenTimestamp: number;
    lastSeenKeys: string[];
    lastPolledAtMs: number;
    nextPollAtMs: number;
    alerts?: OutboxInsert[];
    claimStartedAtMs?: number;
    fastForward?: boolean;
  }): boolean {
    return this.db
      .transaction(() => {
        const proxyAddress = payload.proxyAddress.toLowerCase();
        const updated = this.db
          .prepare(`
            UPDATE tracked_accounts
            SET
              last_seen_timestamp = @timestamp,
              last_seen_keys_json = @keys,
              skip_activity_through_timestamp = MAX(skip_activity_through_timestamp, @skipThrough),
              last_polled_at_ms = @lastPolledAtMs,
              next_poll_at_ms = @nextPollAtMs,
              consecutive_errors = 0,
              consecutive_page_budget_exhaustions = 0,
              inflight = 0,
              inflight_started_at_ms = NULL,
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
            WHERE proxy_address = @proxyAddress
              AND (@claim IS NULL OR (inflight = 1 AND inflight_started_at_ms = @claim))
          `)
          .run({
            proxyAddress,
            timestamp: payload.lastSeenTimestamp,
            keys: JSON.stringify(payload.lastSeenKeys),
            skipThrough: payload.fastForward ? payload.lastSeenTimestamp : 0,
            lastPolledAtMs: payload.lastPolledAtMs,
            nextPollAtMs: payload.nextPollAtMs,
            claim: payload.claimStartedAtMs ?? null
          });
        if (updated.changes !== 1) return false;

        for (const alert of payload.fastForward ? [] : (payload.alerts ?? [])) {
          this.db
            .prepare(`INSERT INTO alert_outbox(subscription_id,proxy_address,chat_id,text,context_json)
          SELECT id,proxy_address,telegram_chat_id,?,? FROM subscriptions
          WHERE proxy_address=? AND telegram_chat_id=?`)
            .run(alert.text, alert.contextJson, proxyAddress, alert.chatId);
        }
        return true;
      })
      .immediate();
  }

  public recoverOutbox(): number {
    return this.db
      .transaction(() => {
        this.db
          .prepare(`DELETE FROM chat_unavailable_streaks WHERE chat_id IN (
        SELECT chat_id FROM alert_outbox WHERE status='SENDING')`)
          .run();
        return this.db
          .prepare(
            "UPDATE alert_outbox SET status='UNKNOWN',last_error='Process stopped during send' WHERE status='SENDING'"
          )
          .run().changes;
      })
      .immediate();
  }

  public listPendingAlerts(nowMs: number, limit = 100): OutboxRow[] {
    return this.listReadyAlerts(nowMs, limit);
  }

  public claimAlert(id: number): boolean {
    return (
      this.db
        .prepare(`UPDATE alert_outbox SET status='SENDING',attempts=attempts+1
      WHERE id=? AND status='PENDING' AND attempts<? AND EXISTS
      (SELECT 1 FROM subscriptions WHERE id=alert_outbox.subscription_id)`)
        .run(id, MAX_ALERT_ATTEMPTS).changes === 1
    );
  }

  public finishAlert(
    id: number,
    status: AlertStatus,
    error: string | null = null,
    nextAttemptMs = 0
  ): boolean {
    return this.settleAlert(id, { status, error, nextAttemptMs }).updated;
  }

  public settleAlert(
    id: number,
    completion: AlertCompletion
  ): {
    updated: boolean;
    removedSubscriptions: number;
  } {
    const { status, error = null, nextAttemptMs = 0, pruneAfterPermanentRejects = 0 } = completion;
    return this.db
      .transaction(() => {
        const row = this.db
          .prepare(`UPDATE alert_outbox SET
          status=CASE WHEN @status='PENDING' AND attempts>=@maxAttempts THEN 'REJECTED' ELSE @status END,
          last_error=CASE WHEN @status='PENDING' AND attempts>=@maxAttempts THEN @exhausted ELSE @error END,
          next_attempt_ms=CASE WHEN @status='PENDING' AND attempts<@maxAttempts THEN @nextAttemptMs ELSE 0 END,
          updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
          WHERE id=@id AND status='SENDING'
          RETURNING chat_id, subscription_id`)
          .get({
            id,
            status,
            error,
            nextAttemptMs,
            maxAttempts: MAX_ALERT_ATTEMPTS,
            exhausted: ATTEMPTS_EXHAUSTED
          }) as { chat_id: number; subscription_id: number } | undefined;
        if (!row) return { updated: false, removedSubscriptions: 0 };

        if (status !== "REJECTED" || pruneAfterPermanentRejects === 0) {
          this.clearChatUnavailableStreak(row.chat_id);
          return { updated: true, removedSubscriptions: 0 };
        }

        // A rejection from a subscription removed during the send cannot prune a new subscription.
        const streak = this.db
          .prepare(`INSERT INTO chat_unavailable_streaks(chat_id, consecutive_rejects)
          SELECT telegram_chat_id, 1 FROM subscriptions WHERE id=? AND telegram_chat_id=?
          ON CONFLICT(chat_id) DO UPDATE SET consecutive_rejects=consecutive_rejects+1
          RETURNING consecutive_rejects`)
          .get(row.subscription_id, row.chat_id) as { consecutive_rejects: number } | undefined;
        const removedSubscriptions =
          streak && streak.consecutive_rejects >= pruneAfterPermanentRejects
            ? this.removeAllSubscriptions(row.chat_id).removedSubscriptions
            : 0;
        return { updated: true, removedSubscriptions };
      })
      .immediate();
  }

  public outboxCounts(): Record<string, number> {
    return Object.fromEntries(
      (
        this.db.prepare("SELECT status,count(*) AS n FROM alert_outbox GROUP BY status").all() as Array<{
          status: string;
          n: number;
        }>
      ).map((row) => [row.status, row.n])
    );
  }

  public markPollFailure(
    proxyAddress: string,
    nextPollAtMs: number,
    consecutiveErrors: number,
    options: { claimStartedAtMs?: number; consecutivePageBudgetExhaustions?: number } = {}
  ): boolean {
    const updated = this.db
      .prepare(`
        UPDATE tracked_accounts
        SET
          next_poll_at_ms = @nextPollAtMs,
          consecutive_errors = @consecutiveErrors,
          consecutive_page_budget_exhaustions = @budgetExhaustions,
          inflight = 0,
          inflight_started_at_ms = NULL,
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        WHERE proxy_address = @proxyAddress
          AND (@claim IS NULL OR (inflight = 1 AND inflight_started_at_ms = @claim))
      `)
      .run({
        nextPollAtMs,
        consecutiveErrors,
        proxyAddress: proxyAddress.toLowerCase(),
        budgetExhaustions: options.consecutivePageBudgetExhaustions ?? 0,
        claim: options.claimStartedAtMs ?? null
      });
    return updated.changes === 1;
  }

  public clearStaleInflight(nowMs: number, staleAfterMs: number): number {
    const threshold = nowMs - staleAfterMs;
    const result = this.db
      .prepare(
        `
          UPDATE tracked_accounts
          SET
            inflight = 0,
            inflight_started_at_ms = NULL,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          WHERE inflight = 1
            AND inflight_started_at_ms IS NOT NULL
            AND inflight_started_at_ms <= ?
        `
      )
      .run(threshold);

    return result.changes;
  }

  public rebalanceTrackedAccountIntervals(nowMs: number, intervalMs: number): number {
    const result = this.db
      .prepare(
        `
          UPDATE tracked_accounts
          SET
            next_poll_at_ms = CASE
              WHEN last_polled_at_ms IS NULL THEN MIN(next_poll_at_ms, ?)
              ELSE MAX(next_poll_at_ms, last_polled_at_ms + ?)
            END,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          WHERE inflight = 0
        `
      )
      .run(nowMs, intervalMs);

    return result.changes;
  }

  public checkpointWalTruncate(): void {
    this.db.pragma("wal_checkpoint(TRUNCATE)");
  }

  public insertPollRun(row: PollRunRecord): void {
    this.db
      .prepare(
        `
          INSERT INTO poll_runs (
            kind,
            proxy_address,
            status,
            duration_ms,
            api_calls,
            retries,
            fetched_rows,
            new_rows,
            alerts_sent,
            queue_depth,
            tracked_accounts,
            inflight_accounts,
            effective_interval_ms,
            throttle_wait_ms,
            note
          )
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `
      )
      .run(
        row.kind,
        row.proxyAddress ?? null,
        row.status,
        row.durationMs,
        row.apiCalls,
        row.retries,
        row.fetchedRows,
        row.newRows,
        row.alertsSent,
        row.queueDepth,
        row.trackedAccounts,
        row.inflightAccounts,
        row.effectiveIntervalMs,
        row.throttleWaitMs,
        row.note ?? null
      );
  }

  public getFreshnessStats(): FreshnessStats {
    const row = this.db
      .prepare(
        `
          SELECT
            COUNT(*) AS trackedAccounts,
            SUM(CASE WHEN last_seen_timestamp > 0 THEN 1 ELSE 0 END) AS initializedAccounts,
            MAX(last_seen_timestamp) AS newestSeenTimestamp
          FROM tracked_accounts
        `
      )
      .get() as {
      trackedAccounts: number;
      initializedAccounts: number | null;
      newestSeenTimestamp: number | null;
    };

    return {
      trackedAccounts: row.trackedAccounts,
      initializedAccounts: row.initializedAccounts ?? 0,
      newestSeenTimestamp: row.newestSeenTimestamp ?? 0
    };
  }
}
