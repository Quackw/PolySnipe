import { ensureSchema } from "./schema.js";
const ALERT_DELAY_RETENTION_MS = 24 * 60 * 60 * 1000;
const ALERT_DELAY_MAX_ROWS = 25_000;
const SECOND_MS = 1000;
const DELAY_BUCKET_DEFINITIONS = [
    { label: "<5s", minMs: 0, maxMs: 5 * SECOND_MS },
    { label: "5-10s", minMs: 5 * SECOND_MS, maxMs: 10 * SECOND_MS },
    { label: "10-30s", minMs: 10 * SECOND_MS, maxMs: 30 * SECOND_MS },
    { label: "30-60s", minMs: 30 * SECOND_MS, maxMs: 60 * SECOND_MS },
    { label: "1-2m", minMs: 60 * SECOND_MS, maxMs: 2 * 60 * SECOND_MS },
    { label: "2-5m", minMs: 2 * 60 * SECOND_MS, maxMs: 5 * 60 * SECOND_MS },
    { label: "5-10m", minMs: 5 * 60 * SECOND_MS, maxMs: 10 * 60 * SECOND_MS },
    { label: ">10m", minMs: 10 * 60 * SECOND_MS }
];
const DELAY_REASON_ORDER = [
    "upstream_stale",
    "poll_fetch_slow",
    "dispatch_backlog",
    "telegram_slow"
];
function parseKeysJson(value) {
    try {
        const parsed = JSON.parse(value);
        if (Array.isArray(parsed)) {
            return parsed.filter((item) => typeof item === "string");
        }
    }
    catch {
        return [];
    }
    return [];
}
function mapTrackedAccount(row) {
    return {
        proxyAddress: row.proxy_address,
        name: row.name ?? undefined,
        pseudonym: row.pseudonym ?? undefined,
        profileSlug: row.profile_slug ?? undefined,
        lastSeenTimestamp: row.last_seen_timestamp,
        lastSeenKeys: parseKeysJson(row.last_seen_keys_json),
        lastPolledAtMs: row.last_polled_at_ms ?? undefined,
        nextPollAtMs: row.next_poll_at_ms,
        consecutiveErrors: row.consecutive_errors,
        inflight: row.inflight === 1,
        inflightStartedAtMs: row.inflight_started_at_ms ?? undefined
    };
}
function mapSession(row) {
    return {
        telegramChatId: row.telegram_chat_id,
        mode: row.mode,
        awaitingUserId: row.awaiting_user_id ?? undefined,
        expiresAtMs: row.expires_at_ms ?? undefined
    };
}
function toIsoUtc(ms) {
    return new Date(ms).toISOString();
}
function clampDurationMs(startMs, endMs) {
    return Math.max(0, endMs - startMs);
}
function findDelayBucketLabel(delayMs) {
    const match = DELAY_BUCKET_DEFINITIONS.find((bucket) => {
        if (delayMs < bucket.minMs) {
            return false;
        }
        return typeof bucket.maxMs === "number" ? delayMs < bucket.maxMs : true;
    });
    return match?.label ?? ">10m";
}
export class AppRepository {
    db;
    constructor(db) {
        this.db = db;
        ensureSchema(this.db);
    }
    close() {
        this.db.close();
    }
    hasAccessGrant(userId) {
        return Boolean(this.db.prepare("SELECT 1 FROM access_grants WHERE telegram_user_id = ?").get(userId));
    }
    requestAccess(userId) {
        if (this.hasAccessGrant(userId))
            return "granted";
        const inserted = this.db.prepare("INSERT OR IGNORE INTO access_requests (telegram_user_id, status) VALUES (?, 'pending')").run(userId).changes;
        if (inserted)
            return "created";
        const row = this.db.prepare("SELECT status FROM access_requests WHERE telegram_user_id = ?")
            .get(userId);
        return row.status === "approved" ? "denied" : row.status;
    }
    approveAccess(userId) {
        return this.db.transaction((id) => {
            const changed = this.db.prepare("UPDATE access_requests SET status = 'approved', decided_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE telegram_user_id = ? AND status IN ('pending', 'denied', 'revoked')").run(id).changes;
            if (!changed)
                return false;
            this.db.prepare("INSERT OR IGNORE INTO access_grants (telegram_user_id) VALUES (?)").run(id);
            return true;
        })(userId);
    }
    approvePendingAccess(userId) {
        return this.db.transaction((id) => {
            const changed = this.db.prepare("UPDATE access_requests SET status = 'approved', decided_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE telegram_user_id = ? AND status = 'pending'").run(id).changes;
            if (!changed)
                return false;
            this.db.prepare("INSERT OR IGNORE INTO access_grants (telegram_user_id) VALUES (?)").run(id);
            return true;
        })(userId);
    }
    denyAccess(userId) {
        return this.db.prepare("UPDATE access_requests SET status = 'denied', decided_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE telegram_user_id = ? AND status = 'pending'").run(userId).changes > 0;
    }
    revokeAccess(userId) {
        return this.db.transaction((id) => {
            const removed = this.db.prepare("DELETE FROM access_grants WHERE telegram_user_id = ?").run(id).changes;
            if (!removed)
                return false;
            this.db.prepare("UPDATE access_requests SET status = 'revoked', decided_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE telegram_user_id = ?").run(id);
            this.removeAllSubscriptions(id);
            this.db.prepare("DELETE FROM chat_sessions WHERE telegram_chat_id = ?").run(id);
            return true;
        })(userId);
    }
    listPendingAccessRequests(limit = 20) {
        const rows = this.db.prepare("SELECT telegram_user_id AS id FROM access_requests WHERE status = 'pending' ORDER BY requested_at ASC LIMIT ?").all(limit);
        return rows.map((row) => row.id);
    }
    listAccessGrants(limit = 20) {
        const rows = this.db.prepare("SELECT telegram_user_id AS id FROM access_grants ORDER BY granted_at DESC LIMIT ?").all(limit);
        return rows.map((row) => row.id);
    }
    listBlockedAccessRequests(limit = 20) {
        const rows = this.db.prepare("SELECT telegram_user_id AS id FROM access_requests WHERE status IN ('denied', 'revoked') ORDER BY decided_at DESC LIMIT ?").all(limit);
        return rows.map((row) => row.id);
    }
    upsertSubscriber(telegramUserId, telegramChatId, chatType) {
        this.db
            .prepare(`
          INSERT INTO subscribers (telegram_user_id, telegram_chat_id, chat_type)
          VALUES (?, ?, ?)
          ON CONFLICT (telegram_user_id, telegram_chat_id) DO UPDATE SET
            chat_type = excluded.chat_type,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        `)
            .run(telegramUserId, telegramChatId, chatType);
        const row = this.db
            .prepare(`
          SELECT id
          FROM subscribers
          WHERE telegram_user_id = ? AND telegram_chat_id = ?
          LIMIT 1
        `)
            .get(telegramUserId, telegramChatId);
        if (!row) {
            throw new Error("Failed to read subscriber id after upsert");
        }
        return row.id;
    }
    getChatSession(chatId) {
        const row = this.db
            .prepare(`
          SELECT telegram_chat_id, mode, awaiting_user_id, expires_at_ms
          FROM chat_sessions
          WHERE telegram_chat_id = ?
          LIMIT 1
        `)
            .get(chatId);
        return row ? mapSession(row) : null;
    }
    setChatSessionAwaiting(chatId, awaitingUserId, expiresAtMs) {
        this.db
            .prepare(`
          INSERT INTO chat_sessions (telegram_chat_id, mode, awaiting_user_id, expires_at_ms)
          VALUES (?, 'awaiting_subscribe', ?, ?)
          ON CONFLICT (telegram_chat_id) DO UPDATE SET
            mode = 'awaiting_subscribe',
            awaiting_user_id = excluded.awaiting_user_id,
            expires_at_ms = excluded.expires_at_ms,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        `)
            .run(chatId, awaitingUserId, expiresAtMs);
    }
    clearChatSession(chatId) {
        this.db
            .prepare(`
          INSERT INTO chat_sessions (telegram_chat_id, mode, awaiting_user_id, expires_at_ms)
          VALUES (?, 'idle', NULL, NULL)
          ON CONFLICT (telegram_chat_id) DO UPDATE SET
            mode = 'idle',
            awaiting_user_id = NULL,
            expires_at_ms = NULL,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
        `)
            .run(chatId);
    }
    clearExpiredChatSessions(nowMs) {
        const result = this.db
            .prepare(`
          UPDATE chat_sessions
          SET
            mode = 'idle',
            awaiting_user_id = NULL,
            expires_at_ms = NULL,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          WHERE mode = 'awaiting_subscribe'
            AND expires_at_ms IS NOT NULL
            AND expires_at_ms <= ?
        `)
            .run(nowMs);
        return result.changes;
    }
    upsertTrackedAccount(account) {
        this.db
            .prepare(`
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
        `)
            .run(account.proxyAddress.toLowerCase(), account.name ?? null, account.pseudonym ?? null, account.profileSlug ?? null, account.nextPollAtMs);
    }
    getTrackedAccount(proxyAddress) {
        const row = this.db
            .prepare(`
          SELECT
            proxy_address,
            name,
            pseudonym,
            profile_slug,
            last_seen_timestamp,
            last_seen_keys_json,
            last_polled_at_ms,
            next_poll_at_ms,
            consecutive_errors,
            inflight,
            inflight_started_at_ms
          FROM tracked_accounts
          WHERE proxy_address = ?
          LIMIT 1
        `)
            .get(proxyAddress.toLowerCase());
        return row ? mapTrackedAccount(row) : null;
    }
    createSubscription(subscriberId, chatId, proxyAddress, createdByUserId, notifyFromTs) {
        const result = this.db
            .prepare(`
          INSERT OR IGNORE INTO subscriptions (
            subscriber_id,
            telegram_chat_id,
            proxy_address,
            created_by_user_id,
            notify_from_ts
          )
          VALUES (?, ?, ?, ?, ?)
        `)
            .run(subscriberId, chatId, proxyAddress.toLowerCase(), createdByUserId, notifyFromTs);
        return { created: result.changes > 0 };
    }
    listChatSubscriptions(chatId) {
        const rows = this.db
            .prepare(`
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
        `)
            .all(chatId);
        return rows.map((row) => ({
            proxyAddress: row.proxyAddress,
            name: row.name ?? undefined,
            pseudonym: row.pseudonym ?? undefined,
            profileSlug: row.profileSlug ?? undefined,
            notifyFromTs: row.notifyFromTs
        }));
    }
    listSubscriptionsForAccount(proxyAddress) {
        return this.db
            .prepare(`
          SELECT
            telegram_chat_id AS telegramChatId,
            notify_from_ts AS notifyFromTs
          FROM subscriptions
          WHERE proxy_address = ?
          ORDER BY telegram_chat_id ASC
        `)
            .all(proxyAddress.toLowerCase());
    }
    getAdminSubscriberStats() {
        const rows = this.db
            .prepare(`
          SELECT
            sub.telegram_user_id AS telegramUserId,
            sub.telegram_chat_id AS telegramChatId,
            COUNT(s.id) AS subscriptionsCount
          FROM subscribers sub
          JOIN subscriptions s ON s.subscriber_id = sub.id
          GROUP BY sub.id, sub.telegram_user_id, sub.telegram_chat_id
          ORDER BY subscriptionsCount DESC, sub.telegram_chat_id ASC, sub.telegram_user_id ASC
        `)
            .all();
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
    getAdminUsersReport() {
        const row = this.db
            .prepare(`
          SELECT
            COUNT(DISTINCT telegram_user_id) AS totalUsers,
            MAX(created_at) AS lastUserJoinedAt
          FROM subscribers
          WHERE EXISTS (
            SELECT 1
            FROM subscriptions
            WHERE subscriptions.subscriber_id = subscribers.id
          )
        `)
            .get();
        return {
            totalUsers: row.totalUsers,
            lastUserJoinedAt: row.lastUserJoinedAt ?? undefined
        };
    }
    getAdminSubscriptionsReport() {
        const row = this.db
            .prepare(`
          SELECT
            (SELECT COUNT(*) FROM subscriptions) AS totalSubscriptions,
            (
              SELECT COUNT(DISTINCT subscribers.telegram_user_id)
              FROM subscribers
              JOIN subscriptions ON subscriptions.subscriber_id = subscribers.id
            ) AS totalUsers,
            (SELECT MAX(created_at) FROM subscriptions) AS lastSubscriptionCreatedAt,
            (SELECT MAX(send_started_at_ms) FROM alert_delay_samples) AS lastAlertSentAtMs
        `)
            .get();
        return {
            totalSubscriptions: row.totalSubscriptions,
            avgSubscriptionsPerUser: row.totalUsers > 0 ? row.totalSubscriptions / row.totalUsers : 0,
            lastSubscriptionCreatedAt: row.lastSubscriptionCreatedAt ?? undefined,
            lastAlertSentAt: typeof row.lastAlertSentAtMs === "number" ? toIsoUtc(row.lastAlertSentAtMs) : undefined
        };
    }
    listAdminProfileStats() {
        return this.db
            .prepare(`
          SELECT
            proxy_address AS proxyAddress,
            COUNT(*) AS subscriptionsCount
          FROM subscriptions
          GROUP BY proxy_address
          ORDER BY subscriptionsCount DESC, proxyAddress ASC
        `)
            .all();
    }
    insertAlertDelaySamples(samples) {
        if (samples.length === 0) {
            return;
        }
        const insert = this.db.prepare(`
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
      `);
        const tx = this.db.transaction((rows) => {
            for (const row of rows) {
                insert.run(row.telegramChatId, row.proxyAddress.toLowerCase(), row.activityTimestamp, row.pollStartedAtMs ?? null, row.fetchCompletedAtMs ?? null, row.sendStartedAtMs, row.sendCompletedAtMs ?? null, row.delayMs, row.delayReasonCode ?? null, toIsoUtc(row.sendStartedAtMs));
            }
        });
        tx(samples);
    }
    getAdminPerformanceReport(nowMs, thresholdMs = 30 * SECOND_MS) {
        const bucketCounts = new Map(DELAY_BUCKET_DEFINITIONS.map((bucket) => [bucket.label, 0]));
        const reasonCounts = new Map(DELAY_REASON_ORDER.map((reason) => [reason, 0]));
        const rows = this.db
            .prepare(`
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
        `)
            .all(nowMs - ALERT_DELAY_RETENTION_MS);
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
        let minDelayMs;
        let maxDelayMs;
        let totalDelayMs = 0;
        let classifiedSampleCount = 0;
        let alertsOverThresholdCount = 0;
        let upstreamStaleCount = 0;
        let freshLocalMaxDelayMs;
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
                    freshLocalMaxDelayMs = typeof freshLocalMaxDelayMs === "number"
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
            delayBuckets: DELAY_BUCKET_DEFINITIONS.map((bucket) => ({
                label: bucket.label,
                count: bucketCounts.get(bucket.label) ?? 0
            })),
            delayReasonCounts: DELAY_REASON_ORDER.map((reason) => ({
                reason,
                count: reasonCounts.get(reason) ?? 0
            }))
        };
    }
    cleanupOldAlertDelaySamples(nowMs) {
        const result = this.db
            .prepare(`
          DELETE FROM alert_delay_samples
          WHERE created_at < ?
        `)
            .run(toIsoUtc(nowMs - ALERT_DELAY_RETENTION_MS));
        return result.changes;
    }
    cleanupAlertDelaySamplesOverflow(maxRows = ALERT_DELAY_MAX_ROWS) {
        const result = this.db
            .prepare(`
          DELETE FROM alert_delay_samples
          WHERE id IN (
            SELECT id
            FROM alert_delay_samples
            ORDER BY send_started_at_ms DESC, id DESC
            LIMIT -1 OFFSET ?
          )
        `)
            .run(maxRows);
        return result.changes;
    }
    cleanupOldPollRuns(nowMs, retentionHours) {
        const retentionMs = retentionHours * 60 * 60 * 1000;
        const result = this.db
            .prepare(`
          DELETE FROM poll_runs
          WHERE created_at < ?
        `)
            .run(toIsoUtc(nowMs - retentionMs));
        return result.changes;
    }
    removeSubscription(chatId, proxyAddress) {
        const tx = this.db.transaction((safeChatId, safeProxyAddress) => {
            const removedSubscriptions = this.db
                .prepare(`
            DELETE FROM subscriptions
            WHERE telegram_chat_id = ? AND proxy_address = ?
          `)
                .run(safeChatId, safeProxyAddress).changes;
            const removedTrackedAccounts = removedSubscriptions > 0 ? this.cleanupTrackedAccountIfOrphaned(safeProxyAddress) : 0;
            return { removedSubscriptions, removedTrackedAccounts };
        });
        return tx(chatId, proxyAddress.toLowerCase());
    }
    removeAllSubscriptions(chatId) {
        const tx = this.db.transaction((safeChatId) => {
            const proxyRows = this.db
                .prepare(`
            SELECT DISTINCT proxy_address
            FROM subscriptions
            WHERE telegram_chat_id = ?
          `)
                .all(safeChatId);
            const removedSubscriptions = this.db
                .prepare(`
            DELETE FROM subscriptions
            WHERE telegram_chat_id = ?
          `)
                .run(safeChatId).changes;
            let removedTrackedAccounts = 0;
            for (const row of proxyRows) {
                removedTrackedAccounts += this.cleanupTrackedAccountIfOrphaned(row.proxy_address);
            }
            return { removedSubscriptions, removedTrackedAccounts };
        });
        return tx(chatId);
    }
    cleanupTrackedAccountIfOrphaned(proxyAddress) {
        const result = this.db
            .prepare(`
          DELETE FROM tracked_accounts
          WHERE proxy_address = ?
            AND NOT EXISTS (
              SELECT 1
              FROM subscriptions
              WHERE subscriptions.proxy_address = tracked_accounts.proxy_address
            )
        `)
            .run(proxyAddress.toLowerCase());
        return result.changes;
    }
    cleanupOrphanTrackedAccounts() {
        const result = this.db
            .prepare(`
          DELETE FROM tracked_accounts
          WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions
            WHERE subscriptions.proxy_address = tracked_accounts.proxy_address
          )
        `)
            .run();
        return result.changes;
    }
    countTrackedAccounts() {
        const row = this.db.prepare(`SELECT COUNT(*) AS count FROM tracked_accounts`).get();
        return row.count;
    }
    countInflightTrackedAccounts() {
        const row = this.db.prepare(`SELECT COUNT(*) AS count FROM tracked_accounts WHERE inflight = 1`).get();
        return row.count;
    }
    countDueTrackedAccounts(nowMs) {
        const row = this.db
            .prepare(`
          SELECT COUNT(*) AS count
          FROM tracked_accounts
          WHERE inflight = 0 AND next_poll_at_ms <= ?
        `)
            .get(nowMs);
        return row.count;
    }
    listDueTrackedAccounts(nowMs, limit) {
        const rows = this.db
            .prepare(`
          SELECT
            proxy_address,
            name,
            pseudonym,
            profile_slug,
            last_seen_timestamp,
            last_seen_keys_json,
            last_polled_at_ms,
            next_poll_at_ms,
            consecutive_errors,
            inflight,
            inflight_started_at_ms
          FROM tracked_accounts
          WHERE inflight = 0 AND next_poll_at_ms <= ?
          ORDER BY next_poll_at_ms ASC, proxy_address ASC
          LIMIT ?
        `)
            .all(nowMs, limit);
        return rows.map((row) => mapTrackedAccount(row));
    }
    tryClaimTrackedAccount(proxyAddress, nowMs) {
        const result = this.db
            .prepare(`
          UPDATE tracked_accounts
          SET
            inflight = 1,
            inflight_started_at_ms = ?,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          WHERE proxy_address = ?
            AND inflight = 0
            AND next_poll_at_ms <= ?
        `)
            .run(nowMs, proxyAddress.toLowerCase(), nowMs);
        return result.changes > 0;
    }
    markPollSuccess(payload) {
        this.db
            .prepare(`
          UPDATE tracked_accounts
          SET
            last_seen_timestamp = ?,
            last_seen_keys_json = ?,
            last_polled_at_ms = ?,
            next_poll_at_ms = ?,
            consecutive_errors = 0,
            inflight = 0,
            inflight_started_at_ms = NULL,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          WHERE proxy_address = ?
        `)
            .run(payload.lastSeenTimestamp, JSON.stringify(payload.lastSeenKeys), payload.lastPolledAtMs, payload.nextPollAtMs, payload.proxyAddress.toLowerCase());
    }
    markPollFailure(proxyAddress, nextPollAtMs, consecutiveErrors) {
        this.db
            .prepare(`
          UPDATE tracked_accounts
          SET
            next_poll_at_ms = ?,
            consecutive_errors = ?,
            inflight = 0,
            inflight_started_at_ms = NULL,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          WHERE proxy_address = ?
        `)
            .run(nextPollAtMs, consecutiveErrors, proxyAddress.toLowerCase());
    }
    clearStaleInflight(nowMs, staleAfterMs) {
        const threshold = nowMs - staleAfterMs;
        const result = this.db
            .prepare(`
          UPDATE tracked_accounts
          SET
            inflight = 0,
            inflight_started_at_ms = NULL,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          WHERE inflight = 1
            AND inflight_started_at_ms IS NOT NULL
            AND inflight_started_at_ms <= ?
        `)
            .run(threshold);
        return result.changes;
    }
    rebalanceTrackedAccountIntervals(nowMs, intervalMs) {
        const result = this.db
            .prepare(`
          UPDATE tracked_accounts
          SET
            next_poll_at_ms = CASE
              WHEN last_polled_at_ms IS NULL THEN MIN(next_poll_at_ms, ?)
              ELSE MAX(next_poll_at_ms, last_polled_at_ms + ?)
            END,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
          WHERE inflight = 0
        `)
            .run(nowMs, intervalMs);
        return result.changes;
    }
    checkpointWalTruncate() {
        this.db.pragma("wal_checkpoint(TRUNCATE)");
    }
    insertPollRun(row) {
        this.db
            .prepare(`
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
        `)
            .run(row.kind, row.proxyAddress ?? null, row.status, row.durationMs, row.apiCalls, row.retries, row.fetchedRows, row.newRows, row.alertsSent, row.queueDepth, row.trackedAccounts, row.inflightAccounts, row.effectiveIntervalMs, row.throttleWaitMs, row.note ?? null);
    }
    getFreshnessStats() {
        const row = this.db
            .prepare(`
          SELECT
            COUNT(*) AS trackedAccounts,
            SUM(CASE WHEN last_seen_timestamp > 0 THEN 1 ELSE 0 END) AS initializedAccounts,
            MAX(last_seen_timestamp) AS newestSeenTimestamp
          FROM tracked_accounts
        `)
            .get();
        return {
            trackedAccounts: row.trackedAccounts,
            initializedAccounts: row.initializedAccounts ?? 0,
            newestSeenTimestamp: row.newestSeenTimestamp ?? 0
        };
    }
}
