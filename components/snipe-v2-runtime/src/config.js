import path from "node:path";
import { config as dotenvConfig } from "dotenv";
dotenvConfig();
const BOOL_TRUE_VALUES = new Set(["1", "true", "yes", "y", "on"]);
function required(name) {
    const value = process.env[name]?.trim();
    if (!value) {
        throw new Error(`Missing required env var: ${name}`);
    }
    return value;
}
function parsePositiveInt(name, fallback) {
    const value = Number.parseInt(process.env[name] ?? fallback, 10);
    if (!Number.isFinite(value) || value <= 0) {
        throw new Error(`${name} must be a positive integer`);
    }
    return value;
}
function parseNonNegativeInt(name, fallback) {
    const value = Number.parseInt(process.env[name] ?? fallback, 10);
    if (!Number.isFinite(value) || value < 0) {
        throw new Error(`${name} must be a non-negative integer`);
    }
    return value;
}
function parseBool(name, fallback = "0") {
    const value = (process.env[name] ?? fallback).trim().toLowerCase();
    return BOOL_TRUE_VALUES.has(value);
}
export function loadConfig() {
    const baseTimeoutMs = process.env.HTTP_TIMEOUT_MS ?? "15000";
    const baseRetries = process.env.HTTP_RETRIES ?? "2";
    const ownerRaw = process.env.OWNER_TELEGRAM_USER_IDS?.trim() || required("OWNER_TELEGRAM_USER_ID");
    const ownerParts = ownerRaw.split(",").map((part) => part.trim());
    if (ownerParts.some((part) => !/^\d+$/.test(part))) {
        throw new Error("OWNER_TELEGRAM_USER_IDS must contain positive safe integers");
    }
    const ownerTelegramUserIds = ownerParts.map(Number);
    if (ownerTelegramUserIds.length < 1 || ownerTelegramUserIds.length > 2
        || ownerTelegramUserIds.some((id) => !Number.isSafeInteger(id) || id <= 0)
        || new Set(ownerTelegramUserIds).size !== ownerTelegramUserIds.length) {
        throw new Error("OWNER_TELEGRAM_USER_IDS must contain one or two distinct positive safe integers");
    }
    const sqlitePath = path.resolve(required("SQLITE_PATH"));
    if (path.basename(sqlitePath) === "poly-snipe.db") {
        throw new Error("V2 must use its own SQLite file, not the V1 database");
    }
    const expectedTelegramUsername = required("EXPECTED_TELEGRAM_USERNAME").replace(/^@/, "").toLowerCase();
    if (!/^[a-z][a-z0-9_]{4,31}$/.test(expectedTelegramUsername)) {
        throw new Error("EXPECTED_TELEGRAM_USERNAME must be a valid bot username");
    }
    return {
        telegramBotToken: required("TELEGRAM_BOT_TOKEN"),
        expectedTelegramUsername,
        ownerTelegramUserIds,
        allowedTelegramUserIds: [...ownerTelegramUserIds],
        adminTelegramUserIds: [...ownerTelegramUserIds],
        sqlitePath,
        schedulerTickMs: parsePositiveInt("SCHEDULER_TICK_MS", "250"),
        pollTargetIntervalSec: parsePositiveInt("POLL_TARGET_INTERVAL_SEC", "30"),
        activitySafeRps: parsePositiveInt("ACTIVITY_SAFE_RPS", "2"),
        activityConcurrency: parsePositiveInt("ACTIVITY_CONCURRENCY", "1"),
        activityPageLimit: parsePositiveInt("ACTIVITY_PAGE_LIMIT", "500"),
        activityMaxPages: parsePositiveInt("ACTIVITY_MAX_PAGES", "3"),
        activityOverlapSec: parsePositiveInt("ACTIVITY_OVERLAP_SEC", "2"),
        rebalanceEveryMs: parsePositiveInt("REBALANCE_EVERY_MS", "5000"),
        orphanCleanupEveryMs: parsePositiveInt("ORPHAN_CLEANUP_EVERY_MS", "60000"),
        subscribeModeTtlMs: parsePositiveInt("SUBSCRIBE_MODE_TTL_MS", "600000"),
        staleInflightAfterMs: parsePositiveInt("STALE_INFLIGHT_AFTER_MS", "60000"),
        staleInflightRecoveryEveryMs: parsePositiveInt("STALE_INFLIGHT_RECOVERY_EVERY_MS", "30000"),
        walCheckpointEveryMs: parsePositiveInt("WAL_CHECKPOINT_EVERY_MS", "600000"),
        heartbeatEveryMs: parsePositiveInt("HEARTBEAT_EVERY_MS", "60000"),
        alertDelayThresholdSec: parsePositiveInt("ALERT_DELAY_THRESHOLD_SEC", "30"),
        alertSendConcurrency: parsePositiveInt("ALERT_SEND_CONCURRENCY", "1"),
        pollRunsRetentionHours: parsePositiveInt("POLL_RUNS_RETENTION_HOURS", "72"),
        polymarketTimeoutMs: parsePositiveInt("POLYMARKET_TIMEOUT_MS", baseTimeoutMs),
        polymarketRetries: parseNonNegativeInt("POLYMARKET_RETRIES", process.env.HTTP_RETRIES ?? "1"),
        telegramTimeoutMs: parsePositiveInt("TELEGRAM_TIMEOUT_MS", baseTimeoutMs),
        httpTimeoutMs: parsePositiveInt("HTTP_TIMEOUT_MS", baseTimeoutMs),
        httpRetries: parseNonNegativeInt("HTTP_RETRIES", baseRetries),
        debug: parseBool("DEBUG")
    };
}
