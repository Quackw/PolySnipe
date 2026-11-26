import path from "node:path";
import { config as dotenvConfig } from "dotenv";
import type { AppConfig } from "./types.js";

dotenvConfig();

const BOOL_TRUE_VALUES = new Set(["1", "true", "yes", "y", "on"]);
const DEFAULT_HTTP_TIMEOUT_MS = 15_000;
const DEFAULT_HTTP_RETRIES = 2;
const DEFAULT_POLYMARKET_RETRIES = 1;

function envValue(name: string): string | undefined {
  return process.env[name]?.trim() || undefined;
}

function required(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}

function parsePositiveInt(name: string, fallback: number): number {
  const raw = envValue(name);
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!/^\d+$/.test(raw) || !Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

function parseNonNegativeInt(name: string, fallback: number): number {
  const raw = envValue(name);
  if (raw === undefined) return fallback;
  const value = Number(raw);
  if (!/^\d+$/.test(raw) || !Number.isSafeInteger(value)) {
    throw new Error(`${name} must be a non-negative integer`);
  }
  return value;
}

function parseOptionalPositiveIntList(name: string): number[] {
  const raw = process.env[name]?.trim();
  if (!raw) {
    return [];
  }

  const values = raw.split(",").map((value) => value.trim());
  if (values.some((value) => value.length === 0 || !/^\d+$/.test(value))) {
    throw new Error(`${name} must be a comma-separated list of positive integers`);
  }

  const uniqueValues = new Set<number>();
  for (const value of values) {
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed) || parsed <= 0) {
      throw new Error(`${name} must be a comma-separated list of positive integers`);
    }
    uniqueValues.add(parsed);
  }

  return [...uniqueValues];
}

function parseBool(name: string, fallback = "0"): boolean {
  const value = (process.env[name] ?? fallback).trim().toLowerCase();
  return BOOL_TRUE_VALUES.has(value);
}

export function loadConfig(): AppConfig {
  const telegramBotToken = required("TELEGRAM_BOT_TOKEN");
  const sqlitePath = required("SQLITE_PATH");
  const baseTimeoutMs = parsePositiveInt("HTTP_TIMEOUT_MS", DEFAULT_HTTP_TIMEOUT_MS);
  const baseRetries =
    envValue("HTTP_RETRIES") === undefined
      ? undefined
      : parseNonNegativeInt("HTTP_RETRIES", DEFAULT_HTTP_RETRIES);

  return {
    telegramBotToken,
    adminTelegramUserIds: parseOptionalPositiveIntList("ADMIN_TELEGRAM_USER_IDS"),
    sqlitePath: sqlitePath === ":memory:" ? sqlitePath : path.resolve(sqlitePath),
    schedulerTickMs: parsePositiveInt("SCHEDULER_TICK_MS", 250),
    pollTargetIntervalSec: parsePositiveInt("POLL_TARGET_INTERVAL_SEC", 5),
    activitySafeRps: parsePositiveInt("ACTIVITY_SAFE_RPS", 20),
    activityConcurrency: parsePositiveInt("ACTIVITY_CONCURRENCY", 8),
    activityPageLimit: parsePositiveInt("ACTIVITY_PAGE_LIMIT", 500),
    activityMaxPages: parsePositiveInt("ACTIVITY_MAX_PAGES", 3),
    activityOverlapSec: parsePositiveInt("ACTIVITY_OVERLAP_SEC", 2),
    rebalanceEveryMs: parsePositiveInt("REBALANCE_EVERY_MS", 5000),
    orphanCleanupEveryMs: parsePositiveInt("ORPHAN_CLEANUP_EVERY_MS", 60000),
    subscribeModeTtlMs: parsePositiveInt("SUBSCRIBE_MODE_TTL_MS", 600000),
    staleInflightAfterMs: parsePositiveInt("STALE_INFLIGHT_AFTER_MS", 60000),
    staleInflightRecoveryEveryMs: parsePositiveInt("STALE_INFLIGHT_RECOVERY_EVERY_MS", 30000),
    walCheckpointEveryMs: parsePositiveInt("WAL_CHECKPOINT_EVERY_MS", 600000),
    heartbeatEveryMs: parsePositiveInt("HEARTBEAT_EVERY_MS", 60000),
    alertDelayThresholdSec: parsePositiveInt("ALERT_DELAY_THRESHOLD_SEC", 30),
    alertSendConcurrency: parsePositiveInt("ALERT_SEND_CONCURRENCY", 1),
    pollRunsRetentionHours: parsePositiveInt("POLL_RUNS_RETENTION_HOURS", 72),
    polymarketTimeoutMs: parsePositiveInt("POLYMARKET_TIMEOUT_MS", baseTimeoutMs),
    polymarketRetries: parseNonNegativeInt("POLYMARKET_RETRIES", baseRetries ?? DEFAULT_POLYMARKET_RETRIES),
    telegramTimeoutMs: parsePositiveInt("TELEGRAM_TIMEOUT_MS", baseTimeoutMs),
    httpTimeoutMs: baseTimeoutMs,
    httpRetries: baseRetries ?? DEFAULT_HTTP_RETRIES,
    debug: parseBool("DEBUG")
  };
}
