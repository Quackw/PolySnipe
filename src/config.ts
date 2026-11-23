import path from "node:path";
import { config as dotenvConfig } from "dotenv";
import type { AppConfig } from "./types.js";

dotenvConfig();

const BOOL_TRUE_VALUES = new Set(["1", "true", "yes", "y", "on"]);

function required(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) {
    throw new Error(`Missing required env var: ${name}`);
  }
  return value;
}

function parsePositiveInt(name: string, fallback: string): number {
  const value = Number.parseInt(process.env[name] ?? fallback, 10);
  if (!Number.isFinite(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}

function parseNonNegativeInt(name: string, fallback: string): number {
  const value = Number.parseInt(process.env[name] ?? fallback, 10);
  if (!Number.isFinite(value) || value < 0) {
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
  const baseTimeoutMs = process.env.HTTP_TIMEOUT_MS ?? "15000";
  const baseRetries = process.env.HTTP_RETRIES ?? "2";

  return {
    telegramBotToken: required("TELEGRAM_BOT_TOKEN"),
    adminTelegramUserIds: parseOptionalPositiveIntList("ADMIN_TELEGRAM_USER_IDS"),
    sqlitePath: path.resolve(required("SQLITE_PATH")),
    schedulerTickMs: parsePositiveInt("SCHEDULER_TICK_MS", "250"),
    pollTargetIntervalSec: parsePositiveInt("POLL_TARGET_INTERVAL_SEC", "5"),
    activitySafeRps: parsePositiveInt("ACTIVITY_SAFE_RPS", "20"),
    activityConcurrency: parsePositiveInt("ACTIVITY_CONCURRENCY", "8"),
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
