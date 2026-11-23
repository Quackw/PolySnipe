import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { openDb } from "../src/db/client.js";
import { AppRepository } from "../src/db/repository.js";
import { createTempRepo } from "./helpers.js";

const resources: Array<ReturnType<typeof createTempRepo>> = [];

function setupRepo() {
  const temp = createTempRepo("repo");
  resources.push(temp);
  return temp;
}

afterEach(() => {
  while (resources.length > 0) {
    resources.pop()?.cleanup();
  }
});

describe("repository tracked account cleanup", () => {
  it("removes tracked account after last unsubscribe", () => {
    const { repo } = setupRepo();
    const subscriberId = repo.upsertSubscriber(1, 100, "private");
    repo.upsertTrackedAccount({
      proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
      profileSlug: "weflyhigh",
      nextPollAtMs: 0
    });
    repo.createSubscription(subscriberId, 100, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 1, 1000);

    const removed = repo.removeSubscription(100, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697");

    expect(removed).toEqual({ removedSubscriptions: 1, removedTrackedAccounts: 1 });
    expect(repo.getTrackedAccount("0x03e8a544e97eeff5753bc1e90d46e5ef22af1697")).toBeNull();
  });

  it("keeps tracked account while another chat is still subscribed", () => {
    const { repo } = setupRepo();
    const subscriberA = repo.upsertSubscriber(1, 100, "private");
    const subscriberB = repo.upsertSubscriber(2, 200, "private");
    repo.upsertTrackedAccount({
      proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
      nextPollAtMs: 0
    });
    repo.createSubscription(subscriberA, 100, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 1, 1000);
    repo.createSubscription(subscriberB, 200, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 2, 1000);

    const removed = repo.removeSubscription(100, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697");

    expect(removed).toEqual({ removedSubscriptions: 1, removedTrackedAccounts: 0 });
    expect(repo.getTrackedAccount("0x03e8a544e97eeff5753bc1e90d46e5ef22af1697")).not.toBeNull();
  });

  it("removes orphaned tracked accounts on removeAllSubscriptions", () => {
    const { repo } = setupRepo();
    const subscriberId = repo.upsertSubscriber(1, 100, "private");
    repo.upsertTrackedAccount({
      proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
      nextPollAtMs: 0
    });
    repo.upsertTrackedAccount({
      proxyAddress: "0x1111111111111111111111111111111111111111",
      nextPollAtMs: 0
    });
    repo.createSubscription(subscriberId, 100, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 1, 1000);
    repo.createSubscription(subscriberId, 100, "0x1111111111111111111111111111111111111111", 1, 1000);

    const removed = repo.removeAllSubscriptions(100);

    expect(removed).toEqual({ removedSubscriptions: 2, removedTrackedAccounts: 2 });
    expect(repo.countTrackedAccounts()).toBe(0);
  });
});

describe("repository admin stats", () => {
  it("aggregates active subscribers and excludes subscribers with zero subscriptions", () => {
    const { repo } = setupRepo();
    const subscriberA = repo.upsertSubscriber(1, 100, "private");
    const subscriberB = repo.upsertSubscriber(2, 200, "private");
    repo.upsertSubscriber(3, 300, "private");

    repo.upsertTrackedAccount({
      proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
      nextPollAtMs: 0
    });
    repo.upsertTrackedAccount({
      proxyAddress: "0x1111111111111111111111111111111111111111",
      nextPollAtMs: 0
    });
    repo.upsertTrackedAccount({
      proxyAddress: "0x2222222222222222222222222222222222222222",
      nextPollAtMs: 0
    });

    repo.createSubscription(subscriberA, 100, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 1, 1000);
    repo.createSubscription(subscriberA, 100, "0x1111111111111111111111111111111111111111", 1, 1000);
    repo.createSubscription(subscriberB, 200, "0x2222222222222222222222222222222222222222", 2, 1000);

    const stats = repo.getAdminSubscriberStats();

    expect(stats.totalSubscribers).toBe(2);
    expect(stats.totalSubscriptions).toBe(3);
    expect(stats.rows).toEqual([
      { telegramUserId: 1, telegramChatId: 100, subscriptionsCount: 2 },
      { telegramUserId: 2, telegramChatId: 200, subscriptionsCount: 1 }
    ]);
  });
});

describe("repository admin reports", () => {
  it("upgrades legacy alert delay tables without destructive migration", () => {
    const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "repo-upgrade-"));
    const dbPath = path.join(tmpDir, "state.db");
    const db = openDb(dbPath);
    db.exec(`
      CREATE TABLE alert_delay_samples (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        telegram_chat_id INTEGER NOT NULL,
        proxy_address TEXT NOT NULL,
        activity_timestamp INTEGER NOT NULL,
        send_started_at_ms INTEGER NOT NULL,
        delay_ms INTEGER NOT NULL,
        created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
      );
    `);
    db.close();

    const repo = new AppRepository(openDb(dbPath));
    const rawDb = (repo as unknown as { db: { prepare: (sql: string) => { all: () => Array<{ name: string }> } } }).db;
    const columns = rawDb.prepare(`PRAGMA table_info(alert_delay_samples)`).all().map((row) => row.name);

    expect(columns).toEqual(expect.arrayContaining([
      "poll_started_at_ms",
      "fetch_completed_at_ms",
      "send_completed_at_ms",
      "delay_reason_code"
    ]));

    repo.close();
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("aggregates users report with distinct users and last joined timestamp", () => {
    const { repo } = setupRepo();
    repo.upsertSubscriber(1, 100, "private");
    repo.upsertSubscriber(1, 200, "private");
    repo.upsertSubscriber(2, 300, "private");
    repo.upsertSubscriber(3, 400, "private");
    const trackedSubscriber = repo.upsertSubscriber(4, 500, "private");
    repo.upsertTrackedAccount({ proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", nextPollAtMs: 0 });
    repo.createSubscription(trackedSubscriber, 500, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 4, 1000);

    const db = (repo as unknown as { db: { prepare: (sql: string) => { run: (...args: unknown[]) => unknown } } }).db;
    db.prepare(`UPDATE subscribers SET created_at = ? WHERE telegram_chat_id = ?`).run("2026-03-26T10:00:00.000Z", 100);
    db.prepare(`UPDATE subscribers SET created_at = ? WHERE telegram_chat_id = ?`).run("2026-03-26T11:00:00.000Z", 200);
    db.prepare(`UPDATE subscribers SET created_at = ? WHERE telegram_chat_id = ?`).run("2026-03-26T12:00:00.000Z", 300);
    db.prepare(`UPDATE subscribers SET created_at = ? WHERE telegram_chat_id = ?`).run("2026-03-26T13:00:00.000Z", 400);
    db.prepare(`UPDATE subscribers SET created_at = ? WHERE telegram_chat_id = ?`).run("2026-03-26T14:00:00.000Z", 500);

    expect(repo.getAdminUsersReport()).toEqual({
      totalUsers: 1,
      lastUserJoinedAt: "2026-03-26T14:00:00.000Z"
    });
  });

  it("aggregates subscriptions report with unique-user average and last created timestamp", () => {
    const { repo } = setupRepo();
    const nowMs = Date.UTC(2026, 2, 27, 12, 0, 0);
    const subscriberA = repo.upsertSubscriber(1, 100, "private");
    const subscriberB = repo.upsertSubscriber(2, 200, "private");
    repo.upsertSubscriber(3, 300, "private");
    repo.upsertTrackedAccount({ proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", nextPollAtMs: 0 });
    repo.upsertTrackedAccount({ proxyAddress: "0x1111111111111111111111111111111111111111", nextPollAtMs: 0 });
    repo.upsertTrackedAccount({ proxyAddress: "0x2222222222222222222222222222222222222222", nextPollAtMs: 0 });
    repo.createSubscription(subscriberA, 100, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 1, 1000);
    repo.createSubscription(subscriberA, 100, "0x1111111111111111111111111111111111111111", 1, 1000);
    repo.createSubscription(subscriberB, 200, "0x2222222222222222222222222222222222222222", 2, 1000);

    const db = (repo as unknown as { db: { prepare: (sql: string) => { run: (...args: unknown[]) => unknown } } }).db;
    db.prepare(`UPDATE subscriptions SET created_at = ? WHERE telegram_chat_id = ? AND proxy_address = ?`).run(
      "2026-03-26T10:00:00.000Z",
      100,
      "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697"
    );
    db.prepare(`UPDATE subscriptions SET created_at = ? WHERE telegram_chat_id = ? AND proxy_address = ?`).run(
      "2026-03-26T11:00:00.000Z",
      100,
      "0x1111111111111111111111111111111111111111"
    );
    db.prepare(`UPDATE subscriptions SET created_at = ? WHERE telegram_chat_id = ? AND proxy_address = ?`).run(
      "2026-03-26T12:00:00.000Z",
      200,
      "0x2222222222222222222222222222222222222222"
    );
    repo.insertAlertDelaySamples([
      {
        telegramChatId: 100,
        proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
        activityTimestamp: Math.floor((nowMs - 10_000) / 1000),
        sendStartedAtMs: nowMs - 5_000,
        delayMs: 5_000
      }
    ]);

    expect(repo.getAdminSubscriptionsReport()).toEqual({
      totalSubscriptions: 3,
      avgSubscriptionsPerUser: 1.5,
      lastSubscriptionCreatedAt: "2026-03-26T12:00:00.000Z",
      lastAlertSentAt: "2026-03-27T11:59:55.000Z"
    });
  });

  it("lists profile stats sorted by subscriptions count and proxy address", () => {
    const { repo } = setupRepo();
    const subscriberA = repo.upsertSubscriber(1, 100, "private");
    const subscriberB = repo.upsertSubscriber(2, 200, "private");
    repo.upsertTrackedAccount({ proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", nextPollAtMs: 0 });
    repo.upsertTrackedAccount({ proxyAddress: "0x1111111111111111111111111111111111111111", nextPollAtMs: 0 });
    repo.upsertTrackedAccount({ proxyAddress: "0x2222222222222222222222222222222222222222", nextPollAtMs: 0 });
    repo.createSubscription(subscriberA, 100, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 1, 1000);
    repo.createSubscription(subscriberB, 200, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 2, 1000);
    repo.createSubscription(subscriberA, 100, "0x1111111111111111111111111111111111111111", 1, 1000);
    repo.createSubscription(subscriberB, 200, "0x2222222222222222222222222222222222222222", 2, 1000);

    expect(repo.listAdminProfileStats()).toEqual([
      { proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", subscriptionsCount: 2 },
      { proxyAddress: "0x1111111111111111111111111111111111111111", subscriptionsCount: 1 },
      { proxyAddress: "0x2222222222222222222222222222222222222222", subscriptionsCount: 1 }
    ]);
  });

  it("aggregates performance stats over the last 24 hours and cleans up older samples", () => {
    const { repo } = setupRepo();
    const nowMs = Date.UTC(2026, 2, 27, 12, 0, 0);
    repo.insertAlertDelaySamples([
      {
        telegramChatId: 100,
        proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
        activityTimestamp: Math.floor((nowMs - 10_000) / 1000),
        sendStartedAtMs: nowMs - 5_000,
        delayMs: 5_000
      },
      {
        telegramChatId: 100,
        proxyAddress: "0x1111111111111111111111111111111111111111",
        activityTimestamp: Math.floor((nowMs - 20_000) / 1000),
        sendStartedAtMs: nowMs - 10_000,
        delayMs: 10_000
      },
      {
        telegramChatId: 100,
        proxyAddress: "0x2222222222222222222222222222222222222222",
        activityTimestamp: Math.floor((nowMs - 2 * 24 * 60 * 60 * 1000) / 1000),
        sendStartedAtMs: nowMs - 2 * 24 * 60 * 60 * 1000,
        delayMs: 20_000
      }
    ]);

    expect(repo.getAdminPerformanceReport(nowMs)).toEqual({
      thresholdMs: 30_000,
      sampleCount: 2,
      classifiedSampleCount: 0,
      minDelayMs: 5_000,
      maxDelayMs: 10_000,
      avgDelayMs: 7_500,
      freshLocalMaxDelayMs: undefined,
      alertsOverThresholdCount: 0,
      upstreamStaleCount: 0,
      delayBuckets: [
        { label: "<5s", count: 0 },
        { label: "5-10s", count: 1 },
        { label: "10-30s", count: 1 },
        { label: "30-60s", count: 0 },
        { label: "1-2m", count: 0 },
        { label: "2-5m", count: 0 },
        { label: "5-10m", count: 0 },
        { label: ">10m", count: 0 }
      ],
      delayReasonCounts: [
        { reason: "upstream_stale", count: 0 },
        { reason: "poll_fetch_slow", count: 0 },
        { reason: "dispatch_backlog", count: 0 },
        { reason: "telegram_slow", count: 0 }
      ]
    });

    expect(repo.cleanupOldAlertDelaySamples(nowMs)).toBe(1);
    expect(repo.getAdminPerformanceReport(nowMs)).toEqual({
      thresholdMs: 30_000,
      sampleCount: 2,
      classifiedSampleCount: 0,
      minDelayMs: 5_000,
      maxDelayMs: 10_000,
      avgDelayMs: 7_500,
      freshLocalMaxDelayMs: undefined,
      alertsOverThresholdCount: 0,
      upstreamStaleCount: 0,
      delayBuckets: [
        { label: "<5s", count: 0 },
        { label: "5-10s", count: 1 },
        { label: "10-30s", count: 1 },
        { label: "30-60s", count: 0 },
        { label: "1-2m", count: 0 },
        { label: "2-5m", count: 0 },
        { label: "5-10m", count: 0 },
        { label: ">10m", count: 0 }
      ],
      delayReasonCounts: [
        { reason: "upstream_stale", count: 0 },
        { reason: "poll_fetch_slow", count: 0 },
        { reason: "dispatch_backlog", count: 0 },
        { reason: "telegram_slow", count: 0 }
      ]
    });
  });

  it("aggregates classified delay reasons, bucket counts, and fresh local max", () => {
    const { repo } = setupRepo();
    const nowMs = Date.UTC(2026, 2, 27, 12, 0, 0);
    repo.insertAlertDelaySamples([
      {
        telegramChatId: 100,
        proxyAddress: "0xaaaa",
        activityTimestamp: Math.floor((nowMs - 8_000) / 1000),
        pollStartedAtMs: nowMs - 7_000,
        fetchCompletedAtMs: nowMs - 6_000,
        sendStartedAtMs: nowMs - 5_000,
        sendCompletedAtMs: nowMs - 4_000,
        delayMs: 5_000
      },
      {
        telegramChatId: 100,
        proxyAddress: "0xbbbb",
        activityTimestamp: Math.floor((nowMs - 40_000) / 1000),
        pollStartedAtMs: nowMs - 39_000,
        fetchCompletedAtMs: nowMs - 32_000,
        sendStartedAtMs: nowMs - 31_000,
        sendCompletedAtMs: nowMs - 30_000,
        delayMs: 31_000,
        delayReasonCode: "poll_fetch_slow"
      },
      {
        telegramChatId: 100,
        proxyAddress: "0xcccc",
        activityTimestamp: Math.floor((nowMs - 35_000) / 1000),
        pollStartedAtMs: nowMs - 34_000,
        fetchCompletedAtMs: nowMs - 32_000,
        sendStartedAtMs: nowMs - 29_000,
        sendCompletedAtMs: nowMs - 1_000,
        delayMs: 29_000,
        delayReasonCode: "telegram_slow"
      },
      {
        telegramChatId: 100,
        proxyAddress: "0xdddd",
        activityTimestamp: Math.floor((nowMs - 70_000) / 1000),
        pollStartedAtMs: nowMs - 35_000,
        fetchCompletedAtMs: nowMs - 34_000,
        sendStartedAtMs: nowMs - 33_000,
        sendCompletedAtMs: nowMs - 32_000,
        delayMs: 33_000,
        delayReasonCode: "upstream_stale"
      }
    ]);

    expect(repo.getAdminPerformanceReport(nowMs)).toEqual({
      thresholdMs: 30_000,
      sampleCount: 4,
      classifiedSampleCount: 3,
      minDelayMs: 5_000,
      maxDelayMs: 33_000,
      avgDelayMs: 24_500,
      freshLocalMaxDelayMs: 33_000,
      alertsOverThresholdCount: 2,
      upstreamStaleCount: 1,
      delayBuckets: [
        { label: "<5s", count: 0 },
        { label: "5-10s", count: 1 },
        { label: "10-30s", count: 1 },
        { label: "30-60s", count: 2 },
        { label: "1-2m", count: 0 },
        { label: "2-5m", count: 0 },
        { label: "5-10m", count: 0 },
        { label: ">10m", count: 0 }
      ],
      delayReasonCounts: [
        { reason: "upstream_stale", count: 1 },
        { reason: "poll_fetch_slow", count: 1 },
        { reason: "dispatch_backlog", count: 0 },
        { reason: "telegram_slow", count: 1 }
      ]
    });
  });

  it("cleans up old poll runs using the configured retention window", () => {
    const { repo } = setupRepo();
    const nowMs = Date.UTC(2026, 2, 27, 12, 0, 0);
    repo.insertPollRun({
      kind: "heartbeat",
      status: "ok",
      durationMs: 0,
      apiCalls: 0,
      retries: 0,
      fetchedRows: 0,
      newRows: 0,
      alertsSent: 0,
      queueDepth: 0,
      trackedAccounts: 0,
      inflightAccounts: 0,
      effectiveIntervalMs: 5_000,
      throttleWaitMs: 0
    });

    const db = (repo as unknown as { db: { prepare: (sql: string) => { run: (...args: unknown[]) => unknown; get: (...args: unknown[]) => { count: number } } } }).db;
    db.prepare(`UPDATE poll_runs SET created_at = ?`).run("2026-03-20T12:00:00.000Z");
    repo.insertPollRun({
      kind: "heartbeat",
      status: "ok",
      durationMs: 0,
      apiCalls: 0,
      retries: 0,
      fetchedRows: 0,
      newRows: 0,
      alertsSent: 0,
      queueDepth: 0,
      trackedAccounts: 0,
      inflightAccounts: 0,
      effectiveIntervalMs: 5_000,
      throttleWaitMs: 0
    });

    expect(repo.cleanupOldPollRuns(nowMs, 72)).toBe(1);
    expect(db.prepare(`SELECT COUNT(*) AS count FROM poll_runs`).get().count).toBe(1);
  });

  it("keeps only the latest alert delay samples when the table exceeds the max row cap", () => {
    const { repo } = setupRepo();
    const baseMs = Date.UTC(2026, 2, 27, 12, 0, 0);

    const rows = Array.from({ length: 25_005 }, (_, index) => ({
      telegramChatId: 100,
      proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
      activityTimestamp: Math.floor((baseMs + index * 1_000 - 2_000) / 1000),
      sendStartedAtMs: baseMs + index * 1_000,
      delayMs: 2_000
    }));

    repo.insertAlertDelaySamples(rows);
    expect(repo.cleanupAlertDelaySamplesOverflow()).toBe(5);

    const db = (repo as unknown as { db: { prepare: (sql: string) => { get: (...args: unknown[]) => { count: number; oldestSendStartedAtMs: number } } } }).db;
    const row = db
      .prepare(
        `
          SELECT
            COUNT(*) AS count,
            MIN(send_started_at_ms) AS oldestSendStartedAtMs
          FROM alert_delay_samples
        `
      )
      .get();

    expect(row.count).toBe(25_000);
    expect(row.oldestSendStartedAtMs).toBe(baseMs + 5_000);
  });
});
