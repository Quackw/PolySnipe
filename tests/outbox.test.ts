import { afterEach, describe, it, expect, vi } from "vitest";
import { createTempRepo, testConfig } from "./helpers.js";
import { openDb } from "../src/db/client.js";
import { AppRepository, MAX_ALERT_ATTEMPTS } from "../src/db/repository.js";
import { PollScheduler } from "../src/scheduler.js";
import { createTelegramSender, TelegramRejectedError } from "../src/apis/telegram.js";
import type { PolymarketClient } from "../src/apis/polymarket.js";
const resources: ReturnType<typeof createTempRepo>[] = [];
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  for (const r of resources.splice(0)) r.cleanup();
});
function setup() {
  const t = createTempRepo("outbox");
  resources.push(t);
  const { repo } = t;
  repo.upsertTrackedAccount({ proxyAddress: "wallet", nextPollAtMs: 0 });
  const id = repo.upsertSubscriber(1, 10, "private");
  repo.createSubscription(id, 10, "wallet", 1, 0);
  return t;
}
const alert = {
  chatId: 10,
  text: "hello",
  contextJson: JSON.stringify({ activities: [], timing: { pollStartedAtMs: 1, fetchCompletedAtMs: 2 } })
};
function admit(repo: AppRepository) {
  repo.markPollSuccess({
    proxyAddress: "wallet",
    lastSeenTimestamp: 123,
    lastSeenKeys: ["key"],
    lastPolledAtMs: 1,
    nextPollAtMs: 9999999999999,
    alerts: [alert]
  });
}
const brokenClient = {
  fetchRecentUserActivity: async () => {
    throw Error("API unavailable");
  }
} as unknown as PolymarketClient;
describe("durable admission and restart", () => {
  it("rolls back the watermark and all messages if an insertion fails", () => {
    const { repo, dbPath } = setup();
    const db = openDb(dbPath);
    db.exec(
      "CREATE TRIGGER fail_outbox BEFORE INSERT ON alert_outbox BEGIN SELECT RAISE(ABORT,'disk simulation'); END"
    );
    expect(() => admit(repo)).toThrow("disk simulation");
    expect(repo.getTrackedAccount("wallet")?.lastSeenTimestamp).toBe(0);
    expect(repo.outboxCounts()).toEqual({});
    db.close();
  });
  it("preserves queued text and checkpoint after closing and reopening SQLite", () => {
    const { repo, dbPath } = setup();
    admit(repo);
    repo.close();
    const reopened = new AppRepository(openDb(dbPath));
    expect(reopened.getTrackedAccount("wallet")?.lastSeenTimestamp).toBe(123);
    expect(reopened.listPendingAlerts(Date.now())[0]?.text).toBe("hello");
    reopened.close();
  });
  it("allows only one claim and never replays an interrupted or acknowledged send", () => {
    const { repo } = setup();
    admit(repo);
    const row = repo.listPendingAlerts(Date.now())[0]!;
    expect(repo.claimAlert(row.id)).toBe(true);
    expect(repo.claimAlert(row.id)).toBe(false);
    expect(repo.recoverOutbox()).toBe(1);
    expect(repo.outboxCounts()).toEqual({ UNKNOWN: 1 });
    expect(repo.listPendingAlerts(Date.now())).toEqual([]);
    admit(repo);
    const second = repo.listPendingAlerts(Date.now())[0]!;
    repo.claimAlert(second.id);
    repo.finishAlert(second.id, "DELIVERED");
    expect(repo.recoverOutbox()).toBe(0);
    expect(repo.listPendingAlerts(Date.now())).toEqual([]);
  });
  it("cancels pending messages after unsubscribe even if the chat subscribes again", () => {
    const { repo } = setup();
    admit(repo);
    repo.removeSubscription(10, "wallet");
    repo.upsertTrackedAccount({ proxyAddress: "wallet", nextPollAtMs: 0 });
    const id = repo.upsertSubscriber(1, 10, "private");
    repo.createSubscription(id, 10, "wallet", 1, 0);
    expect(repo.listPendingAlerts(Date.now())).toEqual([]);
    expect(repo.outboxCounts()).toEqual({ CANCELLED: 1 });
  });
  it("drains persisted messages independently of failing Data API and does not replay on restart", async () => {
    vi.useFakeTimers();
    const { repo, dbPath } = setup();
    admit(repo);
    const send = vi.fn(async () => {});
    const scheduler = new PollScheduler(testConfig(dbPath), repo, brokenClient, {
      sendTelegram: send,
      log: () => {}
    });
    scheduler.start();
    await vi.advanceTimersByTimeAsync(20);
    await scheduler.stopAndDrain();
    expect(send).toHaveBeenCalledTimes(1);
    expect(repo.outboxCounts()).toEqual({ DELIVERED: 1 });
    scheduler.start();
    await vi.advanceTimersByTimeAsync(20);
    await scheduler.stopAndDrain();
    expect(send).toHaveBeenCalledTimes(1);
  });
  it("keeps ambiguous sends for review and retries only explicit rate limit rejection", async () => {
    vi.useFakeTimers();
    const { repo, dbPath } = setup();
    admit(repo);
    const send = vi
      .fn()
      .mockRejectedValueOnce(new TelegramRejectedError(429, 1000))
      .mockRejectedValueOnce(new Error("timeout"));
    const scheduler = new PollScheduler(testConfig(dbPath), repo, brokenClient, {
      sendTelegram: send,
      log: () => {}
    });
    scheduler.start();
    await vi.advanceTimersByTimeAsync(500);
    expect(send).toHaveBeenCalledTimes(1);
    expect(repo.outboxCounts()).toEqual({ PENDING: 1 });
    await vi.advanceTimersByTimeAsync(1500);
    await scheduler.stopAndDrain();
    expect(send).toHaveBeenCalledTimes(2);
    expect(repo.outboxCounts()).toEqual({ UNKNOWN: 1 });
  });
});
describe("Telegram acknowledgement", () => {
  const sender = createTelegramSender({ token: "test", timeoutMs: 100, log: () => {} });
  it.each([{ ok: true, result: { message_id: 1, chat: { id: 10 } } }])(
    "accepts a matching explicit Telegram acknowledgement",
    async (body) => {
      vi.stubGlobal(
        "fetch",
        vi.fn(async () => new Response(JSON.stringify(body)))
      );
      await expect(sender(10, "hello")).resolves.toBeUndefined();
    }
  );
  it.each([
    { ok: true },
    { ok: true, result: { message_id: 1, chat: { id: 11 } } },
    { ok: false, error_code: 500 }
  ])("does not mistake a malformed or wrong-chat acknowledgement for delivery", async (body) => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(JSON.stringify(body)))
    );
    await expect(sender(10, "hello")).rejects.toThrow();
  });
  it("classifies an explicit 429 as rejected and preserves retry_after", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(JSON.stringify({ ok: false, error_code: 429, parameters: { retry_after: 4 } }), {
            status: 429
          })
      )
    );
    await expect(sender(10, "hello")).rejects.toMatchObject({ code: 429, retryAfterMs: 4000 });
  });
});

it("persists chunks separately so restart sends only the chunk not yet attempted", async () => {
  vi.useFakeTimers();
  const { repo, dbPath } = setup();
  repo.markPollSuccess({
    proxyAddress: "wallet",
    lastSeenTimestamp: 123,
    lastSeenKeys: ["key"],
    lastPolledAtMs: 1,
    nextPollAtMs: 9999999999999,
    alerts: [alert, { ...alert, text: "second chunk" }]
  });
  const sent: string[] = [];
  const scheduler = new PollScheduler(testConfig(dbPath), repo, brokenClient, {
    sendTelegram: async (_chat, text) => {
      sent.push(text);
      scheduler.stop();
    },
    log: () => {}
  });
  scheduler.start();
  await vi.advanceTimersByTimeAsync(20);
  await scheduler.stopAndDrain();
  expect(repo.outboxCounts()).toEqual({ DELIVERED: 1, PENDING: 1 });
  scheduler.start();
  await vi.advanceTimersByTimeAsync(20);
  await scheduler.stopAndDrain();
  expect(sent).toEqual(["hello", "second chunk"]);
  expect(repo.outboxCounts()).toEqual({ DELIVERED: 2 });
});

it("a delayed 429 retry cannot be overtaken by the next chunk in that chat", () => {
  const { repo } = setup();
  repo.markPollSuccess({
    proxyAddress: "wallet",
    lastSeenTimestamp: 123,
    lastSeenKeys: ["key"],
    lastPolledAtMs: 1,
    nextPollAtMs: 9999999999999,
    alerts: [alert, { ...alert, text: "second" }]
  });
  const first = repo.listPendingAlerts(100)[0]!;
  repo.claimAlert(first.id);
  repo.finishAlert(first.id, "PENDING", "429", 1000);
  expect(repo.listPendingAlerts(500)).toEqual([]);
  expect(repo.listPendingAlerts(1000).map((r) => r.text)).toEqual(["hello"]);
  repo.claimAlert(first.id);
  repo.finishAlert(first.id, "DELIVERED");
  expect(repo.listPendingAlerts(1000).map((r) => r.text)).toEqual(["second"]);
});

it("marks a long outbox row UNKNOWN after a partial delivery and never replays an acknowledged chunk", async () => {
  vi.useFakeTimers();
  const { repo, dbPath } = setup();
  const text = "a".repeat(3900) + "\n" + "b".repeat(300);
  repo.markPollSuccess({
    proxyAddress: "wallet",
    lastSeenTimestamp: 123,
    lastSeenKeys: ["key"],
    lastPolledAtMs: 1,
    nextPollAtMs: 9999999999999,
    alerts: [{ ...alert, text }]
  });
  const fetch = vi
    .fn<typeof globalThis.fetch>()
    .mockResolvedValueOnce(Response.json({ ok: true, result: { message_id: 1, chat: { id: 10 } } }))
    .mockResolvedValueOnce(
      Response.json({ ok: false, error_code: 429, parameters: { retry_after: 1 } }, { status: 429 })
    );
  vi.stubGlobal("fetch", fetch);
  const sender = createTelegramSender({ token: "test", timeoutMs: 100, log: () => {} });
  const scheduler = new PollScheduler(testConfig(dbPath), repo, brokenClient, {
    sendTelegram: sender,
    log: () => {}
  });
  scheduler.start();
  await vi.advanceTimersByTimeAsync(20);
  await scheduler.stopAndDrain();
  expect(fetch).toHaveBeenCalledTimes(2);
  expect(JSON.parse(String(fetch.mock.calls[0]?.[1]?.body)).text).toBe("a".repeat(3900));
  expect(JSON.parse(String(fetch.mock.calls[1]?.[1]?.body)).text).toBe("b".repeat(300));
  expect(repo.outboxCounts()).toEqual({ UNKNOWN: 1 });
  scheduler.start();
  await vi.advanceTimersByTimeAsync(5000);
  await scheduler.stopAndDrain();
  expect(fetch).toHaveBeenCalledTimes(2);
  expect(repo.outboxCounts()).toEqual({ UNKNOWN: 1 });
});

it("cancels a subscription’s pending alerts immediately and preserves its active send", () => {
  const { repo } = setup();
  admit(repo);
  admit(repo);
  const first = repo.listPendingAlerts(100)[0]!;
  expect(repo.claimAlert(first.id)).toBe(true);

  expect(repo.removeSubscription(10, "wallet").removedSubscriptions).toBe(1);
  expect(repo.outboxCounts()).toEqual({ CANCELLED: 1, SENDING: 1 });
  expect(repo.finishAlert(first.id, "DELIVERED")).toBe(true);
  expect(repo.outboxCounts()).toEqual({ CANCELLED: 1, DELIVERED: 1 });
});

it("cancels all pending subscriptions for the requested chat in the same operation", () => {
  const { repo } = setup();
  repo.upsertTrackedAccount({ proxyAddress: "other-wallet", nextPollAtMs: 0 });
  const owner = repo.upsertSubscriber(1, 10, "private");
  const other = repo.upsertSubscriber(2, 20, "private");
  repo.createSubscription(owner, 10, "other-wallet", 1, 0);
  repo.createSubscription(other, 20, "other-wallet", 2, 0);
  admit(repo);
  repo.markPollSuccess({
    proxyAddress: "other-wallet",
    lastSeenTimestamp: 123,
    lastSeenKeys: [],
    lastPolledAtMs: 1,
    nextPollAtMs: 1000,
    alerts: [alert, { ...alert, chatId: 20 }]
  });

  expect(repo.removeAllSubscriptions(10)).toEqual({ removedSubscriptions: 2, removedTrackedAccounts: 1 });
  expect(repo.outboxCounts()).toEqual({ CANCELLED: 2, PENDING: 1 });
  expect(repo.listPendingAlerts(100).map((row) => row.chat_id)).toEqual([20]);
});

it("rolls back outbox cancellation if removing the subscription fails", () => {
  const { repo, dbPath } = setup();
  admit(repo);
  const db = openDb(dbPath);
  try {
    db.exec(
      "CREATE TRIGGER fail_removal BEFORE DELETE ON subscriptions BEGIN SELECT RAISE(ABORT,'removal failed'); END"
    );
    expect(() => repo.removeSubscription(10, "wallet")).toThrow("removal failed");
    expect(repo.listChatSubscriptions(10)).toHaveLength(1);
    expect(repo.outboxCounts()).toEqual({ PENDING: 1 });
  } finally {
    db.close();
  }
});

it("preserves per-chat FIFO during backoff and dispatches a different chat", () => {
  const { repo } = setup();
  const other = repo.upsertSubscriber(2, 20, "private");
  repo.createSubscription(other, 20, "wallet", 2, 0);
  repo.markPollSuccess({
    proxyAddress: "wallet",
    lastSeenTimestamp: 123,
    lastSeenKeys: [],
    lastPolledAtMs: 1,
    nextPollAtMs: 1000,
    alerts: [alert, { ...alert, text: "second" }, { ...alert, chatId: 20, text: "other chat" }]
  });
  const first = repo.listPendingAlerts(100)[0]!;
  expect(repo.claimAlert(first.id)).toBe(true);
  expect(repo.listPendingAlerts(100).map((row) => row.text)).toEqual(["other chat"]);
  expect(repo.finishAlert(first.id, "PENDING", "429", 1000)).toBe(true);
  expect(repo.listPendingAlerts(999).map((row) => row.text)).toEqual(["other chat"]);
  expect(repo.listPendingAlerts(1000).map((row) => row.text)).toEqual(["hello", "other chat"]);
});

it("does not finalize an alert that recovery already marked UNKNOWN", () => {
  const { repo } = setup();
  admit(repo);
  const { id } = repo.listPendingAlerts(100)[0]!;
  expect(repo.claimAlert(id)).toBe(true);
  expect(repo.recoverOutbox()).toBe(1);
  expect(repo.finishAlert(id, "DELIVERED")).toBe(false);
  expect(repo.finishAlert(id, "PENDING", "429", 1000)).toBe(false);
  expect(repo.outboxCounts()).toEqual({ UNKNOWN: 1 });
});

it("puts the final retryable send in REJECTED and unblocks the next alert", () => {
  const { repo, dbPath } = setup();
  admit(repo);
  admit(repo);
  const { id } = repo.listPendingAlerts(100)[0]!;
  for (let attempt = 1; attempt <= MAX_ALERT_ATTEMPTS; attempt++) {
    expect(repo.listPendingAlerts(attempt * 100)[0]?.id).toBe(id);
    expect(repo.claimAlert(id)).toBe(true);
    expect(repo.finishAlert(id, "PENDING", "429", (attempt + 1) * 100)).toBe(true);
  }
  expect(repo.outboxCounts()).toEqual({ PENDING: 1, REJECTED: 1 });
  expect(repo.claimAlert(id)).toBe(false);
  expect(repo.listPendingAlerts(2000)[0]?.id).not.toBe(id);
  const db = openDb(dbPath);
  try {
    expect(
      db.prepare("SELECT attempts,next_attempt_ms,last_error FROM alert_outbox WHERE id=?").get(id)
    ).toEqual({
      attempts: MAX_ALERT_ATTEMPTS,
      next_attempt_ms: 0,
      last_error: "Delivery attempt limit reached"
    });
  } finally {
    db.close();
  }
});

it("adds the subscription index to a populated database and handles an already exhausted pending row", () => {
  const { repo, dbPath } = setup();
  admit(repo);
  const { id } = repo.listPendingAlerts(100)[0]!;
  const db = openDb(dbPath);
  db.exec("DROP INDEX idx_alert_outbox_subscription");
  db.prepare("UPDATE alert_outbox SET attempts=? WHERE id=?").run(MAX_ALERT_ATTEMPTS, id);
  const reopened = new AppRepository(db);
  try {
    expect(db.pragma("index_info(idx_alert_outbox_subscription)")).toMatchObject([
      { name: "subscription_id" },
      { name: "status" }
    ]);
    expect(db.prepare("SELECT text,attempts FROM alert_outbox WHERE id=?").get(id)).toEqual({
      text: "hello",
      attempts: MAX_ALERT_ATTEMPTS
    });
    expect(reopened.claimAlert(id)).toBe(false);
    expect(reopened.listPendingAlerts(100)).toEqual([]);
    expect(reopened.outboxCounts()).toEqual({ REJECTED: 1 });
  } finally {
    reopened.close();
  }
});
