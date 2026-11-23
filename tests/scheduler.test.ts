import { AppRepository } from "../src/db/repository.js";
import { openDb } from "../src/db/client.js";
import { afterEach, describe, expect, it, vi } from "vitest";
import { PollScheduler, calculateEffectiveIntervalMs, computeRetryDelayMs } from "../src/scheduler.js";
import { DefaultPolymarketClient, type PolymarketClient } from "../src/apis/polymarket.js";
import { createTempRepo, testConfig } from "./helpers.js";

const resources: Array<ReturnType<typeof createTempRepo>> = [];

function setupRepo() {
  const temp = createTempRepo("scheduler");
  resources.push(temp);
  return temp;
}

afterEach(() => {
  vi.useRealTimers();
  while (resources.length > 0) {
    resources.pop()?.cleanup();
  }
});

describe("scheduler helpers", () => {
  it("computes adaptive interval", () => {
    expect(calculateEffectiveIntervalMs(0, 70, 5)).toBe(5000);
    expect(calculateEffectiveIntervalMs(350, 70, 5)).toBe(5000);
    expect(calculateEffectiveIntervalMs(500, 70, 5)).toBe(8000);
    expect(calculateEffectiveIntervalMs(1000, 70, 5)).toBe(15000);
  });

  it("computes retry backoff tiers", () => {
    expect(computeRetryDelayMs(1, 0)).toBe(2000);
    expect(computeRetryDelayMs(2, 0)).toBe(5000);
    expect(computeRetryDelayMs(3, 0)).toBe(10000);
    expect(computeRetryDelayMs(4, 0)).toBe(30000);
  });
});

describe("PollScheduler", () => {
  it("waits for an in-flight poll before completing shutdown", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-27T12:00:00.000Z"));
    const { repo, dbPath } = setupRepo();
    repo.upsertTrackedAccount({
      proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
      nextPollAtMs: 0
    });

    let releasePoll: (() => void) | undefined;
    const client: PolymarketClient = {
      async resolveAccountInput() {
        throw new Error("not used");
      },
      async fetchRecentUserActivity() {
        await new Promise<void>((resolve) => {
          releasePoll = resolve;
        });
        return { activities: [], apiCalls: 1, retries: 0, throttleWaitMs: 0 };
      }
    };
    const scheduler = new PollScheduler(testConfig(dbPath, { schedulerTickMs: 10 }), repo, client, {
      sendTelegram: async () => undefined,
      log: () => {}
    });

    scheduler.start();
    await vi.advanceTimersByTimeAsync(10);
    let drained = false;
    const stopPromise = scheduler.stopAndDrain().then(() => {
      drained = true;
    });

    await vi.advanceTimersByTimeAsync(100);
    expect(drained).toBe(false);
    releasePoll?.();
    await vi.advanceTimersByTimeAsync(25);
    await stopPromise;
    expect(drained).toBe(true);
  });

  it("bootstraps on first poll and alerts on the next new activity", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-27T12:00:00.000Z"));
    const { repo, dbPath } = setupRepo();
    const subscriberId = repo.upsertSubscriber(1, 100, "private");
    repo.upsertTrackedAccount({
      proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
      profileSlug: "weflyhigh",
      nextPollAtMs: 0
    });
    repo.createSubscription(subscriberId, 100, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 1, 1000);

    let callCount = 0;
    const client: PolymarketClient = {
      async resolveAccountInput() {
        throw new Error("not used");
      },
      async fetchRecentUserActivity(_wallet, _start, _end, hooks) {
        callCount += 1;
        expect(hooks?.bootstrapOnly).toBe(callCount === 1);
        if (callCount === 1) {
          return {
            activities: [
              { proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", timestamp: 1001, type: "TRADE", transactionHash: "0x1" }
            ],
            apiCalls: 1,
            retries: 0,
            throttleWaitMs: 0
          };
        }

        return {
          activities: [
            {
              proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
              timestamp: 1002,
              type: "TRADE",
              transactionHash: "0x2",
              side: "BUY",
              outcome: "YES",
              title: "US forces enter Iran by April 30?",
              slug: "us-forces-enter-iran-by-april-30",
              size: 4,
              price: 0.42,
              usdcSize: 1.68
            }
          ],
          apiCalls: 1,
          retries: 0,
          throttleWaitMs: 0
        };
      }
    };

    const sent: Array<{ chatId: number; text: string }> = [];
    const scheduler = new PollScheduler(testConfig(dbPath, { schedulerTickMs: 10, pollTargetIntervalSec: 1 }), repo, client, {
      sendTelegram: async (chatId, text) => {
        sent.push({ chatId, text });
      },
      log: () => {}
    });

    scheduler.start();
    await vi.advanceTimersByTimeAsync(50);
    expect(sent).toHaveLength(0);

    await vi.advanceTimersByTimeAsync(1200);
    scheduler.stop();

    expect(sent).toHaveLength(1);
    expect(sent[0]?.chatId).toBe(100);
    expect(sent[0]?.text).toContain('<a href="https://polymarket.com/@weflyhigh">@weflyhigh</a>');
    expect(sent[0]?.text).toContain('<b>TRADE</b>, outcome: <b>YES</b>, side: <b>BUY</b>');
    expect(repo.getAdminPerformanceReport(Date.now()).sampleCount).toBe(1);
  });

  it("keeps poll failures log-only", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-27T12:00:00.000Z"));
    const { repo, dbPath } = setupRepo();
    const subscriberId = repo.upsertSubscriber(1, 100, "private");
    repo.upsertTrackedAccount({
      proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
      nextPollAtMs: 0
    });
    repo.createSubscription(subscriberId, 100, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 1, 1000);

    const sendTelegram = vi.fn(async () => undefined);
    const client: PolymarketClient = {
      async resolveAccountInput() {
        throw new Error("not used");
      },
      async fetchRecentUserActivity() {
        throw new Error("poll failed");
      }
    };
    const logs: string[] = [];

    const scheduler = new PollScheduler(testConfig(dbPath, { schedulerTickMs: 10, pollTargetIntervalSec: 1 }), repo, client, {
      sendTelegram,
      log: (line) => logs.push(line)
    });

    scheduler.start();
    await vi.advanceTimersByTimeAsync(50);
    scheduler.stop();

    expect(sendTelegram).not.toHaveBeenCalled();
    expect(logs.some((line) => line.includes("[poll:error]"))).toBe(true);
  });

  it("records one performance sample per activity per sent chat alert", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-27T12:00:00.000Z"));
    const { repo, dbPath } = setupRepo();
    const subscriberA = repo.upsertSubscriber(1, 100, "private");
    const subscriberB = repo.upsertSubscriber(2, 200, "private");
    repo.upsertTrackedAccount({
      proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
      nextPollAtMs: 0
    });
    repo.createSubscription(subscriberA, 100, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 1, 1000);
    repo.createSubscription(subscriberB, 200, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 2, 1000);

    let callCount = 0;
    const client: PolymarketClient = {
      async resolveAccountInput() {
        throw new Error("not used");
      },
      async fetchRecentUserActivity(_wallet, _start, _end, hooks) {
        callCount += 1;
        expect(hooks?.bootstrapOnly).toBe(callCount === 1);
        if (callCount === 1) {
          return {
            activities: [
              { proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", timestamp: Math.floor(Date.now() / 1000) - 5, type: "TRADE", transactionHash: "0x1" }
            ],
            apiCalls: 1,
            retries: 0,
            throttleWaitMs: 0
          };
        }

        return {
          activities: [
            { proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", timestamp: Math.floor(Date.now() / 1000) - 5, type: "TRADE", transactionHash: "0x2", side: "BUY" },
            { proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", timestamp: Math.floor(Date.now() / 1000) - 4, type: "MERGE", transactionHash: "0x3" },
            { proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", timestamp: Math.floor(Date.now() / 1000) - 3, type: "REFERRAL_REWARD", transactionHash: "0x4" }
          ],
          apiCalls: 1,
          retries: 0,
          throttleWaitMs: 0
        };
      }
    };

    const scheduler = new PollScheduler(testConfig(dbPath, { schedulerTickMs: 10, pollTargetIntervalSec: 1 }), repo, client, {
      sendTelegram: async () => undefined,
      log: () => {}
    });

    scheduler.start();
    await vi.advanceTimersByTimeAsync(50);
    await vi.advanceTimersByTimeAsync(1200);
    scheduler.stop();

    expect(repo.getAdminPerformanceReport(Date.now()).sampleCount).toBe(6);
  });

  it("does not record performance samples for failed sends", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-27T12:00:00.000Z"));
    const { repo, dbPath } = setupRepo();
    const subscriberId = repo.upsertSubscriber(1, 100, "private");
    repo.upsertTrackedAccount({
      proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
      nextPollAtMs: 0
    });
    repo.createSubscription(subscriberId, 100, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 1, 1000);

    let callCount = 0;
    const client: PolymarketClient = {
      async resolveAccountInput() {
        throw new Error("not used");
      },
      async fetchRecentUserActivity(_wallet, _start, _end, hooks) {
        callCount += 1;
        expect(hooks?.bootstrapOnly).toBe(callCount === 1);
        if (callCount === 1) {
          return {
            activities: [
              { proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", timestamp: Math.floor(Date.now() / 1000) - 5, type: "TRADE", transactionHash: "0x1" }
            ],
            apiCalls: 1,
            retries: 0,
            throttleWaitMs: 0
          };
        }

        return {
          activities: [
            {
              proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
              timestamp: Math.floor(Date.now() / 1000) - 4,
              type: "TRADE",
              transactionHash: "0x2",
              side: "SELL"
            }
          ],
          apiCalls: 1,
          retries: 0,
          throttleWaitMs: 0
        };
      }
    };

    const scheduler = new PollScheduler(testConfig(dbPath, { schedulerTickMs: 10, pollTargetIntervalSec: 1 }), repo, client, {
      sendTelegram: async () => {
        throw new Error("telegram send failed");
      },
      log: () => {}
    });

    scheduler.start();
    await vi.advanceTimersByTimeAsync(50);
    await vi.advanceTimersByTimeAsync(1200);
    scheduler.stop();

    expect(repo.getAdminPerformanceReport(Date.now()).sampleCount).toBe(0);
  });

  it("sends alerts for supported non-trade activity", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-27T12:00:00.000Z"));
    const { repo, dbPath } = setupRepo();
    const subscriberId = repo.upsertSubscriber(1, 100, "private");
    repo.upsertTrackedAccount({
      proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
      nextPollAtMs: 0
    });
    repo.createSubscription(subscriberId, 100, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 1, 1000);

    let callCount = 0;
    const sent: Array<{ chatId: number; text: string }> = [];
    const client: PolymarketClient = {
      async resolveAccountInput() {
        throw new Error("not used");
      },
      async fetchRecentUserActivity(_wallet, _start, _end, hooks) {
        callCount += 1;
        expect(hooks?.bootstrapOnly).toBe(callCount === 1);
        if (callCount === 1) {
          return {
            activities: [
              { proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", timestamp: 1001, type: "TRADE", transactionHash: "0x1" }
            ],
            apiCalls: 1,
            retries: 0,
            throttleWaitMs: 0
          };
        }

        return {
          activities: [
            { proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", timestamp: 1002, type: "MERGE", transactionHash: "0x2", title: "Market merge" }
          ],
          apiCalls: 1,
          retries: 0,
          throttleWaitMs: 0
        };
      }
    };

    const scheduler = new PollScheduler(testConfig(dbPath, { schedulerTickMs: 10, pollTargetIntervalSec: 1 }), repo, client, {
      sendTelegram: async (chatId, text) => {
        sent.push({ chatId, text });
      },
      log: () => {}
    });

    scheduler.start();
    await vi.advanceTimersByTimeAsync(50);
    await vi.advanceTimersByTimeAsync(1200);
    scheduler.stop();

    expect(sent).toHaveLength(1);
    expect(sent[0]?.text).toContain("Market merge | <b>MERGE</b>");
    expect(repo.getAdminPerformanceReport(Date.now()).sampleCount).toBe(1);
  });

  it("prunes old performance samples during heartbeat maintenance", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-27T12:00:00.000Z"));
    const { repo, dbPath } = setupRepo();
    const nowMs = Date.now();
    repo.insertAlertDelaySamples([
      {
        telegramChatId: 100,
        proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
        activityTimestamp: Math.floor((nowMs - 2 * 24 * 60 * 60 * 1000) / 1000),
        sendStartedAtMs: nowMs - 2 * 24 * 60 * 60 * 1000,
        delayMs: 5_000
      }
    ]);

    const scheduler = new PollScheduler(testConfig(dbPath, { heartbeatEveryMs: 100 }), repo, {
      async resolveAccountInput() {
        throw new Error("not used");
      },
      async fetchRecentUserActivity() {
        return { activities: [], apiCalls: 1, retries: 0, throttleWaitMs: 0 };
      }
    }, {
      sendTelegram: async () => undefined,
      log: () => {}
    });

    scheduler.start();
    await vi.advanceTimersByTimeAsync(150);
    scheduler.stop();

    expect(repo.cleanupOldAlertDelaySamples(Date.now())).toBe(0);
  });

  it("logs and classifies upstream-stale delays without treating them as scheduler freshness regressions", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-27T12:00:00.000Z"));
    const { repo, dbPath } = setupRepo();
    const subscriberId = repo.upsertSubscriber(1, 100, "private");
    repo.upsertTrackedAccount({
      proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
      nextPollAtMs: 0
    });
    repo.createSubscription(subscriberId, 100, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 1, 1000);

    let callCount = 0;
    const client: PolymarketClient = {
      async resolveAccountInput() {
        throw new Error("not used");
      },
      async fetchRecentUserActivity(_wallet, _start, _end, hooks) {
        callCount += 1;
        expect(hooks?.bootstrapOnly).toBe(callCount === 1);
        if (callCount === 1) {
          return {
            activities: [
              { proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", timestamp: 1_001, type: "TRADE", transactionHash: "0x1" }
            ],
            apiCalls: 1,
            retries: 0,
            throttleWaitMs: 0
          };
        }

        return {
          activities: [
            { proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", timestamp: 1_002, type: "TRADE", transactionHash: "0x2" }
          ],
          apiCalls: 1,
          retries: 0,
          throttleWaitMs: 0
        };
      }
    };
    const logs: string[] = [];

    const scheduler = new PollScheduler(testConfig(dbPath, {
      schedulerTickMs: 10,
      pollTargetIntervalSec: 1,
      alertDelayThresholdSec: 30
    }), repo, client, {
      sendTelegram: async () => undefined,
      log: (line) => logs.push(line)
    });

    scheduler.start();
    await vi.advanceTimersByTimeAsync(50);
    await vi.advanceTimersByTimeAsync(1_200);
    scheduler.stop();

    const report = repo.getAdminPerformanceReport(Date.now());
    expect(report.classifiedSampleCount).toBe(1);
    expect(report.upstreamStaleCount).toBe(1);
    expect(report.delayReasonCounts.find((item) => item.reason === "upstream_stale")?.count).toBe(1);
    expect(logs.some((line) => line.includes("[alert:stale_batch]"))).toBe(true);
    expect(logs.some((line) => line.includes("[alert:delay]") && line.includes("reason=upstream_stale"))).toBe(true);
  });

  it("keeps per-chat send failures isolated and still records successful alert samples", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-03-27T12:00:00.000Z"));
    const { repo, dbPath } = setupRepo();
    const subscriberA = repo.upsertSubscriber(1, 100, "private");
    const subscriberB = repo.upsertSubscriber(2, 200, "private");
    repo.upsertTrackedAccount({
      proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
      nextPollAtMs: 0
    });
    repo.createSubscription(subscriberA, 100, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 1, 1000);
    repo.createSubscription(subscriberB, 200, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 2, 1000);

    let callCount = 0;
    const client: PolymarketClient = {
      async resolveAccountInput() {
        throw new Error("not used");
      },
      async fetchRecentUserActivity(_wallet, _start, _end, hooks) {
        callCount += 1;
        expect(hooks?.bootstrapOnly).toBe(callCount === 1);
        if (callCount === 1) {
          return {
            activities: [
              { proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", timestamp: Math.floor(Date.now() / 1000) - 5, type: "TRADE", transactionHash: "0x1" }
            ],
            apiCalls: 1,
            retries: 0,
            throttleWaitMs: 0
          };
        }

        return {
          activities: [
            { proxyWallet: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", timestamp: Math.floor(Date.now() / 1000) - 5, type: "TRADE", transactionHash: "0x2" }
          ],
          apiCalls: 1,
          retries: 0,
          throttleWaitMs: 0
        };
      }
    };
    const logs: string[] = [];
    const sentChats: number[] = [];

    const scheduler = new PollScheduler(testConfig(dbPath, {
      schedulerTickMs: 10,
      pollTargetIntervalSec: 1,
      alertSendConcurrency: 2
    }), repo, client, {
      sendTelegram: async (chatId) => {
        if (chatId === 100) {
          throw new Error("chat blocked");
        }
        sentChats.push(chatId);
      },
      log: (line) => logs.push(line)
    });

    scheduler.start();
    await vi.advanceTimersByTimeAsync(50);
    await vi.advanceTimersByTimeAsync(1_200);
    scheduler.stop();

    expect(sentChats).toEqual([200]);
    expect(repo.getAdminPerformanceReport(Date.now()).sampleCount).toBe(1);
    expect(logs.some((line) => line.includes("[alert:error]") && line.includes("chat=100"))).toBe(true);
  });
});

it("incomplete v2 traversal leaves persisted watermark unchanged and sends nothing", async () => {
  vi.useFakeTimers();vi.setSystemTime(new Date("2026-10-23T08:00:00Z"));
  const {repo,dbPath}=setupRepo();
  const wallet="0x03e8a544e97eeff5753bc1e90d46e5ef22af1697";
  const subscriberId=repo.upsertSubscriber(1,100,"private");
  repo.upsertTrackedAccount({proxyAddress:wallet,nextPollAtMs:0});
  repo.createSubscription(subscriberId,100,wallet,1,0);
  repo.markPollSuccess({proxyAddress:wallet,lastSeenTimestamp:110,lastSeenKeys:["legacy-key"],lastPolledAtMs:1,nextPollAtMs:0});
  const sendTelegram=vi.fn(async()=>undefined);
  vi.stubGlobal("fetch",vi.fn(async()=>new Response(JSON.stringify({
    data:[{proxy_wallet:wallet,timestamp:120,type:"TRADE",transaction_hash:"0x1"}],
    pagination:{next_cursor:"repeated",has_more:true}
  }),{status:200})));
  const config=testConfig(dbPath,{activityMaxPages:2});
  const scheduler=new PollScheduler(config,repo,new DefaultPolymarketClient(config),{sendTelegram,log:()=>{}});
  try{
    scheduler.start();await vi.advanceTimersByTimeAsync(100);await scheduler.stopAndDrain();
    const stored=repo.getTrackedAccount(wallet)!;
    expect(stored.lastSeenTimestamp).toBe(110);expect(stored.lastSeenKeys).toEqual(["legacy-key"]);
    expect(sendTelegram).not.toHaveBeenCalled();
    const diskRepo=new AppRepository(openDb(dbPath));
    try{expect(diskRepo.getTrackedAccount(wallet)?.lastSeenKeys).toEqual(["legacy-key"]);}finally{diskRepo.close();}
  }finally{scheduler.stop();vi.unstubAllGlobals();}
});

it("v2 duplicate rows and reopening the saved database do not resend an acknowledged event",async()=>{
  vi.useFakeTimers();vi.setSystemTime(new Date(200000));
  const {repo,dbPath}=setupRepo();const wallet="0x03e8a544e97eeff5753bc1e90d46e5ef22af1697";
  const subscriberId=repo.upsertSubscriber(1,100,"private");
  repo.upsertTrackedAccount({proxyAddress:wallet,nextPollAtMs:0});
  repo.createSubscription(subscriberId,100,wallet,1,0);
  repo.markPollSuccess({proxyAddress:wallet,lastSeenTimestamp:110,lastSeenKeys:["old"],lastPolledAtMs:1,nextPollAtMs:0});
  const row={proxy_wallet:wallet,timestamp:120,type:"TRADE",transaction_hash:"0x1",condition_id:"0xc",token_id:"123",size:2,usdc_size:1};
  vi.stubGlobal("fetch",vi.fn(async()=>new Response(JSON.stringify({data:[row,row],pagination:{next_cursor:null,has_more:false}}),{status:200})));
  const sendTelegram=vi.fn(async()=>undefined),config=testConfig(dbPath);
  const first=new PollScheduler(config,repo,new DefaultPolymarketClient(config),{sendTelegram,log:()=>{}});
  try{
    first.start();await vi.advanceTimersByTimeAsync(100);await first.stopAndDrain();
    expect(sendTelegram).toHaveBeenCalledTimes(1);
    expect(repo.getTrackedAccount(wallet)?.lastSeenTimestamp).toBe(120);
    const reopened=new AppRepository(openDb(dbPath));
    const second=new PollScheduler(config,reopened,new DefaultPolymarketClient(config),{sendTelegram,log:()=>{}});
    try{
      await vi.advanceTimersByTimeAsync(6000);second.start();await vi.advanceTimersByTimeAsync(100);await second.stopAndDrain();
      expect(sendTelegram).toHaveBeenCalledTimes(1);expect(reopened.getTrackedAccount(wallet)?.lastSeenKeys).toHaveLength(1);
    }finally{second.stop();reopened.close();}
  }finally{first.stop();vi.unstubAllGlobals();}
});
