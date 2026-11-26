import { afterEach, describe, expect, it, vi } from "vitest";
import { DefaultPolymarketClient, parseUserActivity } from "../src/apis/polymarket.js";
import { buildActivityKey, updateWatermarkState } from "../src/services/activity.js";
import { SlidingWindowRateGate } from "../src/utils/rateGate.js";
import { testConfig } from "./helpers.js";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

describe("DefaultPolymarketClient", () => {
  it("resolves a profile slug from public-search exact match", async () => {
    const fetchMock = vi.fn(async () => {
      return new Response(
        JSON.stringify({
          profiles: [{ name: "weflyhigh", pseudonym: "Admired-Teacher", proxyWallet: "0xabc" }]
        }),
        { status: 200 }
      );
    });
    vi.stubGlobal("fetch", fetchMock);

    const client = new DefaultPolymarketClient(testConfig("/tmp/test.db"));
    const result = await client.resolveAccountInput({ kind: "profile_slug", slug: "weflyhigh" });

    expect(result).toEqual({
      proxyAddress: "0xabc",
      name: "weflyhigh",
      pseudonym: "Admired-Teacher",
      profileSlug: "weflyhigh"
    });
    expect(fetchMock.mock.calls.length).toBeGreaterThan(0);
    const firstCall = fetchMock.mock.calls[0] as unknown[] | undefined;
    expect(String(firstCall?.[0])).toContain("/public-search?");
  });

  it("acquires a Gamma ticket per JSON attempt and pauses on 429", async () => {
    vi.useFakeTimers();
    const acquire = vi.spyOn(SlidingWindowRateGate.prototype, "acquire");
    const pause = vi.spyOn(SlidingWindowRateGate.prototype, "pauseFor");
    const { client, fetchMock } = clientWith(
      [
        new Response(null, { status: 429, headers: { "Retry-After": "2" } }),
        { profiles: [{ name: "fixture", proxyWallet: wallet }] }
      ],
      { polymarketRetries: 1 }
    );

    const request = client.resolveAccountInput({ kind: "profile_slug", slug: "fixture" });
    await vi.advanceTimersByTimeAsync(1999);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(acquire).toHaveBeenCalledTimes(1);
    expect(pause).toHaveBeenCalledExactlyOnceWith(2000);
    await vi.advanceTimersByTimeAsync(1);
    await expect(request).resolves.toMatchObject({ proxyAddress: wallet });
    expect(acquire).toHaveBeenCalledTimes(2);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it("acquires a Gamma ticket per profile page attempt and pauses on 429", async () => {
    vi.useFakeTimers();
    const acquire = vi.spyOn(SlidingWindowRateGate.prototype, "acquire");
    const pause = vi.spyOn(SlidingWindowRateGate.prototype, "pauseFor");
    const urlPaths: string[] = [];
    const responses = [
      new Response('{"profiles":[]}'),
      new Response(null, { status: 429, headers: { "Retry-After": "2" } }),
      new Response(`<script>{"proxyAddress":"${wallet}"}</script>`),
      new Response(JSON.stringify({ proxyWallet: wallet, name: "fixture" }))
    ];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string) => {
        urlPaths.push(new URL(url).pathname);
        return responses.shift();
      })
    );
    const client = new DefaultPolymarketClient(testConfig("/tmp/test.db", { polymarketRetries: 1 }));

    const request = client.resolveAccountInput({ kind: "profile_slug", slug: "fixture" });
    await vi.advanceTimersByTimeAsync(1999);
    expect(urlPaths).toEqual(["/public-search", "/@fixture"]);
    expect(acquire).toHaveBeenCalledTimes(2);
    expect(pause).toHaveBeenCalledExactlyOnceWith(2000);
    await vi.advanceTimersByTimeAsync(1);
    await expect(request).resolves.toMatchObject({ proxyAddress: wallet });
    expect(urlPaths).toEqual(["/public-search", "/@fixture", "/@fixture", "/public-profile"]);
    expect(acquire).toHaveBeenCalledTimes(4);
  });

  it("returns null for a missing public profile after exactly one request", async () => {
    const { client, fetchMock } = clientWith([new Response(null, { status: 404 })], { polymarketRetries: 3 });
    await expect(client["fetchPublicProfile"](wallet)).resolves.toBeNull();
    expect(fetchMock).toHaveBeenCalledTimes(1);
  });
});
const wallet = "0xabc";
const row = (timestamp = 120, changes: Record<string, unknown> = {}) => ({
  proxy_wallet: wallet,
  timestamp,
  type: "TRADE",
  transaction_hash: "0x1",
  condition_id: "0xc",
  token_id: "123",
  side: "BUY",
  size: 2,
  usdc_size: 1,
  ...changes
});
const page = (data: unknown[] = [], next: string | null = null) => ({
  data,
  pagination: { next_cursor: next, has_more: next !== null }
});
function clientWith(bodies: unknown[], options = {}) {
  const fetchMock = vi.fn(async () => {
    const body = bodies.shift();
    if (body instanceof Error) throw body;
    if (body instanceof Response) return body;
    return new Response(JSON.stringify(body), { status: 200 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return { client: new DefaultPolymarketClient(testConfig("/tmp/test.db", options)), fetchMock };
}

describe("activity v2 contract", () => {
  it("follows opaque cursors across short and empty pages with frozen filters", async () => {
    const { client, fetchMock } = clientWith([
      page([row()], "a+/="),
      page([], "b"),
      page([row(119, { type: "COMMENT" }), row(118, { type: "MERGE" })])
    ]);
    const result = await client.fetchRecentUserActivity(wallet, 100, 200);
    expect(result.activities.map((r) => r.type)).toEqual(["TRADE", "MERGE"]);
    expect(result.apiCalls).toBe(3);
    const urls = fetchMock.mock.calls.map((call) => new URL(String((call as unknown[])[0])));
    expect(urls.map((u) => u.searchParams.get("cursor"))).toEqual([null, "a+/=", "b"]);
    for (const [i, u] of urls.entries()) {
      expect(u.pathname).toBe("/v2/activity");
      expect(u.searchParams.has("offset")).toBe(false);
      expect(u.searchParams.get("user")).toBe(wallet);
      expect(u.searchParams.get("start")).toBe("100");
      expect(u.searchParams.get("end")).toBe("200");
      expect(u.searchParams.get("sort_direction")).toBe("DESC");
      expect(u.searchParams.get("sort_by")).toBe("TIMESTAMP");
      expect(u.searchParams.get("limit")).toBe(i === 0 ? "3" : null);
    }
  });
  it("accepts a full terminal page and moves end only between polls", async () => {
    const { client, fetchMock } = clientWith([page([row(120), row(119), row(118)]), page([])]);
    expect((await client.fetchRecentUserActivity(wallet, 100, 200)).activities).toHaveLength(3);
    await client.fetchRecentUserActivity(wallet, 100, 201);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(new URL(String((fetchMock.mock.calls[1] as unknown[])[0])).searchParams.get("end")).toBe("201");
  });
  it.each([
    ["legacy bare array", [row()]],
    ["missing pagination", { data: [] }],
    ["missing cursor", { data: [], pagination: { has_more: false } }],
    ["missing has_more", { data: [], pagination: { next_cursor: null } }],
    ["contradictory has_more", { data: [], pagination: { next_cursor: null, has_more: true } }],
    ["empty cursor", page([], "")],
    ["invalid rows", page([null])],
    ["wrong wallet", page([row(120, { proxy_wallet: "0xother" })])],
    ["outside start", page([row(99)])],
    ["outside end", page([row(201)])],
    ["numeric token ID", page([row(120, { token_id: 123 })])],
    ["nonfinite amount", page([row(120, { size: "NaN" })])],
    ["oversized page", page([row(), row(), row(), row()])],
    ["wrong order", page([row(118), row(120)])]
  ])("rejects %s without returning a partial result", async (_label, body) => {
    const { client } = clientWith([body]);
    await expect(client.fetchRecentUserActivity(wallet, 100, 200)).rejects.toThrow();
  });
  it("rejects repeated cursors and exhausted page budget", async () => {
    const a = clientWith([page([row()], "a"), page([], "a")]);
    await expect(a.client.fetchRecentUserActivity(wallet, 100, 200)).rejects.toThrow(/Repeated/);
    const b = clientWith([page([row()], "a")], { activityMaxPages: 1 });
    await expect(b.client.fetchRecentUserActivity(wallet, 100, 200)).rejects.toThrow(/budget/);
  });
  it("rejects a missing terminal cursor after a valid page", async () => {
    const { client, fetchMock } = clientWith([
      page([row()], "a"),
      { data: [], pagination: { has_more: false } }
    ]);
    await expect(client.fetchRecentUserActivity(wallet, 100, 200)).rejects.toThrow(/next_cursor/);
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });
  it.each([
    new Error("network"),
    new Response("{}", { status: 503 }),
    new Response("invalid JSON", { status: 200 })
  ])("fails a partial walk on transport/JSON errors", async (failure) => {
    const { client } = clientWith([page([row()], "a"), failure]);
    await expect(client.fetchRecentUserActivity(wallet, 100, 200)).rejects.toThrow();
  });
  it("bootstrap covers the whole latest timestamp without scanning years of older history", async () => {
    const { client, fetchMock } = clientWith([
      page([row()], "a"),
      page([row(120, { transaction_hash: "0x2" }), row(119)], "older")
    ]);
    const result = await client.fetchRecentUserActivity(wallet, 0, 200, { bootstrapOnly: true });
    const state = updateWatermarkState(
      { initialized: false, lastSeenTimestamp: 0, lastSeenKeys: [] },
      result.activities
    );
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(state.bootstrap).toBe(true);
    expect(state.newActivities).toEqual([]);
    expect(state.nextState.lastSeenKeys).toHaveLength(2);
  });
  it("preserves legacy watermark keys, including explicit null amounts, without displaying unknown as zero", () => {
    const normalized = parseUserActivity([row(120, { size: null, usdc_size: null, price: null })], "v2")[0]!;
    const oldKey = buildActivityKey({
      proxyWallet: wallet,
      timestamp: 120,
      type: "TRADE",
      conditionId: "0xc",
      transactionHash: "0x1",
      asset: "123",
      side: "BUY",
      size: 0,
      usdcSize: 0
    });
    expect(normalized.size).toBeUndefined();
    expect(normalized.usdcSize).toBeUndefined();
    expect(normalized.price).toBeUndefined();
    expect(buildActivityKey(normalized)).toBe(oldKey);
    const old = { initialized: true, lastSeenTimestamp: 120, lastSeenKeys: [oldKey] };
    expect(updateWatermarkState(old, [normalized, normalized]).newActivities).toEqual([]);
    const changed = parseUserActivity([row(120, { token_id: "124", size: null, usdc_size: null })], "v2");
    expect(updateWatermarkState(old, changed).newActivities).toHaveLength(1);
  });
});

describe("activity normalization", () => {
  it.each([1_700_000_000, "1700000000", 1_700_000_000_123, "1700000000123", "2023-11-14T22:13:20Z"])(
    "normalizes %s to epoch seconds with the same activity key",
    (timestamp) => {
      const [activity] = parseUserActivity([row(120, { timestamp })], "v2");
      const [expected] = parseUserActivity([row(1_700_000_000)], "v2");
      expect(activity?.timestamp).toBe(1_700_000_000);
      expect(activity?.legacyActivityKey).toBe(expected?.legacyActivityKey);
    }
  );

  it.each([0, -1, "0", "-1", "  ", 1.5, Number.MAX_SAFE_INTEGER + 1])(
    "rejects invalid timestamp %s",
    (timestamp) => {
      expect(() => parseUserActivity([row(120, { timestamp })], "v2")).toThrow(/timestamp/);
    }
  );

  it("normalizes trade sides and accepts an absent side for non-trade activity", () => {
    const activities = parseUserActivity(
      [row(120, { side: "buy" }), row(119, { side: " Sell " }), row(118, { type: "MERGE", side: "" })],
      "v2"
    );
    expect(activities.map((activity) => activity.side)).toEqual(["BUY", "SELL", undefined]);
  });

  it.each(["HOLD", 1])("rejects an invalid side %s", (side) => {
    expect(() => parseUserActivity([row(120, { side })], "v2")).toThrow(/side/);
  });
});

it("refuses a latest timestamp group larger than the persisted 500-key capacity", async () => {
  const rows = Array.from({ length: 501 }, (_, i) => row(120, { transaction_hash: `0x${i}` }));
  const { client } = clientWith([page(rows.slice(0, 300), "a"), page(rows.slice(300))], {
    activityPageLimit: 300,
    activityMaxPages: 2
  });
  await expect(client.fetchRecentUserActivity(wallet, 100, 200)).rejects.toThrow(/capacity/);
});

it("v1 and v2 retain identical normal identities and normalized fields", () => {
  const legacy = {
    proxyWallet: wallet,
    timestamp: 120,
    type: "trade",
    transactionHash: "0x1",
    conditionId: "0xC",
    asset: "123",
    side: "buy",
    size: 2,
    usdcSize: 1
  };
  expect(parseUserActivity([legacy], "v1")).toEqual(parseUserActivity([row()], "v2"));
});
it("bootstrap rejects a repeated cursor even after reaching an older timestamp", async () => {
  const { client } = clientWith([page([row()], "a"), page([row(119)], "a")]);
  await expect(client.fetchRecentUserActivity(wallet, 0, 200, { bootstrapOnly: true })).rejects.toThrow(
    /Repeated/
  );
});
