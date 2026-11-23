import { afterEach, describe, expect, it, vi } from "vitest";
import { DefaultPolymarketClient, parseUserActivity } from "../src/apis/polymarket.js";
import { buildActivityKey, updateWatermarkState } from "../src/services/activity.js";
import { testConfig } from "./helpers.js";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
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

});
const wallet = "0xabc";
const row = (timestamp = 120, changes: Record<string, unknown> = {}) => ({
  proxy_wallet: wallet, timestamp, type: "TRADE", transaction_hash: "0x1",
  condition_id: "0xc", token_id: "123", side: "BUY", size: 2, usdc_size: 1, ...changes
});
const page = (data: unknown[] = [], next: string | null = null) => ({ data, pagination: { next_cursor: next, has_more: next !== null } });
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
    const { client, fetchMock } = clientWith([page([row()], "a+/="), page([], "b"), page([row(119, { type: "COMMENT" }), row(118, {type:"MERGE"})])]);
    const result = await client.fetchRecentUserActivity(wallet, 100, 200);
    expect(result.activities.map(r => r.type)).toEqual(["TRADE", "MERGE"]);
    expect(result.apiCalls).toBe(3);
    const urls = fetchMock.mock.calls.map(call => new URL(String((call as unknown[])[0])));
    expect(urls.map(u => u.searchParams.get("cursor"))).toEqual([null, "a+/=", "b"]);
    for (const [i,u] of urls.entries()) {
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
    const {client,fetchMock}=clientWith([page([row(120),row(119),row(118)]),page([])]);
    expect((await client.fetchRecentUserActivity(wallet,100,200)).activities).toHaveLength(3);
    await client.fetchRecentUserActivity(wallet,100,201);
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(new URL(String((fetchMock.mock.calls[1] as unknown[])[0])).searchParams.get("end")).toBe("201");
  });
  it.each([
    ["legacy bare array", [row()]],
    ["missing pagination", {data:[]}],
    ["missing cursor", {data:[],pagination:{has_more:false}}],
    ["missing has_more", {data:[],pagination:{next_cursor:null}}],
    ["contradictory has_more", {data:[],pagination:{next_cursor:null,has_more:true}}],
    ["empty cursor", page([], "")],
    ["invalid rows", page([null])],
    ["wrong wallet", page([row(120,{proxy_wallet:"0xother"})])],
    ["outside start", page([row(99)])],
    ["outside end", page([row(201)])],
    ["numeric token ID", page([row(120,{token_id:123})])],
    ["nonfinite amount", page([row(120,{size:"NaN"})])],
    ["oversized page", page([row(),row(),row(),row()])],
    ["wrong order", page([row(118),row(120)])]
  ])("rejects %s without returning a partial result", async (_label, body) => {
    const {client}=clientWith([body]);
    await expect(client.fetchRecentUserActivity(wallet,100,200)).rejects.toThrow();
  });
  it("rejects repeated cursors and exhausted page budget",async()=>{
    const a=clientWith([page([row()],"a"),page([],"a")]);
    await expect(a.client.fetchRecentUserActivity(wallet,100,200)).rejects.toThrow(/Repeated/);
    const b=clientWith([page([row()],"a")],{activityMaxPages:1});
    await expect(b.client.fetchRecentUserActivity(wallet,100,200)).rejects.toThrow(/budget/);
  });
  it.each([new Error("network"),new Response("{}",{status:503}),new Response("invalid JSON",{status:200})])("fails a partial walk on transport/JSON errors",async failure=>{
    const {client}=clientWith([page([row()],"a"),failure]);
    await expect(client.fetchRecentUserActivity(wallet,100,200)).rejects.toThrow();
  });
  it("bootstrap covers the whole latest timestamp without scanning years of older history",async()=>{
    const {client,fetchMock}=clientWith([page([row()],"a"),page([row(120,{transaction_hash:"0x2"}),row(119)],"older")]);
    const result=await client.fetchRecentUserActivity(wallet,0,200,{bootstrapOnly:true});
    const state=updateWatermarkState({initialized:false,lastSeenTimestamp:0,lastSeenKeys:[]},result.activities);
    expect(fetchMock).toHaveBeenCalledTimes(2);expect(state.bootstrap).toBe(true);
    expect(state.newActivities).toEqual([]);expect(state.nextState.lastSeenKeys).toHaveLength(2);
  });
  it("preserves legacy watermark keys, including explicit null amounts, without displaying unknown as zero",()=>{
    const normalized=parseUserActivity([row(120,{size:null,usdc_size:null,price:null})],"v2")[0]!;
    const oldKey=buildActivityKey({proxyWallet:wallet,timestamp:120,type:"TRADE",conditionId:"0xc",transactionHash:"0x1",asset:"123",side:"BUY",size:0,usdcSize:0});
    expect(normalized.size).toBeUndefined();expect(normalized.usdcSize).toBeUndefined();expect(normalized.price).toBeUndefined();
    expect(buildActivityKey(normalized)).toBe(oldKey);
    const old={initialized:true,lastSeenTimestamp:120,lastSeenKeys:[oldKey]};
    expect(updateWatermarkState(old,[normalized,normalized]).newActivities).toEqual([]);
    const changed=parseUserActivity([row(120,{token_id:"124",size:null,usdc_size:null})],"v2");
    expect(updateWatermarkState(old,changed).newActivities).toHaveLength(1);
  });
});

it("refuses a latest timestamp group larger than the persisted 500-key capacity",async()=>{
  const rows=Array.from({length:501},(_,i)=>row(120,{transaction_hash:`0x${i}`}));
  const {client}=clientWith([page(rows.slice(0,300),"a"),page(rows.slice(300))],{activityPageLimit:300,activityMaxPages:2});
  await expect(client.fetchRecentUserActivity(wallet,100,200)).rejects.toThrow(/capacity/);
});


it("v1 and v2 retain identical normal identities and normalized fields",()=>{
  const legacy={proxyWallet:wallet,timestamp:120,type:"trade",transactionHash:"0x1",conditionId:"0xC",asset:"123",side:"buy",size:2,usdcSize:1};
  expect(parseUserActivity([legacy],"v1")).toEqual(parseUserActivity([row()],"v2"));
});
it("bootstrap rejects a repeated cursor even after reaching an older timestamp",async()=>{
  const {client}=clientWith([page([row()],"a"),page([row(119)],"a")]);
  await expect(client.fetchRecentUserActivity(wallet,0,200,{bootstrapOnly:true})).rejects.toThrow(/Repeated/);
});
