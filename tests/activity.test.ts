import { describe, expect, it } from "vitest";
import { buildActivityKey, filterAllowedActivities, updateWatermarkState } from "../src/services/activity.js";
import type { UserActivity } from "../src/types.js";

function activity(timestamp: number, suffix: string): UserActivity {
  return {
    proxyWallet: "0xabc",
    timestamp,
    type: "TRADE",
    conditionId: "0xcond",
    transactionHash: `0xtx-${suffix}`,
    asset: "asset",
    side: "BUY",
    size: 1,
    usdcSize: 2,
    outcomeIndex: 0
  };
}

describe("filterAllowedActivities", () => {
  it("keeps only supported alert activity types", () => {
    const rows: UserActivity[] = [
      activity(10, "a"),
      { ...activity(11, "b"), type: "MERGE", side: undefined },
      { ...activity(12, "c"), type: "REFERRAL_REWARD", side: undefined },
      { ...activity(13, "d"), type: "COMMENT" }
    ];
    expect(filterAllowedActivities(rows)).toEqual([rows[0], rows[1], rows[2]]);
  });
});

describe("updateWatermarkState", () => {
  it("bootstraps on first run", () => {
    const rows = [activity(20, "a"), activity(19, "b")];
    const result = updateWatermarkState(
      { initialized: false, lastSeenTimestamp: 0, lastSeenKeys: [] },
      rows
    );

    expect(result.bootstrap).toBe(true);
    expect(result.newActivities).toEqual([]);
    expect(result.nextState.lastSeenTimestamp).toBe(20);
    expect(result.nextState.lastSeenKeys).toEqual([buildActivityKey(rows[0]!)]);
  });

  it("returns only new rows at the watermark boundary", () => {
    const existing = activity(20, "a");
    const next = activity(20, "b");
    const newer = activity(25, "c");

    const result = updateWatermarkState(
      {
        initialized: true,
        lastSeenTimestamp: 20,
        lastSeenKeys: [buildActivityKey(existing)]
      },
      [existing, next, newer]
    );

    expect(result.bootstrap).toBe(false);
    expect(result.newActivities).toEqual([newer, next]);
    expect(result.nextState.lastSeenTimestamp).toBe(25);
  });

  it("does not treat an outcomeIndex-only trade difference as a new activity", () => {
    const existing = activity(20, "a");
    const duplicate = { ...existing, outcomeIndex: 999 };

    const result = updateWatermarkState(
      {
        initialized: true,
        lastSeenTimestamp: 20,
        lastSeenKeys: [buildActivityKey(existing)]
      },
      [duplicate]
    );

    expect(result.bootstrap).toBe(false);
    expect(result.newActivities).toEqual([]);
    expect(result.nextState.lastSeenTimestamp).toBe(20);
  });

  it("collapses duplicate activities within the same fetched batch", () => {
    const first = activity(25, "a");
    const duplicate = { ...first, outcomeIndex: 999 };

    const result = updateWatermarkState(
      {
        initialized: true,
        lastSeenTimestamp: 20,
        lastSeenKeys: [buildActivityKey(activity(20, "old"))]
      },
      [first, duplicate]
    );

    expect(result.bootstrap).toBe(false);
    expect(result.newActivities).toEqual([first]);
    expect(result.nextState.lastSeenTimestamp).toBe(25);
    expect(result.nextState.lastSeenKeys).toEqual([buildActivityKey(first)]);
  });
});
