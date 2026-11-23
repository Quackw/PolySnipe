import { afterEach, describe, expect, it, vi } from "vitest";
import { SlidingWindowRateGate } from "../src/utils/rateGate.js";

afterEach(() => {
  vi.useRealTimers();
});

describe("SlidingWindowRateGate", () => {
  it("pauses new requests during a shared cooldown", async () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-07T00:00:00.000Z"));
    const gate = new SlidingWindowRateGate(100, 10_000, 5);
    gate.pauseFor(1000);

    let acquired = false;
    const pending = gate.acquire().then((ticket) => {
      acquired = true;
      ticket.release();
    });

    await vi.advanceTimersByTimeAsync(999);
    expect(acquired).toBe(false);
    await vi.advanceTimersByTimeAsync(1);
    await pending;
    expect(acquired).toBe(true);
  });
});
