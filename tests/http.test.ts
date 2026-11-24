import { afterEach, describe, expect, it, vi } from "vitest";
import { getText, calculateHttpRetryDelayMs, parseRetryAfterMs } from "../src/apis/http.js";

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
});

it("reacquires for every attempt and releases before retry sleep", async () => {
  vi.useFakeTimers();
  const release = vi.fn();
  const acquire = vi.fn(async () => release);
  const pause = vi.fn();
  vi.stubGlobal(
    "fetch",
    vi
      .fn()
      .mockResolvedValueOnce(new Response("limited", { status: 429 }))
      .mockResolvedValueOnce(new Response("ok"))
  );
  const request = getText("https://example.invalid", {
    timeoutMs: 5000,
    retries: 1,
    beforeAttempt: acquire,
    onRateLimit: pause
  });
  await vi.advanceTimersByTimeAsync(1);
  expect(release).toHaveBeenCalledTimes(1);
  expect(pause).toHaveBeenCalledTimes(1);
  await vi.runAllTimersAsync();
  expect(await request).toBe("ok");
  expect(acquire).toHaveBeenCalledTimes(2);
  expect(release).toHaveBeenCalledTimes(2);
});

it("pauses the shared queue even when no retries remain", async () => {
  const pause = vi.fn(),
    release = vi.fn();
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response("limited", { status: 429, headers: { "retry-after": "10" } }))
  );
  await expect(
    getText("https://example.invalid", {
      timeoutMs: 1000,
      retries: 0,
      beforeAttempt: async () => release,
      onRateLimit: pause
    })
  ).rejects.toThrow("429");
  expect(pause.mock.calls[0]?.[0]).toBeGreaterThanOrEqual(10000);
  expect(release).toHaveBeenCalledTimes(1);
});

describe("HTTP retry helpers", () => {
  it("parses Retry-After seconds and dates", () => {
    expect(parseRetryAfterMs("2", 1000)).toBe(2000);
    expect(parseRetryAfterMs("Thu, 01 Jan 1970 00:00:05 GMT", 1000)).toBe(4000);
    expect(parseRetryAfterMs("invalid", 1000)).toBeUndefined();
  });

  it("uses a longer exponential delay for rate limits", () => {
    expect(calculateHttpRetryDelayMs({ statusCode: 429 }, 1, 0)).toBe(1000);
    expect(calculateHttpRetryDelayMs({ statusCode: 429 }, 2, 0)).toBe(2000);
    expect(calculateHttpRetryDelayMs({ statusCode: 429, retryAfterMs: 5000 }, 1, 0)).toBe(5000);
  });

  it("preserves the short retry delay for non-rate-limit failures", () => {
    expect(calculateHttpRetryDelayMs({ statusCode: 500 }, 2, 0)).toBe(400);
  });
});
