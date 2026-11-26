import { afterEach, describe, expect, it, vi } from "vitest";
import { getJson, getText, calculateHttpRetryDelayMs, parseRetryAfterMs } from "../src/apis/http.js";

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
  expect(pause.mock.invocationCallOrder[0]).toBeLessThan(release.mock.invocationCallOrder[0]!);
});

it.each([400, 401, 403, 404, 422])(
  "returns HTTP %i after one attempt even if reading its body would fail",
  async (status) => {
    const response = new Response("fixture error", { status });
    const readBody = vi.spyOn(response, "text").mockRejectedValue(new TypeError("body stream lost"));
    const fetchMock = vi.fn().mockResolvedValue(response);
    const onRetry = vi.fn();
    vi.stubGlobal("fetch", fetchMock);

    await expect(
      getText("https://example.invalid", { timeoutMs: 1000, retries: 2, onRetry })
    ).rejects.toMatchObject({ statusCode: status });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(onRetry).not.toHaveBeenCalled();
    expect(readBody).not.toHaveBeenCalled();
  }
);

it.each([-2, 0.9, NaN, Infinity])("makes one attempt with a retry budget of %s", async (retries) => {
  const fetchMock = vi.fn(async () => new Response(null, { status: 503 }));
  vi.stubGlobal("fetch", fetchMock);

  await expect(getText("https://example.invalid", { timeoutMs: 1000, retries })).rejects.toMatchObject({
    statusCode: 503
  });
  expect(fetchMock).toHaveBeenCalledTimes(1);
});

it.each([new TypeError("fetch failed"), new DOMException("request timed out", "AbortError")])(
  "retries %s and returns the next response",
  async (failure) => {
    vi.useFakeTimers();
    const fetchMock = vi.fn().mockRejectedValueOnce(failure).mockResolvedValueOnce(new Response("ok"));
    vi.stubGlobal("fetch", fetchMock);

    const request = getText("https://example.invalid", { timeoutMs: 1000, retries: 1 });
    await vi.runAllTimersAsync();
    await expect(request).resolves.toBe("ok");
    expect(fetchMock).toHaveBeenCalledTimes(2);
  }
);

it("retries HTTP 408", async () => {
  vi.useFakeTimers();
  const fetchMock = vi
    .fn()
    .mockResolvedValueOnce(new Response(null, { status: 408 }))
    .mockResolvedValueOnce(new Response("ok"));
  vi.stubGlobal("fetch", fetchMock);

  const request = getText("https://example.invalid", { timeoutMs: 1000, retries: 1 });
  await vi.runAllTimersAsync();
  await expect(request).resolves.toBe("ok");
  expect(fetchMock).toHaveBeenCalledTimes(2);
});

it("retries HTML and truncated JSON served with HTTP 200", async () => {
  vi.useFakeTimers();
  const fetchMock = vi
    .fn()
    .mockResolvedValueOnce(new Response("<html>temporary page</html>"))
    .mockResolvedValueOnce(new Response('{"active":'))
    .mockResolvedValueOnce(new Response('{"active":true}'));
  const onRetry = vi.fn();
  vi.stubGlobal("fetch", fetchMock);

  const request = getJson<{ active: boolean }>("https://example.invalid", {
    timeoutMs: 1000,
    retries: 2,
    onRetry
  });
  await vi.runAllTimersAsync();
  await expect(request).resolves.toEqual({ active: true });
  expect(fetchMock).toHaveBeenCalledTimes(3);
  expect(onRetry.mock.calls.map(([, , error]) => error.name)).toEqual(["SyntaxError", "SyntaxError"]);
});

describe("HTTP retry helpers", () => {
  it("parses Retry-After seconds and dates", () => {
    expect(parseRetryAfterMs("2", 1000)).toBe(2000);
    expect(parseRetryAfterMs("Thu, 01 Jan 1970 00:00:05 GMT", 1000)).toBe(4000);
    expect(parseRetryAfterMs("invalid", 1000)).toBeUndefined();
  });

  it("ignores empty Retry-After and bounds large seconds and date values", () => {
    expect(parseRetryAfterMs("")).toBeUndefined();
    expect(parseRetryAfterMs("   ")).toBeUndefined();
    expect(parseRetryAfterMs(" 2 ")).toBe(2000);
    expect(parseRetryAfterMs("-1", 1000)).toBeUndefined();
    expect(parseRetryAfterMs("86400")).toBe(60_000);
    expect(parseRetryAfterMs("Fri, 02 Jan 1970 00:00:00 GMT", 1000)).toBe(60_000);
  });

  it("uses a longer exponential delay for rate limits", () => {
    expect(calculateHttpRetryDelayMs({ statusCode: 429 }, 1, 0)).toBe(1000);
    expect(calculateHttpRetryDelayMs({ statusCode: 429 }, 2, 0)).toBe(2000);
    expect(calculateHttpRetryDelayMs({ statusCode: 429, retryAfterMs: 5000 }, 1, 0)).toBe(5000);
  });

  it.each([{ statusCode: 503 }, new TypeError("fetch failed"), new DOMException("expired", "AbortError")])(
    "uses bounded exponential backoff with jitter for %s",
    (failure) => {
      expect(calculateHttpRetryDelayMs(failure, 1, 0)).toBe(200);
      expect(calculateHttpRetryDelayMs(failure, 2, 0)).toBe(400);
      expect(calculateHttpRetryDelayMs(failure, 3, 0.5)).toBe(925);
      expect(calculateHttpRetryDelayMs(failure, 25, 1)).toBe(30_000);
    }
  );

  it("caps the local 429 backoff and honors the bounded Retry-After", () => {
    expect(calculateHttpRetryDelayMs({ statusCode: 429 }, 25, 1)).toBe(30_000);
    expect(calculateHttpRetryDelayMs({ statusCode: 429, retryAfterMs: 60_000 }, 25, 1)).toBe(60_000);
  });
});
