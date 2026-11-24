import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { loadConfig } from "../src/config.js";

beforeEach(() => {
  vi.stubEnv("TELEGRAM_BOT_TOKEN", "test");
  vi.stubEnv("SQLITE_PATH", ":memory:");
  for (const name of [
    "SCHEDULER_TICK_MS", "HTTP_TIMEOUT_MS", "HTTP_RETRIES",
    "POLYMARKET_TIMEOUT_MS", "POLYMARKET_RETRIES", "TELEGRAM_TIMEOUT_MS"
  ]) vi.stubEnv(name, undefined);
});
afterEach(() => vi.unstubAllEnvs());

it.each([undefined, "", "   "])("uses documented defaults for unset or blank numeric values (%j)", (value) => {
  for (const name of [
    "SCHEDULER_TICK_MS", "HTTP_TIMEOUT_MS", "HTTP_RETRIES",
    "POLYMARKET_TIMEOUT_MS", "POLYMARKET_RETRIES", "TELEGRAM_TIMEOUT_MS"
  ]) vi.stubEnv(name, value);
  expect(loadConfig()).toMatchObject({
    sqlitePath: ":memory:",
    schedulerTickMs: 250,
    httpTimeoutMs: 15_000,
    httpRetries: 2,
    polymarketTimeoutMs: 15_000,
    polymarketRetries: 1,
    telegramTimeoutMs: 15_000
  });
});

it("inherits supplied legacy HTTP values, including zero retries, and accepts explicit overrides", () => {
  vi.stubEnv("HTTP_TIMEOUT_MS", " 2400 ");
  vi.stubEnv("HTTP_RETRIES", " 0 ");
  vi.stubEnv("TELEGRAM_TIMEOUT_MS", " 3600 ");
  vi.stubEnv("POLYMARKET_RETRIES", " ");
  expect(loadConfig()).toMatchObject({
    polymarketTimeoutMs: 2_400, polymarketRetries: 0, httpRetries: 0, telegramTimeoutMs: 3_600
  });
  vi.stubEnv("POLYMARKET_RETRIES", "3");
  expect(loadConfig().polymarketRetries).toBe(3);
});

it.each(["SCHEDULER_TICK_MS", "HTTP_TIMEOUT_MS", "HTTP_RETRIES", "POLYMARKET_RETRIES"])(
  "rejects malformed and out-of-range integers in %s",
  (name) => {
    for (const value of ["10abc", "10.5", "-1", "0x10", "9007199254740992"]) {
      vi.stubEnv(name, value);
      expect(loadConfig).toThrow(name);
    }
  }
);
