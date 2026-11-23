import { afterEach, expect, it, vi } from "vitest";
import { pollingHealth } from "../src/services/pollingHealth.js";
afterEach(() => vi.useRealTimers());
it("trips once after stalled polling", () => {
  vi.useFakeTimers(); const fail = vi.fn(); const health = pollingHealth(fail);
  vi.advanceTimersByTime(179_000); expect(fail).not.toHaveBeenCalled();
  vi.advanceTimersByTime(21_000); expect(fail).toHaveBeenCalledTimes(1); health.stop();
});
it("successful empty polling keeps an idle bot healthy", () => {
  vi.useFakeTimers(); const fail = vi.fn(); const health = pollingHealth(fail);
  for (let i = 0; i < 100; i++) { vi.advanceTimersByTime(25_000); health.success(); }
  expect(fail).not.toHaveBeenCalled(); health.stop();
});
it("stopping disables health checks", () => {
  vi.useFakeTimers(); const fail = vi.fn(); const health = pollingHealth(fail);
  health.stop(); vi.advanceTimersByTime(300_000); expect(fail).not.toHaveBeenCalled();
});
