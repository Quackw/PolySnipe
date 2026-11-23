import { describe, expect, it, vi } from "vitest";
import { notifyFatalError } from "../src/services/runtimeErrors.js";

describe("notifyFatalError", () => {
  it("invokes the notifier with the given source and error", async () => {
    const notifyError = vi.fn(async () => undefined);
    const log = vi.fn();

    await notifyFatalError({ notifyError }, "startup", new Error("boom"), log);

    expect(notifyError).toHaveBeenCalledWith("startup", expect.any(Error));
    expect(log).not.toHaveBeenCalled();
  });

  it("swallows notifier failures and logs them", async () => {
    const notifyError = vi.fn(async () => {
      throw new Error("notify failed");
    });
    const log = vi.fn();

    await expect(notifyFatalError({ notifyError }, "startup", new Error("boom"), log)).resolves.toBeUndefined();
    expect(log).toHaveBeenCalledWith(expect.stringContaining("[notify:fatal]"));
  });
});
