import { describe, expect, it, vi } from "vitest";
import { createAdminNotifier, renderAdminErrorNotification } from "../src/services/adminNotifier.js";

describe("renderAdminErrorNotification", () => {
  it("renders source, message, and stack", () => {
    const error = new Error("boom");
    error.stack = "Error: boom\n at test";

    const text = renderAdminErrorNotification("startup", error);

    expect(text).toContain("<b>Fatal bot error</b>");
    expect(text).toContain("Source: <b>startup</b>");
    expect(text).toContain("Message: <code>boom</code>");
    expect(text).toContain("<b>Stack</b>");
  });
});

describe("createAdminNotifier", () => {
  it("does nothing when adminTelegramUserIds are unset", async () => {
    const sendTelegram = vi.fn(async () => undefined);
    const log = vi.fn();
    const notifier = createAdminNotifier({ adminTelegramUserIds: [] }, sendTelegram, log);

    await notifier.notifyError("startup", new Error("boom"));

    expect(sendTelegram).not.toHaveBeenCalled();
    expect(log).not.toHaveBeenCalled();
  });

  it("sends notification to all admin chats", async () => {
    const sendTelegram = vi.fn(async () => undefined);
    const log = vi.fn();
    const notifier = createAdminNotifier({ adminTelegramUserIds: [10, 20] }, sendTelegram, log);

    await notifier.notifyError("startup", new Error("boom"));

    expect(sendTelegram).toHaveBeenCalledTimes(2);
    const firstCall = sendTelegram.mock.calls[0] as unknown[] | undefined;
    const secondCall = sendTelegram.mock.calls[1] as unknown[] | undefined;
    expect(firstCall?.[0]).toBe(10);
    expect(secondCall?.[0]).toBe(20);
    expect(String(firstCall?.[1])).toContain("Fatal bot error");
  });

  it("continues after one admin notification send fails and logs it", async () => {
    const sendTelegram = vi.fn(async (chatId: number) => {
      if (chatId === 10) {
        throw new Error("telegram failed");
      }
    });
    const log = vi.fn();
    const notifier = createAdminNotifier({ adminTelegramUserIds: [10, 20] }, sendTelegram, log);

    await expect(notifier.notifyError("startup", new Error("boom"))).resolves.toBeUndefined();
    expect(sendTelegram).toHaveBeenCalledTimes(2);
    expect(sendTelegram).toHaveBeenNthCalledWith(1, 10, expect.any(String));
    expect(sendTelegram).toHaveBeenNthCalledWith(2, 20, expect.any(String));
    expect(log).toHaveBeenCalledWith(expect.stringContaining("[notify:error] source=startup admin=10 error=telegram failed"));
  });
});
