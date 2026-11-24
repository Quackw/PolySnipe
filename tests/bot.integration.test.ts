import { afterEach, describe, expect, it, vi } from "vitest";
import type { Bot } from "grammy";
import type { Update } from "grammy/types";
import { createTelegramBot } from "../src/bot.js";
import type { PolymarketClient } from "../src/apis/polymarket.js";
import { createTempRepo, installTelegramApiMock, testConfig, type TelegramCall } from "./helpers.js";

const resources: Array<ReturnType<typeof createTempRepo>> = [];

function setupRepo() {
  const temp = createTempRepo("bot");
  resources.push(temp);
  return temp;
}

function messageUpdate(
  updateId: number,
  chatId: number,
  userId: number,
  text: string,
  chatType: "private" | "group" = "private"
): Update {
  const entities = text.startsWith("/")
    ? [{ offset: 0, length: text.length, type: "bot_command" as const }]
    : undefined;
  return {
    update_id: updateId,
    message: {
      message_id: updateId,
      date: 1,
      chat: { id: chatId, type: chatType },
      from: { id: userId, is_bot: false, first_name: "User" },
      text,
      entities
    }
  } as Update;
}

function keyboardButtons(call: { payload: Record<string, unknown> }): string[][] {
  const replyMarkup = call.payload.reply_markup as { keyboard?: Array<Array<{ text?: string }>> } | undefined;
  return (replyMarkup?.keyboard ?? []).map((row) => row.map((button) => String(button.text ?? "")));
}

function inlineButtons(call: { payload: Record<string, unknown> }): string[][] {
  const replyMarkup = call.payload.reply_markup as
    | { inline_keyboard?: Array<Array<{ text?: string }>> }
    | undefined;
  return (replyMarkup?.inline_keyboard ?? []).map((row) => row.map((button) => String(button.text ?? "")));
}

function sendMessageCalls(calls: TelegramCall[]): TelegramCall[] {
  return calls.filter((call) => call.method === "sendMessage");
}

function callbackUpdate(updateId: number, chatId: number, userId: number, data: string): Update {
  return {
    update_id: updateId,
    callback_query: {
      id: `cb-${updateId}`,
      from: { id: userId, is_bot: false, first_name: "User" },
      chat_instance: "instance",
      data,
      message: {
        message_id: updateId,
        date: 1,
        chat: { id: chatId, type: "private" },
        text: "chooser"
      }
    }
  } as Update;
}

function createMockClient(): PolymarketClient {
  return {
    async resolveAccountInput(input) {
      if (input.kind === "profile_slug") {
        return {
          proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
          profileSlug: input.slug,
          pseudonym: "Admired-Teacher"
        };
      }

      return {
        proxyAddress: input.proxyAddress,
        pseudonym: "Admired-Teacher"
      };
    },
    async fetchRecentUserActivity() {
      return { activities: [], apiCalls: 1, retries: 0, throttleWaitMs: 0 };
    }
  };
}

afterEach(() => {
  while (resources.length > 0) {
    resources.pop()?.cleanup();
  }
});

describe("bot flow", () => {
  it("shows only Subscribe and Unsubscribe for non-admin private chats", async () => {
    const { repo, dbPath } = setupRepo();
    const bot = createTelegramBot(
      testConfig(dbPath, { adminTelegramUserIds: [99] }),
      repo,
      createMockClient()
    );
    bot.botInfo = {
      id: 999,
      is_bot: true,
      first_name: "Bot",
      username: "test_bot",
      can_join_groups: true,
      can_read_all_group_messages: false,
      supports_inline_queries: false
    } as never;

    const calls = installTelegramApiMock(bot as Bot);

    await bot.handleUpdate(messageUpdate(1, 123, 10, "/start"));

    expect(keyboardButtons(calls[0]!)).toEqual([["Subscribe", "Unsubscribe"]]);
  });

  it("shows Admin for admins in private chats", async () => {
    const { repo, dbPath } = setupRepo();
    const bot = createTelegramBot(
      testConfig(dbPath, { adminTelegramUserIds: [10, 20] }),
      repo,
      createMockClient()
    );
    bot.botInfo = {
      id: 999,
      is_bot: true,
      first_name: "Bot",
      username: "test_bot",
      can_join_groups: true,
      can_read_all_group_messages: false,
      supports_inline_queries: false
    } as never;

    const calls = installTelegramApiMock(bot as Bot);

    await bot.handleUpdate(messageUpdate(1, 123, 10, "/start"));

    expect(keyboardButtons(calls[0]!)).toEqual([["Subscribe", "Unsubscribe", "Admin"]]);
  });

  it("keeps the two-button keyboard in group chats even for admins", async () => {
    const { repo, dbPath } = setupRepo();
    const bot = createTelegramBot(
      testConfig(dbPath, { adminTelegramUserIds: [10, 20] }),
      repo,
      createMockClient()
    );
    bot.botInfo = {
      id: 999,
      is_bot: true,
      first_name: "Bot",
      username: "test_bot",
      can_join_groups: true,
      can_read_all_group_messages: false,
      supports_inline_queries: false
    } as never;

    const calls = installTelegramApiMock(bot as Bot);

    await bot.handleUpdate(messageUpdate(1, -123, 10, "/start", "group"));

    expect(keyboardButtons(calls[0]!)).toEqual([["Subscribe", "Unsubscribe"]]);
  });

  it("subscribes after pressing Subscribe and receiving a profile URL", async () => {
    const { repo, dbPath } = setupRepo();
    const bot = createTelegramBot(testConfig(dbPath), repo, createMockClient());
    bot.botInfo = {
      id: 999,
      is_bot: true,
      first_name: "Bot",
      username: "test_bot",
      can_join_groups: true,
      can_read_all_group_messages: false,
      supports_inline_queries: false
    } as never;

    const calls = installTelegramApiMock(bot as Bot);

    await bot.handleUpdate(messageUpdate(1, 123, 10, "/start"));
    await bot.handleUpdate(messageUpdate(2, 123, 10, "Subscribe"));
    await bot.handleUpdate(messageUpdate(3, 123, 10, "https://polymarket.com/@weflyhigh?tab=positions"));

    const subscriptions = repo.listChatSubscriptions(123);
    expect(subscriptions).toHaveLength(1);
    expect(subscriptions[0]?.proxyAddress).toBe("0x03e8a544e97eeff5753bc1e90d46e5ef22af1697");
    const subscribeMessage = calls.find((call) => String(call.payload.text).includes("Subscribed"));
    expect(subscribeMessage).toBeTruthy();
    expect(String(subscribeMessage?.payload.text)).toContain(
      '<b>Subscribed to</b> <a href="https://polymarket.com/@weflyhigh">@weflyhigh</a>'
    );
    expect(String(subscribeMessage?.payload.text)).not.toContain("Admired-Teacher");
    expect(String(subscribeMessage?.payload.text)).not.toContain("Proxy address");
    expect(String(subscribeMessage?.payload.text)).not.toContain("Profile:");
  });

  it("shows paginated unsubscribe buttons and removes the tracked account on last unsubscribe", async () => {
    const { repo, dbPath } = setupRepo();
    const subscriberId = repo.upsertSubscriber(10, 123, "private");
    for (let idx = 0; idx < 6; idx += 1) {
      const proxyAddress = `0x${String(idx).padStart(40, "1")}`;
      repo.upsertTrackedAccount({ proxyAddress, profileSlug: `slug-${idx}`, nextPollAtMs: 0 });
      repo.createSubscription(subscriberId, 123, proxyAddress, 10, 1000);
    }

    const bot = createTelegramBot(testConfig(dbPath), repo, createMockClient());
    bot.botInfo = {
      id: 999,
      is_bot: true,
      first_name: "Bot",
      username: "test_bot",
      can_join_groups: true,
      can_read_all_group_messages: false,
      supports_inline_queries: false
    } as never;

    const calls = installTelegramApiMock(bot as Bot);

    await bot.handleUpdate(messageUpdate(1, 123, 10, "Unsubscribe"));
    await bot.handleUpdate(callbackUpdate(2, 123, 10, "unpg:1"));
    await bot.handleUpdate(callbackUpdate(3, 123, 10, "unsel:0x1111111111111111111111111111111111111115"));

    expect(repo.getTrackedAccount("0x1111111111111111111111111111111111111115")).toBeNull();
    const unsubscribeMessage = calls.find(
      (call) => call.method === "editMessageText" && String(call.payload.text).includes("Unsubscribed from")
    );
    expect(unsubscribeMessage).toBeTruthy();
    expect(String(unsubscribeMessage?.payload.text)).toContain(
      '<a href="https://polymarket.com/@slug-5">@slug-5</a>'
    );
    expect(String(unsubscribeMessage?.payload.text)).not.toContain("Tracked account removed");
  });

  it("denies unauthorized /admin access", async () => {
    const { repo, dbPath } = setupRepo();
    const bot = createTelegramBot(
      testConfig(dbPath, { adminTelegramUserIds: [999, 555] }),
      repo,
      createMockClient()
    );
    bot.botInfo = {
      id: 999,
      is_bot: true,
      first_name: "Bot",
      username: "test_bot",
      can_join_groups: true,
      can_read_all_group_messages: false,
      supports_inline_queries: false
    } as never;

    const calls = installTelegramApiMock(bot as Bot);

    await bot.handleUpdate(messageUpdate(1, 123, 10, "/admin"));

    expect(calls.some((call) => String(call.payload.text).includes("Admin only."))).toBe(true);
  });

  it("denies unauthorized Admin button access", async () => {
    const { repo, dbPath } = setupRepo();
    const bot = createTelegramBot(
      testConfig(dbPath, { adminTelegramUserIds: [999, 555] }),
      repo,
      createMockClient()
    );
    bot.botInfo = {
      id: 999,
      is_bot: true,
      first_name: "Bot",
      username: "test_bot",
      can_join_groups: true,
      can_read_all_group_messages: false,
      supports_inline_queries: false
    } as never;

    const calls = installTelegramApiMock(bot as Bot);

    await bot.handleUpdate(messageUpdate(1, 123, 10, "Admin"));

    expect(calls.some((call) => String(call.payload.text).includes("Admin only."))).toBe(true);
  });

  it("opens the admin submenu for authorized /admin", async () => {
    const { repo, dbPath } = setupRepo();
    const bot = createTelegramBot(
      testConfig(dbPath, { adminTelegramUserIds: [10, 20] }),
      repo,
      createMockClient()
    );
    bot.botInfo = {
      id: 999,
      is_bot: true,
      first_name: "Bot",
      username: "test_bot",
      can_join_groups: true,
      can_read_all_group_messages: false,
      supports_inline_queries: false
    } as never;

    const calls = installTelegramApiMock(bot as Bot);

    await bot.handleUpdate(messageUpdate(1, 123, 10, "/admin"));

    const adminMessage = calls.find((call) => String(call.payload.text).includes("<b>Admin reports</b>"));
    expect(String(adminMessage?.payload.text ?? "")).toContain("Choose a report:");
    expect(inlineButtons(adminMessage!)).toEqual([
      ["Users", "Subscriptions"],
      ["Profiles", "Performance"]
    ]);
  });

  it("opens the same admin submenu when an authorized admin presses Admin", async () => {
    const { repo, dbPath } = setupRepo();
    const bot = createTelegramBot(
      testConfig(dbPath, { adminTelegramUserIds: [10, 20] }),
      repo,
      createMockClient()
    );
    bot.botInfo = {
      id: 999,
      is_bot: true,
      first_name: "Bot",
      username: "test_bot",
      can_join_groups: true,
      can_read_all_group_messages: false,
      supports_inline_queries: false
    } as never;

    const calls = installTelegramApiMock(bot as Bot);

    await bot.handleUpdate(messageUpdate(1, 123, 10, "Admin"));

    const adminMessage = calls.find((call) => String(call.payload.text).includes("<b>Admin reports</b>"));
    expect(String(adminMessage?.payload.text ?? "")).toContain("Choose a report:");
    expect(inlineButtons(adminMessage!)).toEqual([
      ["Users", "Subscriptions"],
      ["Profiles", "Performance"]
    ]);
  });

  it("renders the users report for an authorized admin callback", async () => {
    const { repo, dbPath } = setupRepo();
    const subscriberA = repo.upsertSubscriber(10, 123, "private");
    const subscriberB = repo.upsertSubscriber(20, 789, "private");
    repo.upsertSubscriber(30, 456, "private");
    repo.upsertTrackedAccount({
      proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
      nextPollAtMs: 0
    });
    repo.upsertTrackedAccount({
      proxyAddress: "0x1111111111111111111111111111111111111111",
      nextPollAtMs: 0
    });
    repo.createSubscription(subscriberA, 123, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 10, 1000);
    repo.createSubscription(subscriberB, 789, "0x1111111111111111111111111111111111111111", 20, 1000);

    const bot = createTelegramBot(
      testConfig(dbPath, { adminTelegramUserIds: [10, 20] }),
      repo,
      createMockClient()
    );
    bot.botInfo = {
      id: 999,
      is_bot: true,
      first_name: "Bot",
      username: "test_bot",
      can_join_groups: true,
      can_read_all_group_messages: false,
      supports_inline_queries: false
    } as never;

    const calls = installTelegramApiMock(bot as Bot);

    await bot.handleUpdate(callbackUpdate(1, 123, 20, "admin:users"));

    const usersMessage = calls.find((call) => String(call.payload.text).includes("<b>Users</b>"));
    expect(String(usersMessage?.payload.text ?? "")).toContain("Users: <b>2</b>");
    expect(String(usersMessage?.payload.text ?? "")).toContain("Last user joined at:");
  });

  it("renders the subscriptions report for an authorized admin callback", async () => {
    const { repo, dbPath } = setupRepo();
    const nowMs = Date.UTC(2026, 2, 27, 12, 0, 0);
    const subscriberA = repo.upsertSubscriber(10, 123, "private");
    const subscriberB = repo.upsertSubscriber(20, 456, "private");
    repo.upsertTrackedAccount({
      proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
      nextPollAtMs: 0
    });
    repo.upsertTrackedAccount({
      proxyAddress: "0x1111111111111111111111111111111111111111",
      nextPollAtMs: 0
    });
    repo.createSubscription(subscriberA, 123, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 10, 1000);
    repo.createSubscription(subscriberA, 123, "0x1111111111111111111111111111111111111111", 10, 1000);
    repo.createSubscription(subscriberB, 456, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 20, 1000);
    repo.insertAlertDelaySamples([
      {
        telegramChatId: 123,
        proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
        activityTimestamp: Math.floor((nowMs - 10_000) / 1000),
        sendStartedAtMs: nowMs - 3_000,
        delayMs: 3_000
      }
    ]);

    const bot = createTelegramBot(
      testConfig(dbPath, { adminTelegramUserIds: [10, 20] }),
      repo,
      createMockClient()
    );
    bot.botInfo = {
      id: 999,
      is_bot: true,
      first_name: "Bot",
      username: "test_bot",
      can_join_groups: true,
      can_read_all_group_messages: false,
      supports_inline_queries: false
    } as never;

    const calls = installTelegramApiMock(bot as Bot);

    await bot.handleUpdate(callbackUpdate(1, 123, 10, "admin:subscriptions"));

    const subscriptionsMessage = calls.find((call) =>
      String(call.payload.text).includes("<b>Subscriptions</b>")
    );
    expect(String(subscriptionsMessage?.payload.text ?? "")).toContain("Subscriptions: <b>3</b>");
    expect(String(subscriptionsMessage?.payload.text ?? "")).toContain("Avg per user: <b>1.5</b>");
    expect(String(subscriptionsMessage?.payload.text ?? "")).toContain("Last subscription created at:");
    expect(String(subscriptionsMessage?.payload.text ?? "")).toContain("Last alert sent at:");
  });

  it("renders the profiles report for an authorized admin callback", async () => {
    const { repo, dbPath } = setupRepo();
    const subscriberA = repo.upsertSubscriber(10, 123, "private");
    const subscriberB = repo.upsertSubscriber(20, 456, "private");
    repo.upsertTrackedAccount({
      proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
      nextPollAtMs: 0
    });
    repo.upsertTrackedAccount({
      proxyAddress: "0x1111111111111111111111111111111111111111",
      nextPollAtMs: 0
    });
    repo.createSubscription(subscriberA, 123, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 10, 1000);
    repo.createSubscription(subscriberB, 456, "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697", 20, 1000);
    repo.createSubscription(subscriberB, 456, "0x1111111111111111111111111111111111111111", 20, 1000);

    const bot = createTelegramBot(
      testConfig(dbPath, { adminTelegramUserIds: [10, 20] }),
      repo,
      createMockClient()
    );
    bot.botInfo = {
      id: 999,
      is_bot: true,
      first_name: "Bot",
      username: "test_bot",
      can_join_groups: true,
      can_read_all_group_messages: false,
      supports_inline_queries: false
    } as never;

    const calls = installTelegramApiMock(bot as Bot);

    await bot.handleUpdate(callbackUpdate(1, 123, 10, "admin:profiles"));

    const profilesMessage = calls.find((call) => String(call.payload.text).includes("<b>Profiles</b>"));
    expect(String(profilesMessage?.payload.text ?? "")).toContain(
      "https://polymarket.com/0x03e8a544e97eeff5753bc1e90d46e5ef22af1697"
    );
    expect(String(profilesMessage?.payload.text ?? "")).toContain(
      "https://polymarket.com/0x1111111111111111111111111111111111111111"
    );
    expect(String(profilesMessage?.payload.text ?? "")).toContain("<b>2</b>");
  });

  it("renders the performance report for an authorized admin callback", async () => {
    const { repo, dbPath } = setupRepo();
    const nowMs = Date.now();
    repo.insertAlertDelaySamples([
      {
        telegramChatId: 123,
        proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
        activityTimestamp: Math.floor((nowMs - 10_000) / 1000),
        pollStartedAtMs: nowMs - 9_000,
        fetchCompletedAtMs: nowMs - 8_000,
        sendStartedAtMs: nowMs,
        sendCompletedAtMs: nowMs + 1_000,
        delayMs: 10_000
      },
      {
        telegramChatId: 123,
        proxyAddress: "0x03e8a544e97eeff5753bc1e90d46e5ef22af1697",
        activityTimestamp: Math.floor((nowMs - 35_000) / 1000),
        pollStartedAtMs: nowMs - 34_000,
        fetchCompletedAtMs: nowMs - 32_000,
        sendStartedAtMs: nowMs - 31_000,
        sendCompletedAtMs: nowMs - 1_000,
        delayMs: 31_000,
        delayReasonCode: "telegram_slow"
      }
    ]);

    const bot = createTelegramBot(
      testConfig(dbPath, { adminTelegramUserIds: [10, 20] }),
      repo,
      createMockClient()
    );
    bot.botInfo = {
      id: 999,
      is_bot: true,
      first_name: "Bot",
      username: "test_bot",
      can_join_groups: true,
      can_read_all_group_messages: false,
      supports_inline_queries: false
    } as never;

    const calls = installTelegramApiMock(bot as Bot);

    await bot.handleUpdate(callbackUpdate(1, 123, 10, "admin:performance"));

    const performanceMessage = calls.find((call) => String(call.payload.text).includes("<b>Performance</b>"));
    expect(String(performanceMessage?.payload.text ?? "")).toContain("Min delay: <b>10s</b>");
    expect(String(performanceMessage?.payload.text ?? "")).toContain("Avg delay: <b>20.5s</b>");
    expect(String(performanceMessage?.payload.text ?? "")).toContain("Max delay: <b>31s</b>");
    expect(String(performanceMessage?.payload.text ?? "")).toContain("Fresh local max: <b>33s</b>");
    expect(String(performanceMessage?.payload.text ?? "")).toContain("Delay buckets:");
    expect(String(performanceMessage?.payload.text ?? "")).toContain("- 10-30s: <b>1</b>");
    expect(String(performanceMessage?.payload.text ?? "")).toContain("- 30-60s: <b>1</b>");
    expect(String(performanceMessage?.payload.text ?? "")).toContain("Delay reasons over 30s:");
    expect(String(performanceMessage?.payload.text ?? "")).toContain("Classified delayed alerts: <b>1</b>");
    expect(String(performanceMessage?.payload.text ?? "")).toContain("Telegram Slow: <b>1</b>");
  });

  it("does not expose callback reports to unauthorized users", async () => {
    const { repo, dbPath } = setupRepo();
    const bot = createTelegramBot(
      testConfig(dbPath, { adminTelegramUserIds: [999, 555] }),
      repo,
      createMockClient()
    );
    bot.botInfo = {
      id: 999,
      is_bot: true,
      first_name: "Bot",
      username: "test_bot",
      can_join_groups: true,
      can_read_all_group_messages: false,
      supports_inline_queries: false
    } as never;

    const calls = installTelegramApiMock(bot as Bot);

    await bot.handleUpdate(callbackUpdate(1, 123, 10, "admin:users"));

    expect(
      calls.some(
        (call) => call.method === "answerCallbackQuery" && String(call.payload.text).includes("Admin only.")
      )
    ).toBe(true);
    expect(calls.some((call) => String(call.payload.text).includes("<b>Users</b>"))).toBe(false);
  });

  it("splits large profiles output into multiple Telegram messages", async () => {
    const { repo, dbPath } = setupRepo();
    const subscriberId = repo.upsertSubscriber(10, 123, "private");
    for (let idx = 0; idx < 120; idx += 1) {
      const proxyAddress = `0x${idx.toString(16).padStart(40, "1")}`;
      repo.upsertTrackedAccount({ proxyAddress, nextPollAtMs: 0 });
      repo.createSubscription(subscriberId, 123, proxyAddress, 10, 1000);
    }

    const bot = createTelegramBot(
      testConfig(dbPath, { adminTelegramUserIds: [10] }),
      repo,
      createMockClient()
    );
    bot.botInfo = {
      id: 999,
      is_bot: true,
      first_name: "Bot",
      username: "test_bot",
      can_join_groups: true,
      can_read_all_group_messages: false,
      supports_inline_queries: false
    } as never;

    const calls = installTelegramApiMock(bot as Bot);

    await bot.handleUpdate(callbackUpdate(1, 123, 10, "admin:profiles"));

    const profileMessages = sendMessageCalls(calls).filter((call) =>
      String(call.payload.text).includes("https://polymarket.com/")
    );
    expect(profileMessages.length).toBeGreaterThan(1);
  });

  it("routes uncaught bot handler errors to the notifier", async () => {
    const { repo, dbPath } = setupRepo();
    const notifyError = vi.fn(async () => undefined);
    const bot = createTelegramBot(
      testConfig(dbPath),
      repo,
      {
        async resolveAccountInput() {
          throw new Error("resolve failed");
        },
        async fetchRecentUserActivity() {
          return { activities: [], apiCalls: 1, retries: 0, throttleWaitMs: 0 };
        }
      },
      { notifyError }
    );
    bot.botInfo = {
      id: 999,
      is_bot: true,
      first_name: "Bot",
      username: "test_bot",
      can_join_groups: true,
      can_read_all_group_messages: false,
      supports_inline_queries: false
    } as never;

    await (bot as Bot & { errorHandler: (error: unknown) => Promise<void> }).errorHandler({
      error: new Error("send failed")
    });
    expect(notifyError).toHaveBeenCalledWith("bot handler", expect.any(Error));
  });
});
