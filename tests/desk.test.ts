import { it, expect } from "vitest";
import { createTelegramBot } from "../src/bot.js";
import { createTempRepo, installTelegramApiMock, testConfig } from "./helpers.js";
import type { PolymarketClient } from "../src/apis/polymarket.js";
import type { Update } from "grammy/types";
it("Desk start requires confirmation; foreign click rejected; own confirmation is idempotent", async () => {
  const temp = createTempRepo("desk");
  try {
    const client = {
      async resolveAccountInput(input: { proxyAddress: string }) {
        return { proxyAddress: input.proxyAddress };
      }
    } as unknown as PolymarketClient;
    const bot = createTelegramBot(testConfig(temp.dbPath), temp.repo, client);
    bot.botInfo = { id: 999, is_bot: true, first_name: "Bot", username: "test_bot" } as never;
    installTelegramApiMock(bot);
    const wallet = "0x" + "a".repeat(40);
    await bot.handleUpdate({
      update_id: 1,
      message: {
        message_id: 1,
        date: 1,
        chat: { id: 10, type: "private" },
        from: { id: 10, is_bot: false, first_name: "User" },
        text: `/start desk_${wallet}`,
        entities: [{ offset: 0, length: 6, type: "bot_command" }]
      }
    } as Update);
    expect(temp.repo.listSubscriptionsForAccount(wallet)).toHaveLength(0);
    const callback = (id: number, user: number) =>
      ({
        update_id: id,
        callback_query: {
          id: String(id),
          from: { id: user, is_bot: false, first_name: "User" },
          chat_instance: "test",
          data: `deskadd:${wallet}`,
          message: { message_id: 1, date: 1, chat: { id: 10, type: "private" }, text: "Confirm" }
        }
      }) as Update;
    await bot.handleUpdate(callback(2, 20));
    expect(temp.repo.listSubscriptionsForAccount(wallet)).toHaveLength(0);
    await bot.handleUpdate(callback(3, 10));
    await bot.handleUpdate(callback(4, 10));
    const subs = temp.repo.listSubscriptionsForAccount(wallet);
    expect(subs).toHaveLength(1);
    expect(subs[0]?.telegramChatId).toBe(10);
  } finally {
    temp.cleanup();
  }
});
