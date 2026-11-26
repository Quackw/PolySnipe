import { Bot, InlineKeyboard, Keyboard, type Context } from "grammy";
import type { AppConfig, ResolvedAccount, RuntimeNotifierDeps } from "./types.js";
import type { AppRepository } from "./db/repository.js";
import type { PolymarketClient } from "./apis/polymarket.js";
import { parseAccountInput } from "./subscriptionInput.js";
import {
  buildAccountLabel,
  renderAdminMenuPrompt,
  renderAdminPerformanceReport,
  renderAdminProfilesReport,
  renderAdminSubscriptionsReport,
  renderAdminUsersReport,
  renderInvalidSubscribeInput,
  renderStartGuide,
  renderSubscribePrompt,
  renderSubscriptionCreated,
  renderUnsubscribed,
  renderUnsubscribedAll,
  renderUnsubscribePrompt
} from "./services/format.js";
import { replyHtml } from "./utils/telegram.js";
import { escapeHtml } from "./utils/html.js";

const UNSUBSCRIBE_PAGE_SIZE = 5;
type AdminReportKey = "users" | "subscriptions" | "profiles" | "performance";

function buildMainKeyboard(showAdminButton: boolean): Keyboard {
  const keyboard = new Keyboard().text("Subscribe").text("Unsubscribe");
  if (showAdminButton) {
    keyboard.text("Admin");
  }
  return keyboard.resized();
}

function subscribeCancelKeyboard(): InlineKeyboard {
  return new InlineKeyboard().text("Cancel", "subcancel");
}

function adminMenuKeyboard(): InlineKeyboard {
  return new InlineKeyboard()
    .text("Users", "admin:users")
    .text("Subscriptions", "admin:subscriptions")
    .row()
    .text("Profiles", "admin:profiles")
    .text("Performance", "admin:performance");
}

function pageCount(totalItems: number, pageSize: number): number {
  return Math.max(1, Math.ceil(totalItems / pageSize));
}

function clampPage(value: number, pages: number): number {
  return Math.min(Math.max(value, 0), pages - 1);
}

function truncateInlineLabel(value: string, maxLen: number): string {
  return value.length <= maxLen ? value : `${value.slice(0, maxLen - 3)}...`;
}

function renderSubscriptionButtonLabel(subscription: {
  proxyAddress: string;
  name?: string | undefined;
  pseudonym?: string | undefined;
  profileSlug?: string | undefined;
}): string {
  const label = buildAccountLabel(subscription);
  return truncateInlineLabel(label.subtitle ? `${label.title} (${label.subtitle})` : label.title, 55);
}

function renderUnsubscribeKeyboard(
  subscriptions: Array<{
    proxyAddress: string;
    name?: string | undefined;
    pseudonym?: string | undefined;
    profileSlug?: string | undefined;
  }>,
  requestedPage: number
): InlineKeyboard {
  const keyboard = new InlineKeyboard();
  const totalPages = pageCount(subscriptions.length, UNSUBSCRIBE_PAGE_SIZE);
  const page = clampPage(requestedPage, totalPages);
  const offset = page * UNSUBSCRIBE_PAGE_SIZE;
  const pageItems = subscriptions.slice(offset, offset + UNSUBSCRIBE_PAGE_SIZE);

  for (const item of pageItems) {
    keyboard.text(renderSubscriptionButtonLabel(item), `unsel:${item.proxyAddress}`).row();
  }

  if (page > 0) {
    keyboard.text("Prev", `unpg:${page - 1}`);
  }
  if (page + 1 < totalPages) {
    keyboard.text("Next", `unpg:${page + 1}`);
  }
  if (page > 0 || page + 1 < totalPages) {
    keyboard.row();
  }

  keyboard.text("Unsubscribe all", "unall").row().text("Cancel", "uncancel");
  return keyboard;
}

async function ensureSubscriber(
  ctx: Context,
  repo: AppRepository
): Promise<{ subscriberId: number; userId: number; chatId: number } | null> {
  if (!ctx.from || !ctx.chat) {
    return null;
  }

  const subscriberId = repo.upsertSubscriber(ctx.from.id, ctx.chat.id, ctx.chat.type);
  return { subscriberId, userId: ctx.from.id, chatId: ctx.chat.id };
}

function isMessageNotModifiedError(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return message.toLowerCase().includes("message is not modified");
}

async function editMessageHtml(
  ctx: Context,
  html: string,
  extra: Record<string, unknown> = {}
): Promise<void> {
  try {
    await ctx.editMessageText(html, {
      parse_mode: "HTML",
      disable_web_page_preview: true,
      ...extra
    } as never);
  } catch (error) {
    if (isMessageNotModifiedError(error)) {
      return;
    }
    throw error;
  }
}

function profileUrl(account: ResolvedAccount): string {
  return account.profileSlug
    ? `https://polymarket.com/@${encodeURIComponent(account.profileSlug)}`
    : `https://polymarket.com/${account.proxyAddress}`;
}

export function createTelegramBot(
  config: AppConfig,
  repo: AppRepository,
  polymarket: PolymarketClient,
  notifier?: RuntimeNotifierDeps
): Bot {
  const bot = new Bot(config.telegramBotToken, { client: { timeoutSeconds: 45 } });

  function isAdminRequester(ctx: Context): boolean {
    return Boolean(ctx.from && config.adminTelegramUserIds.includes(ctx.from.id));
  }

  function mainKeyboard(ctx: Context): Keyboard {
    return buildMainKeyboard(Boolean(ctx.chat?.type === "private" && isAdminRequester(ctx)));
  }

  async function openAdminMenu(ctx: Context): Promise<void> {
    const ids = await ensureSubscriber(ctx, repo);
    if (!ids) {
      return;
    }

    if (!isAdminRequester(ctx)) {
      await replyHtml(ctx, "Admin only.", {
        reply_markup: mainKeyboard(ctx)
      });
      return;
    }

    await replyHtml(ctx, renderAdminMenuPrompt(), {
      reply_markup: adminMenuKeyboard()
    });
  }

  async function sendAdminReport(ctx: Context, report: AdminReportKey): Promise<void> {
    const ids = await ensureSubscriber(ctx, repo);
    if (!ids) {
      return;
    }

    if (!isAdminRequester(ctx)) {
      await replyHtml(ctx, "Admin only.", {
        reply_markup: mainKeyboard(ctx)
      });
      return;
    }

    if (report === "users") {
      await replyHtml(ctx, renderAdminUsersReport(repo.getAdminUsersReport()));
      return;
    }

    if (report === "subscriptions") {
      await replyHtml(ctx, renderAdminSubscriptionsReport(repo.getAdminSubscriptionsReport()));
      return;
    }

    if (report === "profiles") {
      await replyHtml(ctx, renderAdminProfilesReport(repo.listAdminProfileStats()));
      return;
    }

    await replyHtml(
      ctx,
      renderAdminPerformanceReport(
        repo.getAdminPerformanceReport(Date.now(), config.alertDelayThresholdSec * 1000)
      )
    );
  }

  async function handleSubscribeStart(ctx: Context): Promise<void> {
    const ids = await ensureSubscriber(ctx, repo);
    if (!ids) {
      return;
    }

    repo.setChatSessionAwaiting(ids.chatId, ids.userId, Date.now() + config.subscribeModeTtlMs);
    await replyHtml(ctx, renderSubscribePrompt(), {
      reply_markup: subscribeCancelKeyboard()
    });
  }

  async function handleUnsubscribeMenu(ctx: Context): Promise<void> {
    const ids = await ensureSubscriber(ctx, repo);
    if (!ids) {
      return;
    }

    const subscriptions = repo.listChatSubscriptions(ids.chatId);
    if (subscriptions.length === 0) {
      await replyHtml(ctx, "No active subscriptions in this chat.", {
        reply_markup: mainKeyboard(ctx)
      });
      return;
    }

    await replyHtml(
      ctx,
      renderUnsubscribePrompt(subscriptions, 0, pageCount(subscriptions.length, UNSUBSCRIBE_PAGE_SIZE)),
      {
        reply_markup: renderUnsubscribeKeyboard(subscriptions, 0)
      }
    );
  }

  bot.command("start", async (ctx) => {
    const deskWallet = String(ctx.match ?? "").match(/^desk_(0x[0-9a-fA-F]{40})$/)?.[1];
    if (deskWallet && ctx.chat?.type === "private") {
      await replyHtml(
        ctx,
        `Track <code>${deskWallet}</code>? Future activity will be sent here. This is not a profitability recommendation.`,
        {
          reply_markup: new InlineKeyboard()
            .text("Confirm subscription", `deskadd:${deskWallet}`)
            .text("Cancel", "subcancel")
        }
      );
      return;
    }
    const ids = await ensureSubscriber(ctx, repo);
    if (!ids) {
      return;
    }
    repo.clearChatSession(ids.chatId);
    await replyHtml(ctx, renderStartGuide(), {
      reply_markup: mainKeyboard(ctx)
    });
  });

  bot.command("admin", openAdminMenu);

  bot.callbackQuery(/^deskadd:(0x[0-9a-fA-F]{40})$/, async (ctx) => {
    if (ctx.chat?.type !== "private" || ctx.from.id !== ctx.chat.id) {
      await ctx.answerCallbackQuery();
      return;
    }
    const ids = await ensureSubscriber(ctx, repo);
    if (!ids) return;
    try {
      const account = await polymarket.resolveAccountInput({
        kind: "proxy_address",
        proxyAddress: ctx.match[1]!.toLowerCase()
      });
      const nowMs = Date.now();
      repo.upsertTrackedAccount({
        proxyAddress: account.proxyAddress,
        name: account.name,
        pseudonym: account.pseudonym,
        profileSlug: account.profileSlug,
        nextPollAtMs: nowMs
      });
      const created = repo.createSubscription(
        ids.subscriberId,
        ids.chatId,
        account.proxyAddress,
        ids.userId,
        Math.floor(nowMs / 1000)
      );
      await ctx.answerCallbackQuery();
      await editMessageHtml(ctx, renderSubscriptionCreated(account, created.created), {
        reply_markup: new InlineKeyboard()
      });
    } catch {
      await ctx.answerCallbackQuery({ text: "Could not subscribe. Try again." });
    }
  });

  bot.hears(/^Subscribe$/i, handleSubscribeStart);
  bot.hears(/^Unsubscribe$/i, handleUnsubscribeMenu);
  bot.hears(/^Admin$/i, openAdminMenu);

  bot.callbackQuery(/^subcancel$/, async (ctx) => {
    if (ctx.chat) {
      repo.clearChatSession(ctx.chat.id);
    }
    await ctx.answerCallbackQuery();
    await editMessageHtml(ctx, "Subscribe cancelled.");
  });

  bot.callbackQuery(/^uncancel$/, async (ctx) => {
    await ctx.answerCallbackQuery();
    await editMessageHtml(ctx, "Unsubscribe cancelled.");
  });

  bot.callbackQuery(/^admin:/, async (ctx) => {
    if (!ctx.chat || !ctx.from) {
      return;
    }

    if (!isAdminRequester(ctx)) {
      await ctx.answerCallbackQuery({ text: "Admin only." });
      return;
    }

    const report = ctx.callbackQuery.data.slice("admin:".length) as AdminReportKey;
    if (!["users", "subscriptions", "profiles", "performance"].includes(report)) {
      await ctx.answerCallbackQuery({ text: "Unknown report." });
      return;
    }

    await ctx.answerCallbackQuery();
    await sendAdminReport(ctx, report);
  });

  bot.callbackQuery(/^unpg:/, async (ctx) => {
    if (!ctx.chat) {
      return;
    }

    const rawPage = ctx.callbackQuery.data.slice("unpg:".length);
    const page = Number.parseInt(rawPage, 10);
    if (!Number.isInteger(page)) {
      await ctx.answerCallbackQuery({ text: "Invalid page." });
      return;
    }

    const subscriptions = repo.listChatSubscriptions(ctx.chat.id);
    if (subscriptions.length === 0) {
      await ctx.answerCallbackQuery();
      await editMessageHtml(ctx, "No active subscriptions in this chat.");
      return;
    }

    await ctx.answerCallbackQuery();
    await editMessageHtml(
      ctx,
      renderUnsubscribePrompt(
        subscriptions,
        clampPage(page, pageCount(subscriptions.length, UNSUBSCRIBE_PAGE_SIZE)),
        pageCount(subscriptions.length, UNSUBSCRIBE_PAGE_SIZE)
      ),
      {
        reply_markup: renderUnsubscribeKeyboard(subscriptions, page)
      }
    );
  });

  bot.callbackQuery(/^unsel:/, async (ctx) => {
    if (!ctx.chat) {
      return;
    }

    const proxyAddress = ctx.callbackQuery.data.slice("unsel:".length);
    const subscriptions = repo.listChatSubscriptions(ctx.chat.id);
    const selected = subscriptions.find((item) => item.proxyAddress === proxyAddress);
    const result = repo.removeSubscription(ctx.chat.id, proxyAddress);
    await ctx.answerCallbackQuery();

    if (result.removedSubscriptions <= 0) {
      await editMessageHtml(ctx, "Subscription was already removed.");
      return;
    }

    if (selected) {
      await editMessageHtml(ctx, renderUnsubscribed(selected));
      return;
    }

    await editMessageHtml(ctx, renderUnsubscribed({ proxyAddress }));
  });

  bot.callbackQuery(/^unall$/, async (ctx) => {
    if (!ctx.chat) {
      return;
    }

    const result = repo.removeAllSubscriptions(ctx.chat.id);
    await ctx.answerCallbackQuery();
    await editMessageHtml(
      ctx,
      renderUnsubscribedAll(result.removedSubscriptions, result.removedTrackedAccounts)
    );
  });

  bot.on("message:text", async (ctx) => {
    if (!ctx.chat || !ctx.from) {
      return;
    }

    if (/^(subscribe|unsubscribe|admin)$/i.test(ctx.message.text.trim())) {
      return;
    }

    repo.clearExpiredChatSessions(Date.now());
    const session = repo.getChatSession(ctx.chat.id);
    if (!session || session.mode !== "awaiting_subscribe") {
      return;
    }

    if (session.awaitingUserId && session.awaitingUserId !== ctx.from.id) {
      await replyHtml(
        ctx,
        "Subscribe mode is waiting for the user who started it. Press <b>Subscribe</b> again if you want to replace that request.",
        { reply_markup: mainKeyboard(ctx) }
      );
      return;
    }

    const parsed = parseAccountInput(ctx.message.text);
    if (!parsed) {
      await replyHtml(ctx, renderInvalidSubscribeInput(), {
        reply_markup: subscribeCancelKeyboard()
      });
      return;
    }

    const ids = await ensureSubscriber(ctx, repo);
    if (!ids) {
      return;
    }

    try {
      const account = await polymarket.resolveAccountInput(parsed);
      const nowMs = Date.now();
      const nowSec = Math.floor(nowMs / 1000);

      repo.upsertTrackedAccount({
        proxyAddress: account.proxyAddress,
        name: account.name,
        pseudonym: account.pseudonym,
        profileSlug: account.profileSlug,
        nextPollAtMs: nowMs
      });

      const created = repo.createSubscription(
        ids.subscriberId,
        ids.chatId,
        account.proxyAddress,
        ids.userId,
        nowSec
      );
      repo.clearChatSession(ids.chatId);

      await replyHtml(ctx, renderSubscriptionCreated(account, created.created), {
        reply_markup: mainKeyboard(ctx)
      });
    } catch (error) {
      await replyHtml(
        ctx,
        `Subscription failed: <code>${escapeHtml(error instanceof Error ? error.message : String(error))}</code>`,
        { reply_markup: subscribeCancelKeyboard() }
      );
    }
  });

  bot.catch(async (error) => {
    console.error("[error] Telegram command handler failed");
    await notifier?.notifyError("bot handler", error.error);
  });

  return bot;
}
