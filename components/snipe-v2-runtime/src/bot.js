import { Bot, InlineKeyboard, Keyboard } from "grammy";
import { parseAccountInput } from "./subscriptionInput.js";
import { buildAccountLabel, renderAdminMenuPrompt, renderAdminPerformanceReport, renderAdminProfilesReport, renderAdminSubscriptionsReport, renderAdminUsersReport, renderInvalidSubscribeInput, renderStartGuide, renderSubscribePrompt, renderSubscriptionCreated, renderUnsubscribed, renderUnsubscribedAll, renderUnsubscribePrompt } from "./services/format.js";
import { replyHtml } from "./utils/telegram.js";
import { escapeHtml } from "./utils/html.js";
import { isAllowedPrivateUpdate } from "./privateAccess.js";
const UNSUBSCRIBE_PAGE_SIZE = 5;
function buildMainKeyboard(showAdminButton, showAccessButton) {
    const keyboard = new Keyboard().text("Subscribe").text("Unsubscribe");
    if (showAdminButton) {
        keyboard.text("Admin");
    }
    if (showAccessButton) {
        keyboard.text("Access");
    }
    return keyboard.resized();
}
function subscribeCancelKeyboard() {
    return new InlineKeyboard().text("Cancel", "subcancel");
}
function adminMenuKeyboard() {
    return new InlineKeyboard()
        .text("Users", "admin:users")
        .text("Subscriptions", "admin:subscriptions")
        .row()
        .text("Profiles", "admin:profiles")
        .text("Performance", "admin:performance");
}
function pageCount(totalItems, pageSize) {
    return Math.max(1, Math.ceil(totalItems / pageSize));
}
function clampPage(value, pages) {
    return Math.min(Math.max(value, 0), pages - 1);
}
function truncateInlineLabel(value, maxLen) {
    return value.length <= maxLen ? value : `${value.slice(0, maxLen - 3)}...`;
}
function renderSubscriptionButtonLabel(subscription) {
    const label = buildAccountLabel(subscription);
    return truncateInlineLabel(label.subtitle ? `${label.title} (${label.subtitle})` : label.title, 55);
}
function renderUnsubscribeKeyboard(subscriptions, requestedPage) {
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
async function ensureSubscriber(ctx, repo) {
    if (!ctx.from || !ctx.chat) {
        return null;
    }
    const subscriberId = repo.upsertSubscriber(ctx.from.id, ctx.chat.id, ctx.chat.type);
    return { subscriberId, userId: ctx.from.id, chatId: ctx.chat.id };
}
function isMessageNotModifiedError(error) {
    const message = error instanceof Error ? error.message : String(error);
    return message.toLowerCase().includes("message is not modified");
}
async function editMessageHtml(ctx, html, extra = {}) {
    try {
        await ctx.editMessageText(html, {
            parse_mode: "HTML",
            disable_web_page_preview: true,
            ...extra
        });
    }
    catch (error) {
        if (isMessageNotModifiedError(error)) {
            return;
        }
        throw error;
    }
}
function profileUrl(account) {
    return account.profileSlug
        ? `https://polymarket.com/@${encodeURIComponent(account.profileSlug)}`
        : `https://polymarket.com/${account.proxyAddress}`;
}
export function createTelegramBot(config, repo, polymarket, notifier, onStateChanged) {
    const bot = new Bot(config.telegramBotToken, { client: { timeoutSeconds: 45 } });
    const isAllowed = (ctx) => isAllowedPrivateUpdate(ctx, config.allowedTelegramUserIds, (id) => repo.hasAccessGrant(id));
    const isOwner = (ctx) => Boolean(ctx.from && ctx.chat?.type === "private" && ctx.chat.id === ctx.from.id
        && config.ownerTelegramUserIds.includes(ctx.from.id));
    // A stranger can request access only via /start. Every other update stays closed.
    bot.command("start", async (ctx, next) => {
        if (ctx.chat?.type !== "private" || !ctx.from || ctx.chat.id !== ctx.from.id)
            return;
        if (isAllowed(ctx))
            return next();
        const status = repo.requestAccess(ctx.from.id);
        await replyHtml(ctx, status === "created" || status === "pending"
            ? "Access request received. Wait for approval."
            : "Access is not available. Contact the owner.");
        if (status === "created") {
            const name = [ctx.from.first_name, ctx.from.last_name].filter(Boolean).join(" ");
            const handle = ctx.from.username ? `@${ctx.from.username}` : "not set";
            for (const ownerId of config.ownerTelegramUserIds) {
                try {
                    await bot.api.sendMessage(ownerId, `<b>Holy Snipe V2 · access request</b>\n${escapeHtml(name)} · ${escapeHtml(handle)}\nUser ID: <code>${ctx.from.id}</code>`, {
                        parse_mode: "HTML",
                        reply_markup: new InlineKeyboard()
                            .text("Approve", `access:approve:${ctx.from.id}`)
                            .text("Deny", `access:deny:${ctx.from.id}`)
                    });
                }
                catch {
                    console.error("[access] owner notification failed; request remains pending");
                }
            }
        }
    });
    bot.callbackQuery(/^access:(approve|deny|revoke|allow):(\d+)$/, async (ctx) => {
        if (!isOwner(ctx))
            return;
        const action = ctx.match[1];
        const targetId = Number(ctx.match[2]);
        if (!Number.isSafeInteger(targetId) || targetId <= 0 || config.ownerTelegramUserIds.includes(targetId)) {
            await ctx.answerCallbackQuery({ text: "Invalid user" });
            return;
        }
        const changed = action === "approve" ? repo.approvePendingAccess(targetId)
            : action === "allow" ? repo.approveAccess(targetId)
                : action === "deny" ? repo.denyAccess(targetId) : repo.revokeAccess(targetId);
        if (changed)
            onStateChanged?.();
        await ctx.answerCallbackQuery({ text: changed ? "Done" : "Already handled" });
        if (!changed)
            return;
        const decision = action === "approve" || action === "allow" ? "approved" : action === "deny" ? "denied" : "revoked";
        await editMessageHtml(ctx, `Access ${decision} for <code>${targetId}</code>.`);
        for (const ownerId of config.ownerTelegramUserIds) {
            if (ownerId === ctx.from.id)
                continue;
            try {
                await bot.api.sendMessage(ownerId, `Access ${decision} for user ${targetId} by another owner.`);
            }
            catch {
                console.error("[access] co-owner decision notification failed");
            }
        }
        if (action === "approve" || action === "allow" || action === "deny") {
            try {
                await bot.api.sendMessage(targetId, action === "approve" || action === "allow"
                    ? "Access approved. Press /start to use Holy Snipe V2."
                    : "Access request declined.");
            }
            catch {
                console.error("[access] decision notification failed");
            }
        }
    });
    // Reject every other command, callback and message before it can touch subscriptions.
    bot.use(async (ctx, next) => {
        if (!isAllowed(ctx))
            return;
        await next();
    });
    function isAdminRequester(ctx) {
        return Boolean(ctx.from && config.adminTelegramUserIds.includes(ctx.from.id));
    }
    function mainKeyboard(ctx) {
        return buildMainKeyboard(Boolean(ctx.chat?.type === "private" && isAdminRequester(ctx)), isOwner(ctx));
    }
    async function openAccessMenu(ctx) {
        if (!isOwner(ctx))
            return;
        const pending = repo.listPendingAccessRequests();
        const granted = repo.listAccessGrants();
        const blocked = repo.listBlockedAccessRequests();
        const keyboard = new InlineKeyboard();
        for (const id of pending) {
            keyboard.text(`Approve ${id}`, `access:approve:${id}`)
                .text("Deny", `access:deny:${id}`).row();
        }
        for (const id of granted) {
            keyboard.text(`Revoke ${id}`, `access:revoke:${id}`).row();
        }
        for (const id of blocked) {
            keyboard.text(`Allow again ${id}`, `access:allow:${id}`).row();
        }
        await replyHtml(ctx, `<b>Access</b>\nPending: ${pending.length}\nApproved: ${granted.length}\nDenied/revoked: ${blocked.length}\nOnly configured owners can approve or revoke.`, {
            reply_markup: keyboard
        });
    }
    async function openAdminMenu(ctx) {
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
    async function sendAdminReport(ctx, report) {
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
        await replyHtml(ctx, renderAdminPerformanceReport(repo.getAdminPerformanceReport(Date.now(), config.alertDelayThresholdSec * 1000)));
    }
    async function handleSubscribeStart(ctx) {
        const ids = await ensureSubscriber(ctx, repo);
        if (!ids) {
            return;
        }
        repo.setChatSessionAwaiting(ids.chatId, ids.userId, Date.now() + config.subscribeModeTtlMs);
        await replyHtml(ctx, renderSubscribePrompt(), {
            reply_markup: subscribeCancelKeyboard()
        });
    }
    async function handleUnsubscribeMenu(ctx) {
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
        await replyHtml(ctx, renderUnsubscribePrompt(subscriptions, 0, pageCount(subscriptions.length, UNSUBSCRIBE_PAGE_SIZE)), {
            reply_markup: renderUnsubscribeKeyboard(subscriptions, 0)
        });
    }
    bot.command("start", async (ctx) => {
        const deskWallet = String(ctx.match ?? '').match(/^desk_(0x[0-9a-fA-F]{40})$/)?.[1];
        if (deskWallet && ctx.chat?.type === 'private') {
            await replyHtml(ctx, `Track <code>${deskWallet}</code>? Future activity will be sent here. This is not a profitability recommendation.`, {
                reply_markup: new InlineKeyboard().text('Confirm subscription', `deskadd:${deskWallet}`).text('Cancel', 'subcancel')
            });
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
    bot.command("access", openAccessMenu);
    bot.callbackQuery(/^deskadd:(0x[0-9a-fA-F]{40})$/, async (ctx) => {
        if (ctx.chat?.type !== 'private' || ctx.from.id !== ctx.chat.id) {
            await ctx.answerCallbackQuery();
            return;
        }
        const ids = await ensureSubscriber(ctx, repo);
        if (!ids)
            return;
        try {
            const account = await polymarket.resolveAccountInput({ kind: 'proxy_address', proxyAddress: ctx.match[1].toLowerCase() });
            const nowMs = Date.now();
            repo.upsertTrackedAccount({ proxyAddress: account.proxyAddress, name: account.name, pseudonym: account.pseudonym, profileSlug: account.profileSlug, nextPollAtMs: nowMs });
            const created = repo.createSubscription(ids.subscriberId, ids.chatId, account.proxyAddress, ids.userId, Math.floor(nowMs / 1000));
            onStateChanged?.();
            await ctx.answerCallbackQuery();
            await editMessageHtml(ctx, renderSubscriptionCreated(account, created.created), { reply_markup: new InlineKeyboard() });
        }
        catch {
            await ctx.answerCallbackQuery({ text: 'Could not subscribe. Try again.' });
        }
    });
    bot.hears(/^Subscribe$/i, handleSubscribeStart);
    bot.hears(/^Unsubscribe$/i, handleUnsubscribeMenu);
    bot.hears(/^Admin$/i, openAdminMenu);
    bot.hears(/^Access$/i, openAccessMenu);
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
        const report = ctx.callbackQuery.data.slice("admin:".length);
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
        await editMessageHtml(ctx, renderUnsubscribePrompt(subscriptions, clampPage(page, pageCount(subscriptions.length, UNSUBSCRIBE_PAGE_SIZE)), pageCount(subscriptions.length, UNSUBSCRIBE_PAGE_SIZE)), {
            reply_markup: renderUnsubscribeKeyboard(subscriptions, page)
        });
    });
    bot.callbackQuery(/^unsel:/, async (ctx) => {
        if (!ctx.chat) {
            return;
        }
        const proxyAddress = ctx.callbackQuery.data.slice("unsel:".length);
        const subscriptions = repo.listChatSubscriptions(ctx.chat.id);
        const selected = subscriptions.find((item) => item.proxyAddress === proxyAddress);
        const result = repo.removeSubscription(ctx.chat.id, proxyAddress);
        if (result.removedSubscriptions > 0)
            onStateChanged?.();
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
        if (result.removedSubscriptions > 0)
            onStateChanged?.();
        await ctx.answerCallbackQuery();
        await editMessageHtml(ctx, renderUnsubscribedAll(result.removedSubscriptions, result.removedTrackedAccounts));
    });
    bot.on("message:text", async (ctx) => {
        if (!ctx.chat || !ctx.from) {
            return;
        }
        if (/^(subscribe|unsubscribe|admin|access)$/i.test(ctx.message.text.trim())) {
            return;
        }
        repo.clearExpiredChatSessions(Date.now());
        const session = repo.getChatSession(ctx.chat.id);
        if (!session || session.mode !== "awaiting_subscribe") {
            return;
        }
        if (session.awaitingUserId && session.awaitingUserId !== ctx.from.id) {
            await replyHtml(ctx, "Subscribe mode is waiting for the user who started it. Press <b>Subscribe</b> again if you want to replace that request.", { reply_markup: mainKeyboard(ctx) });
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
            const created = repo.createSubscription(ids.subscriberId, ids.chatId, account.proxyAddress, ids.userId, nowSec);
            onStateChanged?.();
            repo.clearChatSession(ids.chatId);
            await replyHtml(ctx, renderSubscriptionCreated(account, created.created), { reply_markup: mainKeyboard(ctx) });
        }
        catch (error) {
            await replyHtml(ctx, `Subscription failed: <code>${escapeHtml(error instanceof Error ? error.message : String(error))}</code>`, { reply_markup: subscribeCancelKeyboard() });
        }
    });
    bot.catch(async (error) => {
        console.error("[error] Telegram command handler failed");
        await notifier?.notifyError("bot handler", error.error);
    });
    return bot;
}
