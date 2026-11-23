import { loadConfig } from "./config.js";
import { openDb } from "./db/client.js";
import { AppRepository } from "./db/repository.js";
import { DefaultPolymarketClient } from "./apis/polymarket.js";
import { createTelegramSender } from "./apis/telegram.js";
import { createTelegramBot } from "./bot.js";
import { PollScheduler } from "./scheduler.js";
import { createAdminNotifier } from "./services/adminNotifier.js";
import { notifyFatalError } from "./services/runtimeErrors.js";
import { createPrivateSender } from "./privateAccess.js";
class BotIdentityMismatchError extends Error {
}
async function main(config = loadConfig()) {
    const db = openDb(config.sqlitePath);
    const repo = new AppRepository(db);
    const rawSender = createTelegramSender({
        token: config.telegramBotToken,
        timeoutMs: config.telegramTimeoutMs,
        log: (line) => console.log(line)
    });
    const sender = createPrivateSender(rawSender, config.allowedTelegramUserIds, (id) => repo.hasAccessGrant(id));
    const notifier = createAdminNotifier(config, sender, (line) => console.log(line));
    const polymarket = new DefaultPolymarketClient(config);
    const scheduler = new PollScheduler(config, repo, polymarket, {
        sendTelegram: sender,
        log: (line) => console.log(line)
    });
    const bot = createTelegramBot(config, repo, polymarket, notifier);
    // Check the token's actual bot identity before polling or receiving updates.
    await bot.init();
    if (bot.botInfo.username.toLowerCase() !== config.expectedTelegramUsername) {
        repo.close();
        throw new BotIdentityMismatchError("Telegram bot username does not match EXPECTED_TELEGRAM_USERNAME");
    }
    let shutdownPromise = null;
    const shutdown = () => {
        if (shutdownPromise) {
            return shutdownPromise;
        }
        shutdownPromise = (async () => {
            bot.stop();
            await scheduler.stopAndDrain();
            repo.close();
        })();
        return shutdownPromise;
    };
    process.once("SIGINT", () => void shutdown());
    process.once("SIGTERM", () => void shutdown());
    process.once("uncaughtException", (error) => {
        void notifyFatalError(notifier, "uncaughtException", error, (line) => console.log(line)).finally(async () => {
            console.error(`[fatal] uncaughtException ${error instanceof Error ? error.name : "unknown"}`);
            process.exitCode = 1;
            await shutdown();
        });
    });
    process.once("unhandledRejection", (reason) => {
        void notifyFatalError(notifier, "unhandledRejection", reason, (line) => console.log(line)).finally(async () => {
            console.error(`[fatal] unhandledRejection ${reason instanceof Error ? reason.name : "unknown"}`);
            process.exitCode = 1;
            await shutdown();
        });
    });
    scheduler.start();
    await bot.start({
        onStart: (info) => {
            console.log(`[ok] Telegram bot started as @${info.username}`);
        }
    });
}
const startupConfig = (() => {
    try {
        return loadConfig();
    }
    catch (error) {
        const message = error instanceof Error ? error.message : "startup failed";
        console.error(`[fatal] ${message}`);
        process.exitCode = 1;
        return null;
    }
})();
if (startupConfig) {
    main(startupConfig).catch((error) => {
        const message = error instanceof Error ? error.name : "unknown";
        if (error instanceof BotIdentityMismatchError) {
            console.error("[fatal] Telegram bot username does not match EXPECTED_TELEGRAM_USERNAME");
            process.exitCode = 1;
            return;
        }
        const rawSender = createTelegramSender({
            token: startupConfig.telegramBotToken,
            timeoutMs: startupConfig.telegramTimeoutMs,
            log: (line) => console.log(line)
        });
        const sender = createPrivateSender(rawSender, startupConfig.allowedTelegramUserIds);
        const notifier = createAdminNotifier(startupConfig, sender, (line) => console.log(line));
        void notifyFatalError(notifier, "startup", error, (line) => console.log(line)).finally(() => {
            console.error(`[fatal] ${message}`);
            process.exitCode = 1;
        });
    });
}
