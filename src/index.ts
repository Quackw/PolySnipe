import { loadConfig } from "./config.js";
import { openDb } from "./db/client.js";
import { AppRepository } from "./db/repository.js";
import { DefaultPolymarketClient } from "./apis/polymarket.js";
import { createTelegramSender } from "./apis/telegram.js";
import { createTelegramBot } from "./bot.js";
import { PollScheduler } from "./scheduler.js";
import { createAdminNotifier } from "./services/adminNotifier.js";
import { notifyFatalError } from "./services/runtimeErrors.js";
import { pollingHealth } from "./services/pollingHealth.js";

async function main(config = loadConfig()): Promise<void> {
  const sender = createTelegramSender({
    token: config.telegramBotToken,
    timeoutMs: config.telegramTimeoutMs,
    log: (line) => console.log(line)
  });
  const notifier = createAdminNotifier(config, sender, (line) => console.log(line));
  const db = openDb(config.sqlitePath);
  const repo = new AppRepository(db);
  const polymarket = new DefaultPolymarketClient(config);
  const scheduler = new PollScheduler(config, repo, polymarket, {
    sendTelegram: sender,
    log: (line) => console.log(line)
  });
  const bot = createTelegramBot(config, repo, polymarket, notifier);

  let shutdownPromise: Promise<void> | null = null;
  let stopping = false;
  const health = pollingHealth(() => {
    console.error("[fatal] Telegram polling stale for 180 seconds; restarting");
    process.exitCode = 1;
    void shutdown();
  });
  bot.api.config.use(async (previous, method, payload, signal) => {
    const result = await previous(method, payload, signal);
    if (method === "getUpdates" && result.ok) health.success();
    return result;
  });
  const shutdown = (): Promise<void> => {
    if (shutdownPromise) {
      return shutdownPromise;
    }

    stopping = true;
    health.stop();
    scheduler.stop();
    const deadline = setTimeout(() => {
      console.error("[fatal] Shutdown deadline exceeded");
      process.exit(1);
    }, 20_000);
    shutdownPromise = (async () => {
      try {
        await Promise.all([bot.isRunning() ? bot.stop() : Promise.resolve(), scheduler.stopAndDrain()]);
        repo.close();
        clearTimeout(deadline);
      } catch {
        console.error("[fatal] Shutdown failed");
        process.exit(1);
      }
    })();
    return shutdownPromise;
  };

  process.once("SIGINT", () => void shutdown());
  process.once("SIGTERM", () => void shutdown());
  process.once("uncaughtException", (error) => {
    void notifyFatalError(notifier, "uncaughtException", error, (line) => console.log(line)).finally(async () => {
      console.error(`[fatal] uncaughtException ${(error as Error).message ?? String(error)}`);
      process.exitCode = 1;
      await shutdown();
    });
  });
  process.once("unhandledRejection", (reason) => {
    void notifyFatalError(notifier, "unhandledRejection", reason, (line) => console.log(line)).finally(async () => {
      console.error(`[fatal] unhandledRejection ${String(reason)}`);
      process.exitCode = 1;
      await shutdown();
    });
  });

  scheduler.start();
  await bot.start({
    timeout: 25,
    onStart: (info) => {
      console.log(`[ok] Telegram bot started as @${info.username}`);
    }
  });
  if (!stopping) {
    console.error("[fatal] Telegram polling exited unexpectedly");
    process.exitCode = 1;
    await shutdown();
  }
}

const startupConfig = (() => {
  try {
    return loadConfig();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error(`[fatal] ${message}`);
    process.exitCode = 1;
    return null;
  }
})();

if (startupConfig) {
  main(startupConfig).catch((error: unknown) => {
    // Do not leave scheduler timers running after the Telegram loop fails.
    // Raw transport errors can contain the Telegram token URL.
    console.error("[fatal] Telegram runtime failed; restarting");
    setTimeout(() => process.exit(1), 5_000);
    const sender = createTelegramSender({
      token: startupConfig.telegramBotToken,
      timeoutMs: startupConfig.telegramTimeoutMs,
      log: (line) => console.log(line)
    });
    const notifier = createAdminNotifier(startupConfig, sender, (line) => console.log(line));
    void notifyFatalError(notifier, "startup", error, (line) => console.log(line)).finally(() => {
      process.exit(1);
    });
  });
}
