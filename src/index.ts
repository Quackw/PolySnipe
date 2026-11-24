import { pathToFileURL } from "node:url";
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

const SHUTDOWN_DEADLINE_MS = 20_000;
const FATAL_NOTIFY_TIMEOUT_MS = 3_000;

async function notifyWithDeadline(notify: () => Promise<void>): Promise<void> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      new Promise<void>((resolve) => { timeout = setTimeout(resolve, FATAL_NOTIFY_TIMEOUT_MS); }),
      Promise.resolve().then(notify)
    ]);
  } catch {
    console.error("[fatal] Error notification failed");
  } finally {
    clearTimeout(timeout);
  }
}

export async function main(config = loadConfig()): Promise<void> {
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

  let shutdownPromise: Promise<void> | undefined;
  let shutdownDeadline: ReturnType<typeof setTimeout> | undefined;
  let stopping = false;
  let fatal = false;
  const armShutdownDeadline = () => {
    shutdownDeadline ??= setTimeout(() => {
      console.error("[fatal] Shutdown deadline exceeded");
      process.exit(1);
    }, SHUTDOWN_DEADLINE_MS);
  };
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
    if (shutdownPromise) return shutdownPromise;
    stopping = true;
    armShutdownDeadline();
    shutdownPromise = Promise.resolve().then(async () => {
      health.stop();
      const results = await Promise.allSettled([
        Promise.resolve().then(() => bot.isRunning() ? bot.stop() : undefined),
        Promise.resolve().then(() => scheduler.stopAndDrain())
      ]);
      db.close();
      if (results.some((result) => result.status === "rejected")) throw new Error("Drain failed");
      clearTimeout(shutdownDeadline);
      process.off("SIGINT", onSignal);
      process.off("SIGTERM", onSignal);
      process.off("uncaughtException", onUncaughtException);
      process.off("unhandledRejection", onUnhandledRejection);
    }).catch(() => {
      console.error("[fatal] Shutdown failed");
      process.exit(1);
    });
    return shutdownPromise;
  };

  function onSignal() {
    void shutdown();
  }
  function onFatal(source: "uncaughtException" | "unhandledRejection", error: unknown) {
    if (fatal) return;
    fatal = true;
    process.exitCode = 1;
    armShutdownDeadline();
    scheduler.stop();
    console.error("[fatal] " + source);
    void notifyWithDeadline(() => notifyFatalError(notifier, source, error, (line) => console.log(line)))
      .then(shutdown)
      .finally(() => process.exit(1));
  }
  function onUncaughtException(error: Error) {
    onFatal("uncaughtException", error);
  }
  function onUnhandledRejection(reason: unknown) {
    onFatal("unhandledRejection", reason);
  }

  process.once("SIGINT", onSignal);
  process.once("SIGTERM", onSignal);
  process.once("uncaughtException", onUncaughtException);
  process.once("unhandledRejection", onUnhandledRejection);

  try {
    scheduler.start();
    await bot.start({
      timeout: 25,
      onStart: (info) => {
        console.log("[ok] Telegram bot started as @" + info.username);
      }
    });
    if (!stopping) {
      console.error("[fatal] Telegram polling exited unexpectedly");
      process.exitCode = 1;
      await shutdown();
    }
  } catch (error) {
    await shutdown();
    throw error;
  }
}

function start(): void {
  let config: ReturnType<typeof loadConfig>;
  try {
    config = loadConfig();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    console.error("[fatal] " + message);
    process.exitCode = 1;
    return;
  }
  main(config).catch((error: unknown) => {
    // Raw transport errors can contain the Telegram token URL.
    console.error("[fatal] Telegram runtime failed; restarting");
    setTimeout(() => process.exit(1), 5_000);
    const sender = createTelegramSender({
      token: config.telegramBotToken,
      timeoutMs: config.telegramTimeoutMs,
      log: (line) => console.log(line)
    });
    const notifier = createAdminNotifier(config, sender, (line) => console.log(line));
    void notifyWithDeadline(() => notifyFatalError(notifier, "startup", error, (line) => console.log(line)))
      .finally(() => process.exit(1));
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) start();
