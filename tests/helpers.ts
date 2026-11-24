import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { Bot } from "grammy";
import { openDb } from "../src/db/client.js";
import { AppRepository } from "../src/db/repository.js";
import type { AppConfig } from "../src/types.js";

export function createTempRepo(prefix = "poly-snipe-test"): {
  repo: AppRepository;
  dbPath: string;
  cleanup: () => void;
} {
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), `${prefix}-`));
  const dbPath = path.join(tmpDir, "state.db");
  const db = openDb(dbPath);
  const repo = new AppRepository(db);

  return {
    repo,
    dbPath,
    cleanup: () => {
      try {
        repo.close();
      } finally {
        fs.rmSync(tmpDir, { recursive: true, force: true });
      }
    }
  };
}

export function testConfig(dbPath: string, overrides: Partial<AppConfig> = {}): AppConfig {
  return {
    telegramBotToken: "token",
    adminTelegramUserIds: [],
    sqlitePath: dbPath,
    schedulerTickMs: 10,
    pollTargetIntervalSec: 1,
    activitySafeRps: 100,
    activityConcurrency: 5,
    activityPageLimit: 3,
    activityMaxPages: 3,
    activityOverlapSec: 2,
    rebalanceEveryMs: 100,
    orphanCleanupEveryMs: 500,
    subscribeModeTtlMs: 600_000,
    staleInflightAfterMs: 60_000,
    staleInflightRecoveryEveryMs: 200,
    walCheckpointEveryMs: 1_000,
    heartbeatEveryMs: 1_000,
    alertDelayThresholdSec: 30,
    alertSendConcurrency: 1,
    pollRunsRetentionHours: 72,
    polymarketTimeoutMs: 1_000,
    polymarketRetries: 0,
    telegramTimeoutMs: 1_000,
    httpTimeoutMs: 1_000,
    httpRetries: 0,
    debug: false,
    ...overrides
  };
}

export interface TelegramCall {
  method: string;
  payload: Record<string, unknown>;
}

export function installTelegramApiMock(bot: Bot): TelegramCall[] {
  const calls: TelegramCall[] = [];
  let messageId = 100;

  const transformer = (async (_prev: unknown, method: string, payload: unknown) => {
    const objectPayload = payload as Record<string, unknown>;
    calls.push({ method, payload: objectPayload });

    if (method === "sendMessage" || method === "editMessageText") {
      return {
        ok: true,
        result: {
          message_id: messageId++,
          date: 1,
          chat: {
            id: Number(objectPayload.chat_id ?? 0),
            type: "private"
          },
          text: String(objectPayload.text ?? "")
        }
      };
    }

    return { ok: true, result: true };
  }) as never;

  bot.api.config.use(transformer);
  return calls;
}
