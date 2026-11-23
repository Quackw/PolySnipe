import { escapeHtml } from "../utils/html.js";
import { splitForTelegram } from "../utils/telegram.js";
import type { AppConfig } from "../types.js";

export type AdminErrorSource = "startup" | "uncaughtException" | "unhandledRejection" | "bot handler";

export interface AdminNotifier {
  notifyError: (source: AdminErrorSource, error: unknown) => Promise<void>;
}

function toErrorLike(error: unknown): { message: string; stack?: string | undefined } {
  if (error instanceof Error) {
    return {
      message: error.message,
      stack: error.stack
    };
  }

  return {
    message: String(error)
  };
}

function truncate(value: string, maxLen: number): string {
  return value.length <= maxLen ? value : `${value.slice(0, maxLen - 3)}...`;
}

export function renderAdminErrorNotification(source: AdminErrorSource, error: unknown): string {
  const details = toErrorLike(error);
  const lines = [
    "<b>Fatal bot error</b>",
    `Source: <b>${escapeHtml(source)}</b>`,
    `Message: <code>${escapeHtml(truncate(details.message, 800))}</code>`
  ];

  if (details.stack) {
    lines.push("", "<b>Stack</b>", `<code>${escapeHtml(truncate(details.stack, 3000))}</code>`);
  }

  return lines.join("\n");
}

export function createAdminNotifier(
  config: Pick<AppConfig, "adminTelegramUserIds">,
  sendTelegram: (chatId: number, text: string) => Promise<void>,
  log: (line: string) => void
): AdminNotifier {
  return {
    notifyError: async (source: AdminErrorSource, error: unknown): Promise<void> => {
      if (config.adminTelegramUserIds.length === 0) {
        return;
      }

      const text = renderAdminErrorNotification(source, error);
      for (const adminTelegramUserId of config.adminTelegramUserIds) {
        try {
          for (const chunk of splitForTelegram(text, 3900)) {
            await sendTelegram(adminTelegramUserId, chunk);
          }
        } catch (notifyError) {
          const details = notifyError instanceof Error ? notifyError.message : String(notifyError);
          log(`[notify:error] source=${source} admin=${adminTelegramUserId} error=${details}`);
        }
      }
    }
  };
}
