import { splitForTelegram } from "../utils/telegram.js";

export interface TelegramSenderOptions {
  token: string;
  timeoutMs: number;
  log: (line: string) => void;
}

export class TelegramRejectedError extends Error {
  constructor(
    public readonly code: number,
    public readonly retryAfterMs = 0,
    public readonly description = ""
  ) {
    super(`Telegram rejected send: ${code}`);
  }
}

export function isPermanentChatUnavailableError(error: unknown): boolean {
  if (!(error instanceof TelegramRejectedError)) return false;

  const description = error.description.trim().toLowerCase();
  if (error.code === 403) {
    return /^(?:forbidden:\s*)?(?:bot was blocked by the user|user is deactivated|bot was kicked from (?:the )?(?:group|supergroup|channel)(?: chat)?)\.?$/.test(
      description
    );
  }
  return error.code === 400 && /^(?:bad request:\s*)?chat not found\.?$/.test(description);
}

export type TelegramSender = (chatId: number, text: string) => Promise<void>;

export function createTelegramSender(options: TelegramSenderOptions): TelegramSender {
  return async (chatId: number, text: string): Promise<void> => {
    let acknowledged = false;
    for (const chunk of splitForTelegram(text, 3900)) {
      try {
        await sendMessage(options.token, chatId, chunk, options.timeoutMs);
        acknowledged = true;
      } catch (error) {
        // A retryable rejection of this chunk cannot undo a previous acknowledgement.
        if (acknowledged) throw new Error("Telegram partial delivery is unconfirmed");
        throw error;
      }
    }
    options.log(`[telegram] sent chat=${chatId} chars=${text.length}`);
  };
}

async function sendMessage(token: string, chatId: number, text: string, timeoutMs: number): Promise<void> {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const response = await fetch(`https://api.telegram.org/bot${token}/sendMessage`, {
      method: "POST",
      signal: controller.signal,
      headers: {
        "content-type": "application/json"
      },
      body: JSON.stringify({
        chat_id: String(chatId),
        text,
        parse_mode: "HTML",
        disable_web_page_preview: true
      })
    });

    const body = (await response.json()) as {
      ok?: unknown;
      error_code?: unknown;
      description?: unknown;
      parameters?: { retry_after?: unknown };
      result?: { message_id?: unknown; chat?: { id?: unknown } };
    };
    if (
      !response.ok &&
      response.status === body?.error_code &&
      body?.ok === false &&
      typeof body.error_code === "number" &&
      body.error_code >= 400 &&
      body.error_code < 500
    ) {
      const seconds = body.parameters?.retry_after;
      throw new TelegramRejectedError(
        body.error_code,
        typeof seconds === "number" && Number.isFinite(seconds) && seconds > 0 ? seconds * 1000 : 30000,
        typeof body.description === "string" ? body.description : ""
      );
    }
    if (
      !response.ok ||
      body?.ok !== true ||
      !Number.isSafeInteger(body.result?.message_id) ||
      Number(body.result?.message_id) <= 0 ||
      String(body.result?.chat?.id) !== String(chatId)
    ) {
      throw new Error("Telegram acknowledgement missing or invalid");
    }
  } finally {
    clearTimeout(timeout);
  }
}
