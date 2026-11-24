import type { Context } from "grammy";
import { escapeHtml } from "./html.js";

export const TELEGRAM_MAX_TEXT_LENGTH = 4096;

export async function replyHtml(
  ctx: Context,
  html: string,
  extra: Record<string, unknown> = {}
): Promise<void> {
  const chunks = splitForTelegram(html, 3900);
  for (const [index, chunk] of chunks.entries()) {
    await ctx.reply(chunk, {
      parse_mode: "HTML",
      disable_web_page_preview: true,
      ...(index === 0 ? extra : {})
    } as never);
  }
}

export function splitForTelegram(text: string, maxLen = TELEGRAM_MAX_TEXT_LENGTH): string[] {
  if (text.length <= maxLen) {
    return [text];
  }

  const out: string[] = [];
  let index = 0;

  while (index < text.length) {
    const end = Math.min(index + maxLen, text.length);
    if (end === text.length) {
      out.push(text.slice(index));
      break;
    }

    const chunk = text.slice(index, end);
    const newlineSplit = chunk.lastIndexOf("\n");
    const spaceSplit = chunk.lastIndexOf(" ");
    const splitAt = newlineSplit > 0 ? newlineSplit : spaceSplit;
    const cut = splitAt > 0 ? splitAt : chunk.length;

    out.push(text.slice(index, index + cut).trimEnd());
    index += cut;
    while (index < text.length && /\s/.test(text[index] ?? "")) {
      index += 1;
    }
  }

  return out.filter((item) => item.length > 0);
}

export function toSafeCode(text: string): string {
  return `<code>${escapeHtml(text)}</code>`;
}
