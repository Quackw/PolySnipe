import { escapeHtml } from "../utils/html.js";
import { splitForTelegram } from "../utils/telegram.js";
function toErrorLike(error) {
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
function truncate(value, maxLen) {
    return value.length <= maxLen ? value : `${value.slice(0, maxLen - 3)}...`;
}
export function renderAdminErrorNotification(source, error) {
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
export function createAdminNotifier(config, sendTelegram, log) {
    return {
        notifyError: async (source, error) => {
            if (config.adminTelegramUserIds.length === 0) {
                return;
            }
            const text = renderAdminErrorNotification(source, error)
                .replaceAll(escapeHtml(config.telegramBotToken), "[redacted]");
            for (const adminTelegramUserId of config.adminTelegramUserIds) {
                try {
                    for (const chunk of splitForTelegram(text, 3900)) {
                        await sendTelegram(adminTelegramUserId, chunk);
                    }
                }
                catch (notifyError) {
                    log(`[notify:error] source=${source} error_type=${notifyError instanceof Error ? notifyError.name : "unknown"}`);
                }
            }
        }
    };
}
