import { splitForTelegram } from "../utils/telegram.js";
export function createTelegramSender(options) {
    return async (chatId, text) => {
        for (const chunk of splitForTelegram(text, 3900)) {
            await sendMessage(options.token, chatId, chunk, options.timeoutMs);
        }
        options.log(`[telegram] sent chars=${text.length}`);
    };
}
async function sendMessage(token, chatId, text, timeoutMs) {
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
        if (!response.ok) {
            const body = await response.text();
            throw new Error(`Telegram sendMessage failed: HTTP ${response.status} ${body.slice(0, 300)}`);
        }
    }
    finally {
        clearTimeout(timeout);
    }
}
