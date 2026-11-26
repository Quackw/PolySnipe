function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
const MAX_RETRY_AFTER_MS = 60_000;
const MAX_BACKOFF_MS = 30_000;
class HttpStatusError extends Error {
    statusCode;
    retryAfterMs;
    constructor(statusCode, url, retryAfterMs) {
        super(`HTTP ${statusCode} for ${url}`);
        this.statusCode = statusCode;
        this.retryAfterMs = retryAfterMs;
    }
}
export function parseRetryAfterMs(value, nowMs = Date.now()) {
    const header = value?.trim();
    if (!header) {
        return undefined;
    }
    const seconds = Number(header);
    if (Number.isFinite(seconds)) {
        return seconds < 0 ? undefined : Math.min(MAX_RETRY_AFTER_MS, Math.ceil(seconds * 1000));
    }
    const dateMs = Date.parse(header);
    return Number.isFinite(dateMs) ? Math.min(MAX_RETRY_AFTER_MS, Math.max(0, dateMs - nowMs)) : undefined;
}
export function calculateHttpRetryDelayMs(error, attempt, randomValue = Math.random()) {
    const httpError = error;
    const baseMs = httpError?.statusCode === 429 ? 1000 : 200;
    const exponentialMs = baseMs * 2 ** Math.max(0, attempt - 1);
    const jitterMs = Math.floor(Math.max(0, Math.min(1, randomValue)) * 250);
    const backoffMs = Math.min(MAX_BACKOFF_MS, exponentialMs + jitterMs);
    return Math.max(backoffMs, Math.min(MAX_RETRY_AFTER_MS, httpError?.retryAfterMs ?? 0));
}
function isRetryable(error) {
    if (error instanceof HttpStatusError) {
        const { statusCode } = error;
        return statusCode === 408 || statusCode === 429 || (statusCode >= 500 && statusCode < 600);
    }
    // Fetch and body-stream network failures reject with TypeError. JSON parsing
    // can fail transiently too, such as when an intermediary serves an HTML page.
    return (error instanceof TypeError ||
        error instanceof SyntaxError ||
        (error instanceof Error && (error.name === "AbortError" || error.name === "TimeoutError")));
}
export function getJson(url, options) {
    return get(url, options, (body) => JSON.parse(body));
}
export function getText(url, options) {
    return get(url, options, (body) => body);
}
async function get(url, options, parse) {
    const maxRetries = Number.isFinite(options.retries) ? Math.max(0, Math.floor(options.retries)) : 0;
    for (let attempt = 0;; attempt += 1) {
        const release = await options.beforeAttempt?.();
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), options.timeoutMs);
        let retry;
        try {
            const response = await fetch(url, {
                method: "GET",
                signal: controller.signal,
                headers: {
                    "User-Agent": "holy-snipe-v2/0.1.0",
                    ...options.headers
                }
            });
            if (!response.ok) {
                // Classify from headers even if the error response body is stalled or truncated.
                void response.body?.cancel().catch(() => undefined);
                throw new HttpStatusError(response.status, url, parseRetryAfterMs(response.headers.get("retry-after")));
            }
            return parse(await response.text());
        }
        catch (error) {
            if (!isRetryable(error)) {
                throw error;
            }
            const delayMs = calculateHttpRetryDelayMs(error, attempt + 1);
            if (error instanceof HttpStatusError && error.statusCode === 429) {
                // Protect other clients of this gate even when this request has no retries left.
                options.onRateLimit?.(delayMs);
            }
            if (attempt >= maxRetries) {
                throw error;
            }
            retry = { error, delayMs };
        }
        finally {
            clearTimeout(timeout);
            release?.();
        }
        options.onRetry?.(url, attempt + 1, retry.error, retry.delayMs);
        await delay(retry.delayMs);
    }
}
