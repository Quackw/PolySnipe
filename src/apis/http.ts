export interface RequestOptions {
  timeoutMs: number;
  retries: number;
  beforeAttempt?: (() => Promise<(() => void) | undefined>) | undefined;
  onRateLimit?: ((delayMs: number) => void) | undefined;
  onRetry?: ((url: string, attempt: number, error: unknown, delayMs: number) => void) | undefined;
  headers?: Record<string, string> | undefined;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

type HttpError = Error & {
  statusCode?: number;
  retryAfterMs?: number;
};

export function parseRetryAfterMs(value: string | null, nowMs = Date.now()): number | undefined {
  if (!value) {
    return undefined;
  }

  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) {
    return Math.ceil(seconds * 1000);
  }

  const dateMs = Date.parse(value);
  return Number.isFinite(dateMs) ? Math.max(0, dateMs - nowMs) : undefined;
}

export function calculateHttpRetryDelayMs(error: unknown, attempt: number, randomValue = Math.random()): number {
  const httpError = error as HttpError;
  if (httpError?.statusCode === 429) {
    const exponentialMs = Math.min(30_000, 1000 * 2 ** Math.max(0, attempt - 1));
    const jitterMs = Math.floor(Math.max(0, Math.min(1, randomValue)) * 250);
    return Math.max(exponentialMs, httpError.retryAfterMs ?? 0) + jitterMs;
  }

  return 200 * attempt;
}

export async function getJson<T>(url: string, options: RequestOptions): Promise<T> {
  const text = await getText(url, options);
  return JSON.parse(text) as T;
}

export async function getText(url: string, options: RequestOptions): Promise<string> {
  let lastError: unknown;

  for (let attempt = 0; attempt <= options.retries; attempt += 1) {
    let ticketRelease = await options.beforeAttempt?.();
    const release = (): void => {
      const fn = ticketRelease;
      ticketRelease = undefined;
      fn?.();
    };
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), options.timeoutMs);

    try {
      const response = await fetch(url, {
        method: "GET",
        signal: controller.signal,
        headers: {
          "User-Agent": "poly-snipe-bot/0.1.0",
          ...options.headers
        }
      });

      if (!response.ok) {
        const body = await response.text();
        const error = new Error(`HTTP ${response.status} for ${url}: ${body.slice(0, 300)}`) as HttpError;
        error.statusCode = response.status;
        const retryAfterMs = parseRetryAfterMs(response.headers.get("retry-after"));
        if (retryAfterMs !== undefined) {
          error.retryAfterMs = retryAfterMs;
        }
        throw error;
      }

      return await response.text();
    } catch (error) {
      lastError = error;
      clearTimeout(timeout);
      release();
      const retryAttempt = attempt + 1;
      const retryDelayMs = calculateHttpRetryDelayMs(error, retryAttempt);
      if ((error as HttpError)?.statusCode === 429) {
        options.onRateLimit?.(retryDelayMs);
      }
      if (attempt >= options.retries) {
        break;
      }
      options.onRetry?.(url, retryAttempt, error, retryDelayMs);
      await delay(retryDelayMs);
    } finally {
      clearTimeout(timeout);
      release();
    }
  }

  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}
