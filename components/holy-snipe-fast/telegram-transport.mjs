import { withTelegramDiagnostics } from './telegram-diagnostics.mjs';
import { telegramHTTPSFetch } from './telegram-https.mjs';
// No retries or logging here. The existing persistent sender owns retry policy.
const networkCodes = new Set([
  'ETIMEDOUT',
  'ECONNRESET',
  'ECONNREFUSED',
  'ENOTFOUND',
  'EAI_AGAIN',
  'UND_ERR_CONNECT_TIMEOUT',
  'UND_ERR_HEADERS_TIMEOUT',
  'UND_ERR_BODY_TIMEOUT',
  'UND_ERR_SOCKET'
]);
function safeFailure(error) {
  if (error?.name === 'TimeoutError' || error?.name === 'AbortError') return 'TRANSPORT_TIMEOUT';
  const code = error?.cause?.code ?? error?.code;
  return networkCodes.has(code) ? code : 'TRANSPORT_ERROR';
}
export async function telegramRequest(
  token,
  method,
  body,
  { timeoutMs = 8000, captureReceipt = false, fetchImpl = telegramHTTPSFetch, onDiagnostic } = {}
) {
  return withTelegramDiagnostics(async (setPhase, recordError) => {
    let response;
    try {
      response = await fetchImpl(`https://api.telegram.org/bot${token}/${method}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(timeoutMs)
      });
    } catch (error) {
      recordError(error);
      return { kind: 'AMBIGUOUS', status: null, reason: safeFailure(error) };
    }
    setPhase('RESPONSE_HEADERS_RECEIVED');
    const status = response.status;
    let value = null;
    if (status === 200 || status === 429 || status >= 400) {
      try {
        value = await response.json();
        setPhase('RESPONSE_BODY_PARSED');
      } catch (error) {
        recordError(error);
        if (status === 200) return { kind: 'AMBIGUOUS', status, reason: 'RESPONSE_INVALID_JSON' };
      }
    }
    const retryAfter =
      Number.isSafeInteger(value?.parameters?.retry_after) && value.parameters.retry_after >= 0
        ? value.parameters.retry_after
        : null;
    if (value?.ok === false && Number.isSafeInteger(value.error_code)) {
      const code = value.error_code;
      return {
        kind: code === 408 || code === 429 || code >= 500 ? 'RETRYABLE' : 'FINAL',
        status,
        error_code: code,
        reason: 'API_' + code,
        retry_after_ms: code === 429 && retryAfter !== null ? retryAfter * 1000 : null
      };
    }
    if (status === 200) {
      // Every sendMessage must have a positive Bot API acknowledgement, not HTTP alone.
      if (method === 'getMe' || method === 'sendMessage' || captureReceipt) {
        if (value?.ok !== true) return { kind: 'AMBIGUOUS', status, reason: 'API_ACK_NOT_CONFIRMED' };
        if (
          (method === 'sendMessage' || captureReceipt) &&
          (!Number.isSafeInteger(value?.result?.message_id) || value.result.message_id <= 0)
        )
          return { kind: 'AMBIGUOUS', status, reason: 'RECEIPT_INVALID' };
      } else {
        try {
          await response.body?.cancel();
        } catch {}
      }
      return { kind: 'OK', status, value };
    }
    try {
      await response.body?.cancel();
    } catch {}
    return {
      kind: status === 408 || status === 429 || status >= 500 ? 'RETRYABLE' : 'FINAL',
      status,
      reason: 'HTTP_' + status,
      retry_after_ms: status === 429 && retryAfter !== null ? retryAfter * 1000 : null
    };
  }, onDiagnostic);
}

export function deliveryDecision(result, row, now) {
  if (result.kind === 'OK')
    return { state: 'DELIVERED', next: 0, error: null, possible: row.possible_duplicate, delivered: now };
  if (result.kind === 'AMBIGUOUS')
    return { state: 'DELIVERY_UNKNOWN', next: 0, error: result.reason, possible: 1, delivered: null };
  if (result.kind === 'RETRYABLE' && row.attempts + 1 < 3)
    return {
      state: 'FAILED_RETRYABLE',
      next: now + Math.max(Math.min(30000, 2000 * 2 ** row.attempts), result.retry_after_ms ?? 0),
      error: result.reason,
      possible: row.possible_duplicate,
      delivered: null
    };
  return {
    state: 'FAILED_FINAL',
    next: 0,
    error: result.reason,
    possible: row.possible_duplicate,
    delivered: null
  };
}
