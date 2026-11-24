import { createHash } from 'node:crypto';

export const sha = (value) => createHash('sha256').update(value).digest('hex');

export function normalizePairs(rows, { exact14 = true } = {}) {
  if (!Array.isArray(rows) || rows.length > 1000 || (exact14 && rows.length !== 14))
    throw Error('SCOPE_INVALID');
  const seen = new Set();
  const result = rows.map((r) => {
    if (
      !r ||
      typeof r.wallet !== 'string' ||
      !/^0x[0-9a-f]{40}$/.test(r.wallet) ||
      !Number.isSafeInteger(r.recipient) ||
      r.recipient <= 0 ||
      !Number.isSafeInteger(r.notify_from_ts) ||
      r.notify_from_ts < 0
    )
      throw Error('SCOPE_INVALID');
    const key = r.wallet + '|' + r.recipient;
    if (seen.has(key)) throw Error('SCOPE_DUPLICATE_PAIR');
    seen.add(key);
    return Object.freeze({ wallet: r.wallet, recipient: r.recipient, notify_from_ts: r.notify_from_ts });
  });
  return Object.freeze(result.sort((a, b) => a.wallet.localeCompare(b.wallet) || a.recipient - b.recipient));
}
export const scopeFingerprint = (rows, options) => sha(JSON.stringify(normalizePairs(rows, options)));
export function validateScopeApproval(rows, approval) {
  const scope = normalizePairs(rows, { exact14: false });
  if (
    !scope.length ||
    approval?.kind !== 'FROZEN_CURRENT_SCOPE_SHADOW_ONLY' ||
    approval.telegram_enabled !== false ||
    approval.production_changes !== false ||
    approval.scope_pairs !== scope.length ||
    approval.scope_wallets !== new Set(scope.map((p) => p.wallet)).size ||
    approval.scope_fingerprint !== scopeFingerprint(scope, { exact14: false }) ||
    !Number.isSafeInteger(approval.captured_at_ms) ||
    approval.captured_at_ms <= 0 ||
    !/^[a-f0-9]{64}$/.test(approval.scope_sha256 ?? '')
  )
    throw Error('EXPLICIT_CURRENT_SCOPE_APPROVAL_REQUIRED');
  return Object.freeze({
    kind: approval.kind,
    scope_pairs: scope.length,
    scope_wallets: approval.scope_wallets,
    scope_fingerprint: approval.scope_fingerprint,
    scope_sha256: approval.scope_sha256,
    captured_at_ms: approval.captured_at_ms,
    telegram_enabled: false,
    production_changes: false
  });
}
