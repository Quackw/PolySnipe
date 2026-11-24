export function isRetryableSqliteLock(error) {
  return /^SQLITE_(BUSY|LOCKED)(?:_|$)/.test(error?.code ?? '');
}
