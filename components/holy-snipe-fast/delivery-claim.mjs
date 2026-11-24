import {isRetryableSqliteLock} from './sqlite-lock.mjs';

// The single sender resolves only its own claim, even if it loses ownership.
export function createDeliveryClaimGuard(db, {onClaimLost, now=Date.now}) {
  const release = db.prepare("UPDATE deliveries SET state=?,claim_token=NULL,next_at_ms=?," +
    "last_error=?,attempts=?,ambiguous_retries=?,send_started_at_ms=? " +
    "WHERE event_key=? AND chat_id=? AND state='SENDING' AND claim_token=?");
  const unknown = db.prepare("UPDATE deliveries SET state='DELIVERY_UNKNOWN',claim_token=NULL," +
    "next_at_ms=0,possible_duplicate=1,last_error='SEND_WORKER_EXCEPTION' " +
    "WHERE event_key=? AND chat_id=? AND state='SENDING' AND claim_token=?");
  let unsettled;

  function flush() {
    if (!unsettled) return true;
    const {row, started, reason} = unsettled;
    const key = [row.event_key, row.chat_id, row.claimToken];
    let result;
    try {
      if (started) {
        result = unknown.run(...key);
      } else {
        const cancelled = reason === 'SUBSCRIPTION_INACTIVE';
        result = release.run(cancelled ? 'CANCELLED_SUBSCRIPTION' : 'PENDING',
          cancelled ? 0 : Math.max(row.next_at_ms, now() + 250),
          cancelled ? 'subscription_removed' : reason, row.attempts,
          row.ambiguous_retries, row.send_started_at_ms, ...key);
      }
    } catch (error) {
      // Keep this SQLite-only write ahead of any future network work.
      if (isRetryableSqliteLock(error)) return false;
      throw error;
    }
    unsettled = undefined;
    if (result.changes !== 1) onClaimLost(row);
    return true;
  }

  async function run(row, beforeRequest, request, onResult) {
    let started = false;
    let settled = false;
    let reason;
    try {
      reason = beforeRequest(row);
      if (reason) return;
      started = true;
      const result = await request(row);
      onResult(result, () => { settled = true; });
    } finally {
      if (!settled) {
        unsettled = {row, started, reason: reason ?? 'SEND_WORKER_EXCEPTION'};
        flush();
      }
    }
  }

  return {run, flush};
}
