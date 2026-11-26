// Read-only local check for a future service/timer. Importing is inert. No
// network, notification, restart or queue access is part of this checker.
import fs from 'node:fs';
import { pathToFileURL } from 'node:url';
import { createIngressControl } from './ingress-control.mjs';
export function checkIngressHealth(value, { mode, now = Date.now() } = {}) {
  const age = now - value?.at_ms;
  if (!Number.isFinite(value?.at_ms) || !Number.isFinite(age) || age < 0 || age > 10000)
    return { state: 'DEGRADED', reason: 'STATUS_NOT_FRESH', alarm: true };
  if (mode === 'FAST') {
    const h = value.rtds_ingress;
    if (!h) return { state: 'DEGRADED', reason: 'INGRESS_CONTROL_UNAVAILABLE', alarm: true };
    if (h.state !== 'TWO_INPUTS_HEALTHY' || h.alarm !== false)
      return { state: 'DEGRADED', reason: 'INGRESS_REDUNDANCY_OR_JOURNAL_LOST', alarm: true };
    const a = value.rtds_peers?.A,
      b = value.rtds_peers?.B,
      peer = b?.health;
    const checked = createIngressControl().sample({
      A: a
        ? {
            ...a,
            last_valid_activity_age_ms: Number.isFinite(a.last_valid_activity_age_ms)
              ? a.last_valid_activity_age_ms + age
              : null
          }
        : null,
      B: peer ? { ...peer, sample_age_ms: peer.sample_age_ms + age } : null,
      BChannel: b?.channel
    });
    if (checked.state !== 'TWO_INPUTS_HEALTHY' || checked.alarm)
      return { state: 'DEGRADED', reason: 'INGRESS_EVIDENCE_EXPIRED', alarm: true };
    return { state: 'HEALTHY', reason: 'TWO_INPUTS_HEALTHY', alarm: false };
  }
  if (mode === 'B') {
    const h = value.peer,
      d = value.diagnostics;
    if (
      value.state !== 'RUNNING' ||
      h?.phase !== 'OPEN' ||
      !Number.isFinite(h.last_valid_activity_age_ms) ||
      h.last_valid_activity_age_ms < 0 ||
      h.last_valid_activity_age_ms + age >= 8000
    )
      return { state: 'DEGRADED', reason: 'B_NOT_RECEIVING', alarm: true };
    if (value.capture?.failures > 0)
      return { state: 'DEGRADED', reason: 'B_CAPTURE_INCOMPLETE', alarm: true };
    if (value.channel !== 'READY')
      return { state: 'DEGRADED', reason: 'B_FAST_CHANNEL_NOT_READY', alarm: true };
    if (
      d?.started !== true ||
      d.disabled !== false ||
      d.errors !== 0 ||
      d.dropped !== 0 ||
      !(d.protected_retention_ms >= 48 * 3600000)
    )
      return { state: 'DEGRADED', reason: 'B_JOURNAL_NOT_PROTECTED', alarm: true };
    return { state: 'HEALTHY', reason: 'B_RECEIVING_JOURNAL_PROTECTED', alarm: false };
  }
  throw Error('CHECK_MODE_INVALID');
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  let result;
  try {
    const [mode, file] = process.argv.slice(2);
    if (process.argv.length !== 4 || !['FAST', 'B'].includes(mode) || !file.startsWith('/'))
      throw Error('CHECK_ARGUMENTS_INVALID');
    const fd = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    let value;
    try {
      const s = fs.fstatSync(fd);
      if (!s.isFile() || s.size > 1048576) throw Error('CHECK_FILE_INVALID');
      value = JSON.parse(fs.readFileSync(fd, 'utf8'));
    } finally {
      fs.closeSync(fd);
    }
    result = checkIngressHealth(value, { mode });
  } catch {
    result = { state: 'DEGRADED', reason: 'STATUS_UNREADABLE_OR_INVALID', alarm: true };
  }
  console.log(JSON.stringify(result));
  process.exitCode = result.alarm ? 2 : 0;
}
