// Authenticated, per-session wallet-set updates for the independent receiver.
// This module is inert: callers own the transport and active subscription query.
import { createHmac, timingSafeEqual } from 'node:crypto';

const validHex = (x) => typeof x === 'string' && /^[a-f0-9]{64}$/.test(x);
function normalizeWallets(wallets) {
  if (
    !Array.isArray(wallets) ||
    wallets.length > 1000 ||
    wallets.some((x) => typeof x !== 'string' || !/^0x[a-f0-9]{40}$/.test(x))
  )
    throw Error('SCOPE_WALLETS_INVALID');
  const sorted = [...wallets].sort();
  if (new Set(sorted).size !== sorted.length) throw Error('SCOPE_WALLETS_INVALID');
  return sorted;
}
function signature(key, runId, nonce, revision, wallets) {
  return createHmac('sha256', key)
    .update(JSON.stringify(['RTDS_B_SCOPE_V1', runId, nonce, revision, wallets]))
    .digest('hex');
}
function checkConfig(runId, nonce, key) {
  if (
    typeof runId !== 'string' ||
    !/^[a-z0-9][a-z0-9-]{7,95}$/.test(runId) ||
    !validHex(nonce) ||
    !Buffer.isBuffer(key) ||
    key.length !== 32
  )
    throw Error('SCOPE_SYNC_CONFIG_INVALID');
}
export function createScopePublisher({ runId, nonce, key }) {
  checkConfig(runId, nonce, key);
  const secret = Buffer.from(key);
  let revision = 0,
    closed = false;
  return Object.freeze({
    update(wallets) {
      if (closed || revision >= Number.MAX_SAFE_INTEGER) throw Error('SCOPE_SYNC_CLOSED');
      const normalized = normalizeWallets(wallets),
        next = ++revision;
      return {
        kind: 'SCOPE_UPDATE',
        revision: next,
        wallets: normalized,
        mac: signature(secret, runId, nonce, next, normalized)
      };
    },
    close() {
      closed = true;
      secret.fill(0);
    }
  });
}
export function createScopeSubscriber({ runId, nonce, key, apply }) {
  checkConfig(runId, nonce, key);
  if (typeof apply !== 'function') throw Error('SCOPE_SYNC_CONFIG_INVALID');
  const secret = Buffer.from(key);
  let revision = 0,
    closed = false;
  return Object.freeze({
    receive(frame) {
      if (
        closed ||
        frame?.kind !== 'SCOPE_UPDATE' ||
        !Number.isSafeInteger(frame.revision) ||
        frame.revision !== revision + 1 ||
        !validHex(frame.mac)
      )
        throw Error('SCOPE_UPDATE_INVALID');
      const wallets = normalizeWallets(frame.wallets);
      if (JSON.stringify(wallets) !== JSON.stringify(frame.wallets)) throw Error('SCOPE_UPDATE_INVALID');
      const expected = signature(secret, runId, nonce, frame.revision, wallets);
      if (!timingSafeEqual(Buffer.from(expected, 'hex'), Buffer.from(frame.mac, 'hex')))
        throw Error('SCOPE_UPDATE_INVALID');
      // Advance only after the receiver accepted the new set. A failure closes
      // the channel at its caller and may never silently retain a stale scope.
      apply(wallets);
      revision = frame.revision;
      return { revision, wallet_count: wallets.length };
    },
    close() {
      closed = true;
      secret.fill(0);
    }
  });
}
