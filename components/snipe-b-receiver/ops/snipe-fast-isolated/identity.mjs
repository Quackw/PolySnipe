import { createHash } from 'node:crypto';

const ADDRESS = /^0x[0-9a-f]{40}$/;
const HASH = /^0x[0-9a-f]{64}$/;

export function normalizeAddress(value) {
  const result = typeof value === 'string' ? value.trim().toLowerCase() : '';
  return ADDRESS.test(result) ? result : null;
}

function decimal(value) {
  if (typeof value !== 'number' && (typeof value !== 'string' || !value.trim())) return null;
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return null;
  const fixed = n.toFixed(8);
  if (Number(fixed) <= 0) return null;
  return fixed.replace(/0+$/, '').replace(/\.$/, '');
}

export function canonicalTrade(payload, monitoredWallet, baselineMs, nowMs = Date.now()) {
  if (!payload || typeof payload !== 'object') return null;
  const wallet = normalizeAddress(payload.proxyWallet);
  if (!wallet || wallet !== normalizeAddress(monitoredWallet)) return null;
  const hash = typeof payload.transactionHash === 'string' ? payload.transactionHash.toLowerCase() : '';
  if (!HASH.test(hash)) return null;
  const asset = typeof payload.asset === 'string' ? payload.asset.trim() : '';
  if (!asset || asset.length > 256) return null;
  const side = payload.side === 'BUY' || payload.side === 'SELL' ? payload.side : null;
  const size = decimal(payload.size);
  const price =
    typeof payload.price === 'number' || (typeof payload.price === 'string' && payload.price.trim())
      ? Number(payload.price)
      : NaN;
  const timestamp = Number(payload.timestamp);
  if (!side || !size || !Number.isFinite(price) || price < 0 || price > 1 || !Number.isSafeInteger(timestamp))
    return null;
  if (timestamp * 1000 < baselineMs || timestamp * 1000 > nowMs + 5000) return null;
  const safe = (value) => (typeof value === 'string' ? value.slice(0, 300) : '');
  const usdcSize = Number(payload.usdcSize);
  return {
    wallet,
    hash,
    asset,
    side,
    size,
    price,
    timestamp,
    market: safe(payload.title),
    outcome: safe(payload.outcome),
    slug: safe(payload.slug),
    usdcSize: Number.isFinite(usdcSize) && usdcSize >= 0 ? usdcSize : null
  };
}

export function eventKey(trade) {
  return createHash('sha256')
    .update([trade.wallet, trade.hash, trade.asset, trade.side, trade.size].join('|'))
    .digest('hex');
}

// One alert per user action; independent of fill size and source representation.
export function userActionIdentity(trade) {
  return JSON.stringify([trade.wallet, trade.hash, trade.asset, trade.side]);
}

export function userActionKey(trade) {
  return createHash('sha256')
    .update([trade.wallet, trade.hash, trade.asset, trade.side].join('|'))
    .digest('hex');
}

export function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function activityNumber(value, maximumFractionDigits) {
  return Number(value).toLocaleString('en-US', { useGrouping: false, maximumFractionDigits });
}

function displayPriceCents(value) {
  const cents = Number(value) * 100;
  const digits = cents >= 1 ? 2 : cents >= 0.01 ? 4 : cents >= 0.0001 ? 6 : 8;
  return activityNumber(cents, digits);
}

export function formatAlert(trade, account = {}) {
  const shortWallet = `${trade.wallet.slice(0, 6)}...${trade.wallet.slice(-4)}`;
  const profileSlug =
    typeof account.profileSlug === 'string' ? account.profileSlug.trim().replace(/^@/, '').slice(0, 120) : '';
  const name =
    [profileSlug ? `@${profileSlug}` : '', account.name, account.pseudonym]
      .find((value) => typeof value === 'string' && value.trim())
      ?.trim()
      .slice(0, 120) || shortWallet;
  const profile = profileSlug
    ? `https://polymarket.com/@${encodeURIComponent(profileSlug)}`
    : `https://polymarket.com/${trade.wallet}`;
  const exactAmount =
    trade.usdcSize !== null &&
    trade.usdcSize !== undefined &&
    Number.isFinite(Number(trade.usdcSize)) &&
    (Number(trade.usdcSize) > 0 || Number(trade.price) === 0);
  const amount = exactAmount ? Number(trade.usdcSize) : Number(trade.size) * Number(trade.price);
  const amountText = `$${amount.toFixed(2)}`;
  const details = [
    '<b>TRADE</b>',
    trade.outcome ? `outcome: <b>${escapeHtml(trade.outcome)}</b>` : null,
    `side: <b>${escapeHtml(trade.side)}</b>`,
    `shares: <b>${escapeHtml(activityNumber(trade.size, 2))}</b>`,
    `avg price: <b>${escapeHtml(displayPriceCents(trade.price))}¢</b>`,
    `usdcSize: <b>${escapeHtml(amountText)}</b>`
  ]
    .filter(Boolean)
    .join(', ');
  const asset = String(trade.asset || '');
  const shortAsset = asset.length > 18 ? `${asset.slice(0, 8)}...${asset.slice(-6)}` : asset;
  const marketTitle = escapeHtml(trade.market || `Market unavailable · asset ${shortAsset}`);
  const market = trade.slug
    ? `<a href="https://polymarket.com/market/${encodeURIComponent(trade.slug)}">${marketTitle}</a>`
    : marketTitle;
  return `<a href="${profile}">${escapeHtml(name)}</a>\n\n${market} | ${details}`;
}
