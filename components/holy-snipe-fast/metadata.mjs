const CONDITION = /^0x[0-9a-f]{64}$/i;
const SLUG = /^[a-z0-9][a-z0-9_-]{0,199}$/i;
const GAMMA = 'https://gamma-api.polymarket.com';

function nonempty(value) {
  return typeof value === 'string' && value.trim() ? value.trim().slice(0, 300) : '';
}

function array(value) {
  if (Array.isArray(value)) return value;
  if (typeof value !== 'string') return [];
  try { const parsed = JSON.parse(value); return Array.isArray(parsed) ? parsed : []; }
  catch { return []; }
}

export function rawMetadata(payload) {
  const market = payload?.market && typeof payload.market === 'object' ? payload.market : {};
  const event = payload?.event && typeof payload.event === 'object' ? payload.event : {};
  const condition = nonempty(payload?.conditionId || payload?.condition_id || market.conditionId ||
    (typeof payload?.market === 'string' && CONDITION.test(payload.market) ? payload.market : ''));
  return {
    asset: nonempty(payload?.asset || payload?.tokenId || payload?.token_id),
    conditionId: CONDITION.test(condition) ? condition.toLowerCase() : '',
    eventSlug: nonempty(payload?.eventSlug || payload?.event_slug || event.slug || market.eventSlug),
    slug: nonempty(payload?.slug || payload?.marketSlug || market.slug),
    title: nonempty(payload?.title || payload?.marketTitle || payload?.question || market.title || market.question),
    outcome: nonempty(payload?.outcome || market.outcome)
  };
}

export function cacheKeys(hints) {
  const keys = [];
  if (hints.asset) keys.push(`asset:${hints.asset}`);
  if (hints.conditionId) keys.push(`condition:${hints.conditionId.toLowerCase()}`);
  if (SLUG.test(hints.slug || '')) keys.push(`slug:${hints.slug.toLowerCase()}`);
  // An event can contain several markets: an event slug alone is never a safe cache key.
  if (SLUG.test(hints.eventSlug || '') && hints.asset)
    keys.push(`event_asset:${hints.eventSlug.toLowerCase()}:${hints.asset}`);
  return keys;
}

export function findCachedMetadata(hints, readKey) {
  for (const key of cacheKeys(hints)) {
    const value = readKey(key);
    if (value?.title && value?.slug) return key.startsWith('asset:') || key.startsWith('event_asset:')
      ? value : { ...value,outcome:'' };
  }
  return null;
}

export function mergeMetadata(trade, metadata) {
  if (!metadata) return trade;
  return { ...trade, market:trade.market || metadata.title || '', slug:trade.slug || metadata.slug || '',
    outcome:trade.outcome || metadata.outcome || '' };
}

export function gammaMarketMetadata(row, hints) {
  if (!row || typeof row !== 'object') return null;
  // Both fields can exist on a CTF market. Never infer the protocol from
  // positionIds presence, and never round a position identifier via Number.
  const version = row.version ?? 'v1';
  if (version !== 'v1' && version !== 'v2') return null;
  const tokens = array(version === 'v2' ? row.positionIds : row.clobTokenIds);
  if (!tokens.length || tokens.some(id => typeof id !== 'string' || !/^\d{1,100}$/.test(id)) ||
      new Set(tokens).size !== tokens.length) return null;
  const conditionId = nonempty(row.conditionId).toLowerCase();
  const slug = nonempty(row.slug);
  const eventSlug = nonempty(row.events?.[0]?.slug);
  if (hints.conditionId && conditionId !== hints.conditionId.toLowerCase()) return null;
  if (hints.slug && slug !== hints.slug && eventSlug !== hints.slug) return null;
  if (hints.eventSlug && eventSlug && eventSlug !== hints.eventSlug) return null;
  const tokenIndex = tokens.indexOf(String(hints.asset));
  if (tokenIndex < 0) return null;
  const title = nonempty(row.question || row.title);
  if (!title || !SLUG.test(slug)) return null;
  const outcomes = array(row.outcomes);
  if (outcomes.length !== tokens.length) return null;
  const outcome = nonempty(outcomes[tokenIndex]);
  return { title,slug,eventSlug,outcome,conditionId,
    marketUrl:`https://polymarket.com/market/${encodeURIComponent(slug)}`,updatedAt:Date.now() };
}

export async function lookupGammaMetadata(hints, { fetchImpl=fetch, timeoutMs=2000 } = {}) {
  const urls = [];
  if (SLUG.test(hints.slug || '')) urls.push(`${GAMMA}/markets/slug/${encodeURIComponent(hints.slug)}`);
  if (hints.conditionId) {
    for (const version of ['v1','v2'])
      urls.push(`${GAMMA}/markets?condition_ids=${encodeURIComponent(hints.conditionId)}&limit=2&closed=any&version=${version}`);
  }
  if (/^\d{1,100}$/.test(hints.asset || '')) {
    urls.push(`${GAMMA}/markets?clob_token_ids=${encodeURIComponent(hints.asset)}&limit=2&closed=any&version=v1`);
    urls.push(`${GAMMA}/markets?position_ids=${encodeURIComponent(hints.asset)}&limit=2&closed=any&version=v2`);
  }
  if (SLUG.test(hints.eventSlug || ''))
    urls.push(`${GAMMA}/events/slug/${encodeURIComponent(hints.eventSlug)}`);
  if (!urls.length) return null;
  const deadline = Date.now() + Math.min(2000, Math.max(0, timeoutMs));
  for (const url of urls) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) break;
    try {
      const response = await fetchImpl(url, { signal:AbortSignal.timeout(remaining) });
      if (!response.ok) { await response.body?.cancel(); continue; }
      const body = await response.json();
      if (Date.now() >= deadline) return null;
      const rows = Array.isArray(body) ? body : Array.isArray(body?.markets) ? body.markets : [body];
      for (const row of rows) {
        const metadata = gammaMarketMetadata(row,hints);
        if (metadata) return metadata;
      }
    } catch { /* A failed metadata lookup must never discard a trade. */ }
  }
  return null;
}

export function shortAsset(asset) {
  const text = String(asset || '');
  return text.length > 18 ? `${text.slice(0, 8)}...${text.slice(-6)}` : text;
}
