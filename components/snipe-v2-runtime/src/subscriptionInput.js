const POLYMARKET_HOSTS = new Set(["polymarket.com", "www.polymarket.com"]);
const ADDRESS_PATTERN = /^0x[a-fA-F0-9]{40}$/i;
function cleanupCandidate(candidate) {
    let value = candidate.trim();
    if (value.startsWith("<") && value.endsWith(">")) {
        value = value.slice(1, -1);
    }
    while (/[),.;!?>\]]$/.test(value)) {
        value = value.slice(0, -1);
    }
    return value;
}
export function normalizeProxyAddress(value) {
    const trimmed = value.trim();
    if (!ADDRESS_PATTERN.test(trimmed)) {
        return null;
    }
    return trimmed.toLowerCase();
}
export function parseAccountInput(input) {
    const raw = cleanupCandidate(input);
    if (raw.length === 0) {
        return null;
    }
    const normalizedAddress = normalizeProxyAddress(raw);
    if (normalizedAddress) {
        return { kind: "proxy_address", proxyAddress: normalizedAddress };
    }
    let url;
    try {
        url = new URL(raw);
    }
    catch {
        return null;
    }
    if (!POLYMARKET_HOSTS.has(url.hostname.toLowerCase())) {
        return null;
    }
    const parts = url.pathname.split("/").filter(Boolean);
    if (parts.length !== 1) {
        return null;
    }
    const firstPart = parts[0] ?? "";
    if (firstPart.startsWith("@")) {
        const slug = firstPart.slice(1).trim();
        return slug.length > 0 ? { kind: "profile_slug", slug } : null;
    }
    const addressFromPath = normalizeProxyAddress(firstPart);
    if (addressFromPath) {
        return { kind: "proxy_address", proxyAddress: addressFromPath };
    }
    return null;
}
