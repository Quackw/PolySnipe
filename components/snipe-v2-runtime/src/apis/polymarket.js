import { getJson, getText } from "./http.js";
import { SlidingWindowRateGate } from "../utils/rateGate.js";
import { filterAllowedActivities } from "../services/activity.js";
const GAMMA_BASE_URL = "https://gamma-api.polymarket.com";
const DATA_BASE_URL = "https://data-api.polymarket.com";
function parseFiniteNumber(value) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
}
function parseString(value) {
    return typeof value === "string" && value.length > 0 ? value : undefined;
}
function parsePublicProfile(raw) {
    if (!raw || typeof raw !== "object") {
        return null;
    }
    const item = raw;
    const proxyWallet = parseString(item.proxyWallet)?.toLowerCase();
    if (!proxyWallet) {
        return null;
    }
    return {
        proxyWallet,
        name: parseString(item.name),
        pseudonym: parseString(item.pseudonym)
    };
}
function parseSearchProfiles(raw) {
    if (!raw || typeof raw !== "object") {
        return [];
    }
    const response = raw;
    const profiles = Array.isArray(response.profiles) ? response.profiles : [];
    const out = [];
    for (const row of profiles) {
        if (!row || typeof row !== "object") {
            continue;
        }
        const item = row;
        const proxyAddress = parseString(item.proxyWallet)?.toLowerCase();
        if (!proxyAddress) {
            continue;
        }
        const name = parseString(item.name);
        out.push({
            proxyAddress,
            name,
            pseudonym: parseString(item.pseudonym),
            profileSlug: name
        });
    }
    return out;
}
function parseUserActivity(raw) {
    if (!Array.isArray(raw)) {
        throw new Error("Unexpected activity response");
    }
    const out = [];
    for (const row of raw) {
        if (!row || typeof row !== "object") {
            continue;
        }
        const item = row;
        const proxyWallet = parseString(item.proxyWallet)?.toLowerCase();
        const type = parseString(item.type)?.toUpperCase();
        const timestamp = parseFiniteNumber(item.timestamp);
        if (!proxyWallet || !type || timestamp === null) {
            continue;
        }
        out.push({
            proxyWallet,
            timestamp,
            type,
            conditionId: parseString(item.conditionId)?.toLowerCase(),
            transactionHash: parseString(item.transactionHash),
            asset: parseString(item.asset),
            side: parseString(item.side)?.toUpperCase(),
            size: parseFiniteNumber(item.size) ?? undefined,
            usdcSize: parseFiniteNumber(item.usdcSize) ?? undefined,
            price: parseFiniteNumber(item.price) ?? undefined,
            outcomeIndex: parseFiniteNumber(item.outcomeIndex) ?? undefined,
            title: parseString(item.title),
            slug: parseString(item.slug),
            eventSlug: parseString(item.eventSlug),
            outcome: parseString(item.outcome),
            name: parseString(item.name),
            pseudonym: parseString(item.pseudonym)
        });
    }
    return out;
}
export class DefaultPolymarketClient {
    config;
    dataGate;
    gammaGate;
    constructor(config) {
        this.config = config;
        this.dataGate = new SlidingWindowRateGate(config.activitySafeRps * 10, 10_000, config.activityConcurrency);
        this.gammaGate = new SlidingWindowRateGate(100, 10_000, 5);
    }
    async resolveAccountInput(input) {
        if (input.kind === "proxy_address") {
            const profile = await this.fetchPublicProfile(input.proxyAddress);
            return {
                proxyAddress: input.proxyAddress,
                name: profile?.name,
                pseudonym: profile?.pseudonym,
                profileSlug: profile?.name
            };
        }
        const slug = input.slug;
        const searchUrl = `${GAMMA_BASE_URL}/public-search?${new URLSearchParams({
            q: slug,
            search_profiles: "true",
            limit_per_type: "10"
        }).toString()}`;
        const { data: searchRaw } = await this.getGammaJson(searchUrl);
        const matches = parseSearchProfiles(searchRaw);
        const exact = matches.find((item) => item.profileSlug?.toLowerCase() === slug.toLowerCase());
        if (exact) {
            return exact;
        }
        const htmlUrl = `https://polymarket.com/@${encodeURIComponent(slug)}`;
        const { data: html } = await this.getGammaText(htmlUrl);
        const proxyAddress = html.match(/"proxyAddress":"([^"]+)"/)?.[1]?.toLowerCase();
        if (!proxyAddress) {
            throw new Error(`Unable to resolve proxy address for @${slug}`);
        }
        const profile = await this.fetchPublicProfile(proxyAddress);
        return {
            proxyAddress,
            name: profile?.name,
            pseudonym: profile?.pseudonym,
            profileSlug: profile?.name ?? slug
        };
    }
    async fetchRecentUserActivity(proxyAddress, startTs, endTs, hooks) {
        const out = [];
        let apiCalls = 0;
        let retries = 0;
        let throttleWaitMs = 0;
        for (let page = 0; page < this.config.activityMaxPages; page += 1) {
            const params = new URLSearchParams({
                user: proxyAddress.toLowerCase(),
                start: String(Math.max(0, startTs)),
                end: String(endTs),
                limit: String(this.config.activityPageLimit),
                offset: String(page * this.config.activityPageLimit),
                sortBy: "TIMESTAMP",
                sortDirection: "DESC"
            });
            const url = `${DATA_BASE_URL}/activity?${params.toString()}`;
            const response = await this.getDataJson(url, {
                onRetry: () => {
                    retries += 1;
                    hooks?.onRetry?.();
                }
            });
            apiCalls += 1;
            throttleWaitMs += response.waitMs;
            const pageActivities = parseUserActivity(response.data);
            out.push(...pageActivities);
            if (pageActivities.length < this.config.activityPageLimit) {
                break;
            }
            const oldestTimestamp = Math.min(...pageActivities.map((item) => item.timestamp));
            if (oldestTimestamp < startTs) {
                break;
            }
        }
        const activities = filterAllowedActivities(out).sort((a, b) => b.timestamp - a.timestamp);
        return {
            activities,
            apiCalls,
            retries,
            throttleWaitMs
        };
    }
    async fetchPublicProfile(address) {
        const url = `${GAMMA_BASE_URL}/public-profile?${new URLSearchParams({ address }).toString()}`;
        try {
            const { data } = await this.getGammaJson(url);
            return parsePublicProfile(data);
        }
        catch (error) {
            const code = error.statusCode;
            if (code === 404) {
                return null;
            }
            throw error;
        }
    }
    async getDataJson(url, hooks) {
        let waitMs = 0;
        const data = await getJson(url, {
            timeoutMs: this.config.polymarketTimeoutMs,
            retries: this.config.polymarketRetries,
            beforeAttempt: async () => {
                const ticket = await this.dataGate.acquire();
                waitMs += ticket.waitMs;
                return ticket.release;
            },
            onRateLimit: (delayMs) => this.dataGate.pauseFor(Math.max(delayMs, 15_000)),
            onRetry: () => {
                hooks?.onRetry?.();
            }
        });
        return { data, waitMs };
    }
    async getGammaJson(url) {
        const ticket = await this.gammaGate.acquire();
        try {
            const data = await getJson(url, {
                timeoutMs: this.config.polymarketTimeoutMs,
                retries: this.config.polymarketRetries
            });
            return { data, waitMs: ticket.waitMs };
        }
        finally {
            ticket.release();
        }
    }
    async getGammaText(url) {
        const ticket = await this.gammaGate.acquire();
        try {
            const data = await getText(url, {
                timeoutMs: this.config.polymarketTimeoutMs,
                retries: this.config.polymarketRetries
            });
            return { data, waitMs: ticket.waitMs };
        }
        finally {
            ticket.release();
        }
    }
}
