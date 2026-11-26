import { getJson, getText, type RequestOptions } from "./http.js";
import { SlidingWindowRateGate } from "../utils/rateGate.js";
import type {
  ActivityPageResult,
  AppConfig,
  PublicProfile,
  ResolvedAccount,
  UserActivity
} from "../types.js";
import { buildActivityKey, filterAllowedActivities } from "../services/activity.js";
import type { ParsedAccountInput } from "../subscriptionInput.js";

const GAMMA_BASE_URL = "https://gamma-api.polymarket.com";
const DATA_BASE_URL = "https://data-api.polymarket.com";

type PublicSearchProfile = {
  name?: unknown;
  pseudonym?: unknown;
  proxyWallet?: unknown;
};

type PublicSearchResponse = {
  profiles?: unknown;
};

type PublicProfileResponse = {
  proxyWallet?: unknown;
  name?: unknown;
  pseudonym?: unknown;
  displayUsernamePublic?: unknown;
};

function parseString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function parsePublicProfile(raw: unknown): PublicProfile | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }

  const item = raw as Record<string, unknown>;
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

function parseSearchProfiles(raw: unknown): ResolvedAccount[] {
  if (!raw || typeof raw !== "object") {
    return [];
  }

  const response = raw as PublicSearchResponse;
  const profiles = Array.isArray(response.profiles) ? response.profiles : [];
  const out: ResolvedAccount[] = [];

  for (const row of profiles) {
    if (!row || typeof row !== "object") {
      continue;
    }

    const item = row as PublicSearchProfile;
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

function activityString(value: unknown, field: string): string | undefined {
  if (value === undefined || value === null || value === "") {
    return undefined;
  }
  if (typeof value !== "string") {
    throw new Error(`Invalid activity field: ${field}`);
  }
  return value;
}

function activityNumber(value: unknown, field: string): number | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (
    (typeof value !== "number" && typeof value !== "string") ||
    (typeof value === "string" && value.trim() === "")
  ) {
    throw new Error(`Invalid activity field: ${field}`);
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid activity field: ${field}`);
  }
  return parsed;
}

function parseActivityTimestamp(value: unknown): number | undefined {
  let timestamp: number;
  if (typeof value === "number") {
    timestamp = value;
  } else if (typeof value === "string" && value.trim() !== "") {
    timestamp = Number(value);
    if (Number.isNaN(timestamp)) {
      timestamp = Math.floor(Date.parse(value) / 1000);
    }
  } else {
    return undefined;
  }

  if (!Number.isSafeInteger(timestamp) || timestamp <= 0) return undefined;
  return timestamp >= 1_000_000_000_000 ? Math.floor(timestamp / 1000) : timestamp;
}

function parseActivitySide(value: unknown): UserActivity["side"] {
  const side = activityString(value, "side")?.trim().toUpperCase();
  if (!side) return undefined;
  if (side === "BUY" || side === "SELL") return side;
  throw new Error("Invalid activity field: side");
}

// Normalize v2 to the existing identity fields so persisted v1 watermark keys still match.
// The v1 mode is only for compatibility fixtures; live requests require the v2 envelope.
export function parseUserActivity(raw: unknown, version: "v1" | "v2"): UserActivity[] {
  if (!Array.isArray(raw)) {
    throw new Error("Unexpected activity response");
  }

  const out: UserActivity[] = [];
  for (const row of raw) {
    if (!row || typeof row !== "object" || Array.isArray(row)) {
      throw new Error("Invalid activity row");
    }

    const item = row as Record<string, unknown>;
    const field = (v1: string, v2: string): unknown => item[version === "v1" ? v1 : v2];
    const proxyWallet = activityString(field("proxyWallet", "proxy_wallet"), "proxy_wallet")?.toLowerCase();
    const type = activityString(item.type, "type")?.toUpperCase();
    const timestamp = parseActivityTimestamp(item.timestamp);

    if (!proxyWallet || !type || timestamp === undefined) {
      throw new Error("Invalid activity identity or timestamp");
    }

    const activity: UserActivity = {
      proxyWallet,
      timestamp,
      type,
      conditionId: activityString(field("conditionId", "condition_id"), "condition_id")?.toLowerCase(),
      transactionHash: activityString(field("transactionHash", "transaction_hash"), "transaction_hash"),
      asset: activityString(field("asset", "token_id"), "token_id"),
      side: parseActivitySide(item.side),
      size: activityNumber(item.size, "size"),
      usdcSize: activityNumber(field("usdcSize", "usdc_size"), "usdc_size"),
      price: activityNumber(item.price, "price"),
      outcomeIndex: activityNumber(field("outcomeIndex", "outcome_index"), "outcome_index"),
      title: activityString(item.title, "title"),
      slug: activityString(item.slug, "slug"),
      eventSlug: activityString(field("eventSlug", "event_slug"), "event_slug"),
      outcome: activityString(item.outcome, "outcome"),
      name: activityString(item.name, "name"),
      pseudonym: activityString(item.pseudonym, "pseudonym")
    };
    // v1 coerced explicit null amounts to zero in persisted watermark keys.
    // Preserve that identity only; unknown v2 amounts remain undefined for display.
    activity.legacyActivityKey = buildActivityKey({
      ...activity,
      size: item.size === null ? 0 : activity.size,
      usdcSize: field("usdcSize", "usdc_size") === null ? 0 : activity.usdcSize
    });
    out.push(activity);
  }

  return out;
}

function parseActivityPage(raw: unknown): { activities: UserActivity[]; nextCursor: string | null } {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    throw new Error("Unexpected v2 activity envelope");
  }
  const page = raw as Record<string, unknown>;
  if (!page.pagination || typeof page.pagination !== "object" || Array.isArray(page.pagination)) {
    throw new Error("Missing v2 activity pagination");
  }
  const pagination = page.pagination as Record<string, unknown>;
  // Only an explicit null marks the final page. A missing cursor is incomplete.
  const nextCursor = pagination.next_cursor;
  if (nextCursor !== null && (typeof nextCursor !== "string" || nextCursor.trim() === "")) {
    throw new Error("Invalid or missing v2 activity next_cursor");
  }
  if (pagination.has_more !== (nextCursor !== null)) {
    throw new Error("Inconsistent v2 activity has_more");
  }
  return { activities: parseUserActivity(page.data, "v2"), nextCursor };
}

export interface ResolveActivityHooks {
  bootstrapOnly?: boolean;
  onRetry?: (() => void) | undefined;
}

export interface PolymarketClient {
  resolveAccountInput(input: ParsedAccountInput): Promise<ResolvedAccount>;
  fetchRecentUserActivity(
    proxyAddress: string,
    startTs: number,
    endTs: number,
    hooks?: ResolveActivityHooks
  ): Promise<ActivityPageResult>;
}

export class DefaultPolymarketClient implements PolymarketClient {
  private readonly dataGate: SlidingWindowRateGate;
  private readonly gammaGate: SlidingWindowRateGate;

  public constructor(private readonly config: AppConfig) {
    this.dataGate = new SlidingWindowRateGate(
      config.activitySafeRps * 10,
      10_000,
      config.activityConcurrency
    );
    this.gammaGate = new SlidingWindowRateGate(100, 10_000, 5);
  }

  public async resolveAccountInput(input: ParsedAccountInput): Promise<ResolvedAccount> {
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
    const { data: searchRaw } = await this.getGammaJson<unknown>(searchUrl);
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

  public async fetchRecentUserActivity(
    proxyAddress: string,
    startTs: number,
    endTs: number,
    hooks?: ResolveActivityHooks
  ): Promise<ActivityPageResult> {
    const out: UserActivity[] = [];
    let apiCalls = 0;
    let retries = 0;
    let throttleWaitMs = 0;
    let cursor: string | undefined;
    const seenCursors = new Set<string>();

    for (let page = 0; page < this.config.activityMaxPages; page += 1) {
      const params = new URLSearchParams({
        user: proxyAddress.toLowerCase(),
        start: String(Math.max(0, startTs)),
        end: String(endTs),
        sort_by: "TIMESTAMP",
        sort_direction: "DESC"
      });
      if (cursor !== undefined) {
        params.set("cursor", cursor);
      } else {
        params.set("limit", String(this.config.activityPageLimit));
      }

      const url = `${DATA_BASE_URL}/v2/activity?${params.toString()}`;
      const response = await this.getDataJson<unknown>(url, {
        onRetry: () => {
          retries += 1;
          hooks?.onRetry?.();
        }
      });

      apiCalls += 1;
      throttleWaitMs += response.waitMs;

      const activityPage = parseActivityPage(response.data);
      if (activityPage.activities.length > this.config.activityPageLimit) {
        throw new Error("Oversized v2 activity page");
      }
      for (const row of activityPage.activities) {
        if (
          row.proxyWallet !== proxyAddress.toLowerCase() ||
          row.timestamp < startTs ||
          row.timestamp > endTs ||
          (out.length > 0 && row.timestamp > out[out.length - 1]!.timestamp)
        ) {
          throw new Error("V2 activity outside requested wallet/window/order");
        }
        out.push(row);
      }
      const allowed = filterAllowedActivities(out);
      const latest = allowed[0]?.timestamp;
      // Bootstrap sends no history. Stop only after the entire newest timestamp
      // group is covered (including across page boundaries), not after a row count.
      const bootstrapCovered =
        hooks?.bootstrapOnly === true && latest !== undefined && out.some((row) => row.timestamp < latest);

      if (activityPage.nextCursor !== null && seenCursors.has(activityPage.nextCursor)) {
        throw new Error("Repeated v2 activity cursor; refusing partial poll");
      }
      // Docs checked 2026-10-23: only explicit null means complete, even on empty/short pages.
      if (activityPage.nextCursor === null || bootstrapCovered) {
        const activities = allowed.sort((a, b) => b.timestamp - a.timestamp);
        if (new Set(activities.filter((row) => row.timestamp === latest).map(buildActivityKey)).size > 500) {
          throw new Error("Activity timestamp exceeds persisted watermark capacity");
        }
        return { activities, apiCalls, retries, throttleWaitMs };
      }
      seenCursors.add(activityPage.nextCursor);
      cursor = activityPage.nextCursor;
    }

    // Throw before the scheduler can persist a watermark or send from an incomplete traversal.
    throw new Error("V2 activity page budget exhausted; refusing partial poll");
  }

  private async fetchPublicProfile(address: string): Promise<PublicProfile | null> {
    const url = `${GAMMA_BASE_URL}/public-profile?${new URLSearchParams({ address }).toString()}`;

    try {
      const { data } = await this.getGammaJson<unknown>(url);
      return parsePublicProfile(data);
    } catch (error) {
      const code = (error as Error & { statusCode?: number }).statusCode;
      if (code === 404) {
        return null;
      }
      throw error;
    }
  }

  private async getDataJson<T>(
    url: string,
    hooks?: { onRetry?: (() => void) | undefined }
  ): Promise<{ data: T; waitMs: number }> {
    let waitMs = 0;
    const data = await getJson<T>(url, {
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

  private getGammaJson<T>(url: string): Promise<{ data: T; waitMs: number }> {
    return this.getGamma(url, getJson<T>);
  }

  private getGammaText(url: string): Promise<{ data: string; waitMs: number }> {
    return this.getGamma(url, getText);
  }

  private async getGamma<T>(
    url: string,
    request: (url: string, options: RequestOptions) => Promise<T>
  ): Promise<{ data: T; waitMs: number }> {
    let waitMs = 0;
    const data = await request(url, {
      timeoutMs: this.config.polymarketTimeoutMs,
      retries: this.config.polymarketRetries,
      beforeAttempt: async () => {
        const ticket = await this.gammaGate.acquire();
        waitMs += ticket.waitMs;
        return ticket.release;
      },
      onRateLimit: (delayMs) => this.gammaGate.pauseFor(delayMs)
    });
    return { data, waitMs };
  }
}
