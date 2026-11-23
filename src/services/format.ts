import { escapeHtml } from "../utils/html.js";
import type {
  AccountLabel,
  AdminPerformanceReport,
  AdminProfileStat,
  AdminSubscriberStats,
  AdminSubscriptionsReport,
  AdminUsersReport,
  ChatSubscriptionView,
  ResolvedAccount,
  UserActivity
} from "../types.js";

function truncate(value: string, maxLen: number): string {
  return value.length <= maxLen ? value : `${value.slice(0, maxLen - 3)}...`;
}

function formatActivityNumber(value: number): string {
  return value.toFixed(2).replace(/\.?0+$/, "");
}

function formatDurationMs(ms: number): string {
  return `${formatActivityNumber(ms / 1000)}s`;
}

function formatDelayReasonLabel(value: string): string {
  return value
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function formatActivityTitleLink(title?: string | undefined, slug?: string | undefined): string | undefined {
  if (!title) {
    return undefined;
  }

  const safeLabel = escapeHtml(truncate(title, 90));
  if (!slug) {
    return safeLabel;
  }

  const marketUrl = `https://polymarket.com/market/${encodeURIComponent(slug)}`;
  return `<a href="${marketUrl}">${safeLabel}</a>`;
}

function formatCurrency(value?: number | undefined): string | undefined {
  if (typeof value !== "number") {
    return undefined;
  }

  return `$${value.toFixed(2)}`;
}

function formatPriceCents(value?: number | undefined): string | undefined {
  if (typeof value !== "number") {
    return undefined;
  }

  const cents = value * 100;
  const digits = cents >= 1 ? 2 : cents >= 0.01 ? 4 : cents >= 0.0001 ? 6 : 8;
  return `${Number(cents.toFixed(digits))}¢`;
}

function formatActivityLine(activity: UserActivity): string {
  const market = formatActivityTitleLink(activity.title, activity.slug);
  const avgPrice = formatPriceCents(activity.price);
  const usdcSize = formatCurrency(activity.usdcSize);

  const details = [
    `<b>${escapeHtml(activity.type)}</b>`,
    activity.outcome ? `outcome: <b>${escapeHtml(activity.outcome)}</b>` : undefined,
    activity.side ? `side: <b>${escapeHtml(activity.side)}</b>` : undefined,
    typeof activity.size === "number" ? `shares: <b>${formatActivityNumber(activity.size)}</b>` : undefined,
    avgPrice ? `avg price: <b>${avgPrice}</b>` : undefined,
    usdcSize ? `usdcSize: <b>${usdcSize}</b>` : undefined
  ].filter((part): part is string => Boolean(part));

  if (!market) {
    return details.join(", ");
  }

  if (details.length === 0) {
    return market;
  }

  return `${market} | ${details.join(", ")}`;
}

export function buildAccountLabel(account: {
  proxyAddress: string;
  name?: string | undefined;
  pseudonym?: string | undefined;
  profileSlug?: string | undefined;
}): AccountLabel {
  if (account.profileSlug) {
    const title = `@${account.profileSlug}`;
    const subtitle = account.name && account.name !== account.profileSlug ? account.name : account.pseudonym;
    return { title, subtitle };
  }

  if (account.name) {
    return { title: account.name, subtitle: account.pseudonym };
  }

  if (account.pseudonym) {
    return { title: account.pseudonym, subtitle: shortenAddress(account.proxyAddress) };
  }

  return { title: shortenAddress(account.proxyAddress) };
}

export function shortenAddress(proxyAddress: string): string {
  return `${proxyAddress.slice(0, 6)}...${proxyAddress.slice(-4)}`;
}

export function renderStartGuide(): string {
  return [
    "<b>Polymarket Account Activity Bot</b>",
    "",
    "Track selected Polymarket accounts and receive alerts for new activity.",
    "",
    "How to use:",
    "1) Press <b>Subscribe</b>.",
    "2) Send one Polymarket profile link or wallet address.",
    "3) Press <b>Unsubscribe</b> to remove one subscription or all subscriptions in this chat.",
    "",
    "Accepted subscribe inputs:",
    "- <code>https://polymarket.com/@weflyhigh</code>",
    "- <code>https://polymarket.com/@weflyhigh?tab=positions</code>",
    "- <code>https://polymarket.com/0x03e8a544e97eeff5753bc1e90d46e5ef22af1697</code>",
    "- <code>0x03e8a544e97eeff5753bc1e90d46e5ef22af1697</code>"
  ].join("\n");
}

export function renderSubscribePrompt(): string {
  return [
    "Send one Polymarket profile URL or wallet address now.",
    "",
    "Examples:",
    "- <code>https://polymarket.com/@weflyhigh</code>",
    "- <code>https://polymarket.com/0x03e8a544e97eeff5753bc1e90d46e5ef22af1697</code>",
    "- <code>0x03e8a544e97eeff5753bc1e90d46e5ef22af1697</code>"
  ].join("\n");
}

export function renderSubscriptionCreated(account: ResolvedAccount, created: boolean): string {
  const label = buildAccountLabel(account);
  const profileUrl = account.profileSlug
    ? `https://polymarket.com/@${encodeURIComponent(account.profileSlug)}`
    : `https://polymarket.com/${account.proxyAddress}`;
  const status = created ? "Subscribed to" : "Already subscribed to";

  return `<b>${status}</b> <a href="${profileUrl}">${escapeHtml(label.title)}</a>`;
}

export function renderInvalidSubscribeInput(): string {
  return [
    "Couldn't parse that input.",
    "",
    "Send exactly one of:",
    "- a Polymarket profile URL",
    "- a Polymarket wallet URL",
    "- a raw wallet address"
  ].join("\n");
}

export function renderUnsubscribePrompt(subscriptions: readonly ChatSubscriptionView[], page: number, totalPages: number): string {
  return [
    "<b>Select account(s) to unsubscribe</b>",
    `Subscriptions: <b>${subscriptions.length}</b>`,
    `Page: <b>${page + 1}/${totalPages}</b>`
  ].join("\n");
}

export function renderUnsubscribed(account: {
  proxyAddress: string;
  name?: string | undefined;
  pseudonym?: string | undefined;
  profileSlug?: string | undefined;
}): string {
  const label = buildAccountLabel(account);
  const url = account.profileSlug
    ? `https://polymarket.com/@${encodeURIComponent(account.profileSlug)}`
    : `https://polymarket.com/${account.proxyAddress}`;

  return `<b>Unsubscribed from</b> <a href="${url}">${escapeHtml(label.title)}</a>`;
}

export function renderUnsubscribedAll(removedSubscriptions: number, removedTrackedAccounts: number): string {
  return [
    "<b>Removed all subscriptions in this chat</b>",
    `Subscriptions removed: <b>${removedSubscriptions}</b>`,
    `Tracked accounts removed: <b>${removedTrackedAccounts}</b>`
  ].join("\n");
}

export function renderActivityAlert(account: ResolvedAccount, activities: readonly UserActivity[]): string {
  const label = buildAccountLabel(account);
  const profileUrl = account.profileSlug
    ? `https://polymarket.com/@${encodeURIComponent(account.profileSlug)}`
    : `https://polymarket.com/${account.proxyAddress}`;

  const lines = [`<a href="${profileUrl}">${escapeHtml(label.title)}</a>`, ""];
  for (const activity of activities) {
    lines.push(formatActivityLine(activity));
  }

  return lines.join("\n");
}

export function renderAdminStats(stats: AdminSubscriberStats): string {
  const lines = [
    "<b>Admin stats</b>",
    `Total subscribers: <b>${stats.totalSubscribers}</b>`,
    `Total subscriptions: <b>${stats.totalSubscriptions}</b>`
  ];

  if (stats.rows.length === 0) {
    lines.push("", "No active subscribers.");
    return lines.join("\n");
  }

  lines.push("", "<b>Subscribers:</b>");
  for (const row of stats.rows) {
    lines.push(
      `- user <code>${row.telegramUserId}</code>, chat <code>${row.telegramChatId}</code>: <b>${row.subscriptionsCount}</b>`
    );
  }

  return lines.join("\n");
}

export function renderAdminMenuPrompt(): string {
  return [
    "<b>Admin reports</b>",
    "Choose a report:"
  ].join("\n");
}

export function renderAdminUsersReport(report: AdminUsersReport): string {
  return [
    "<b>Users</b>",
    `Users: <b>${report.totalUsers}</b>`,
    `Last user joined at: <b>${escapeHtml(report.lastUserJoinedAt ?? "n/a")}</b>`
  ].join("\n");
}

export function renderAdminSubscriptionsReport(report: AdminSubscriptionsReport): string {
  return [
    "<b>Subscriptions</b>",
    `Subscriptions: <b>${report.totalSubscriptions}</b>`,
    `Avg per user: <b>${formatActivityNumber(report.avgSubscriptionsPerUser)}</b>`,
    `Last subscription created at: <b>${escapeHtml(report.lastSubscriptionCreatedAt ?? "n/a")}</b>`,
    `Last alert sent at: <b>${escapeHtml(report.lastAlertSentAt ?? "n/a")}</b>`
  ].join("\n");
}

export function renderAdminProfilesReport(stats: readonly AdminProfileStat[]): string {
  if (stats.length === 0) {
    return [
      "<b>Profiles</b>",
      "No subscriptions yet."
    ].join("\n");
  }

  const lines = ["<b>Profiles</b>"];
  for (const stat of stats) {
    const url = `https://polymarket.com/${stat.proxyAddress}`;
    lines.push(`- <a href="${url}">${escapeHtml(url)}</a>: <b>${stat.subscriptionsCount}</b>`);
  }

  return lines.join("\n");
}

export function renderAdminPerformanceReport(report: AdminPerformanceReport): string {
  if (report.sampleCount === 0) {
    return [
      "<b>Performance</b>",
      "No performance data for the last 24 hours."
    ].join("\n");
  }

  const lines = [
    "<b>Performance</b>",
    `Min delay: <b>${formatDurationMs(report.minDelayMs ?? 0)}</b>`,
    `Avg delay: <b>${formatDurationMs(report.avgDelayMs ?? 0)}</b>`,
    `Max delay: <b>${formatDurationMs(report.maxDelayMs ?? 0)}</b>`,
    `Fresh local max: <b>${typeof report.freshLocalMaxDelayMs === "number" ? formatDurationMs(report.freshLocalMaxDelayMs) : "n/a"}</b>`,
    `Alerts over ${formatDurationMs(report.thresholdMs)}: <b>${report.alertsOverThresholdCount}</b>`,
    `Upstream stale alerts: <b>${report.upstreamStaleCount}</b>`,
    "",
    "<b>Delay buckets:</b>"
  ];

  for (const bucket of report.delayBuckets) {
    lines.push(`- ${escapeHtml(bucket.label)}: <b>${bucket.count}</b>`);
  }

  lines.push(
    "",
    `<b>Delay reasons over ${formatDurationMs(report.thresholdMs)}:</b>`,
    `Classified delayed alerts: <b>${report.classifiedSampleCount}</b>`
  );

  for (const item of report.delayReasonCounts) {
    lines.push(`- ${formatDelayReasonLabel(item.reason)}: <b>${item.count}</b>`);
  }

  return lines.join("\n");
}
