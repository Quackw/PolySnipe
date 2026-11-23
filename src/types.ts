export type ChatMode = "idle" | "awaiting_subscribe";
export type PollRunKind = "account" | "heartbeat";
export type PollRunStatus = "ok" | "bootstrap" | "error";
export type DelayReasonCode = "upstream_stale" | "poll_fetch_slow" | "dispatch_backlog" | "telegram_slow";
export type DelayBucketLabel = "<5s" | "5-10s" | "10-30s" | "30-60s" | "1-2m" | "2-5m" | "5-10m" | ">10m";

export interface DelayBucketCount {
  label: DelayBucketLabel;
  count: number;
}

export interface DelayReasonCount {
  reason: DelayReasonCode;
  count: number;
}

export interface AppConfig {
  telegramBotToken: string;
  adminTelegramUserIds: number[];
  sqlitePath: string;
  schedulerTickMs: number;
  pollTargetIntervalSec: number;
  activitySafeRps: number;
  activityConcurrency: number;
  activityPageLimit: number;
  activityMaxPages: number;
  activityOverlapSec: number;
  rebalanceEveryMs: number;
  orphanCleanupEveryMs: number;
  subscribeModeTtlMs: number;
  staleInflightAfterMs: number;
  staleInflightRecoveryEveryMs: number;
  walCheckpointEveryMs: number;
  heartbeatEveryMs: number;
  alertDelayThresholdSec: number;
  alertSendConcurrency: number;
  pollRunsRetentionHours: number;
  polymarketTimeoutMs: number;
  polymarketRetries: number;
  telegramTimeoutMs: number;
  httpTimeoutMs: number;
  httpRetries: number;
  debug: boolean;
}

export interface SubscriberRecord {
  id: number;
  telegramUserId: number;
  telegramChatId: number;
  chatType: string;
}

export interface ChatSessionRecord {
  telegramChatId: number;
  mode: ChatMode;
  awaitingUserId?: number | undefined;
  expiresAtMs?: number | undefined;
}

export interface TrackedAccountRecord {
  proxyAddress: string;
  name?: string | undefined;
  pseudonym?: string | undefined;
  profileSlug?: string | undefined;
  lastSeenTimestamp: number;
  lastSeenKeys: string[];
  lastPolledAtMs?: number | undefined;
  nextPollAtMs: number;
  consecutiveErrors: number;
  inflight: boolean;
  inflightStartedAtMs?: number | undefined;
}

export interface ChatSubscriptionView {
  proxyAddress: string;
  name?: string | undefined;
  pseudonym?: string | undefined;
  profileSlug?: string | undefined;
  notifyFromTs: number;
}

export interface AccountSubscriptionRecord {
  telegramChatId: number;
  notifyFromTs: number;
}

export interface PollRunRecord {
  kind: PollRunKind;
  proxyAddress?: string | undefined;
  status: PollRunStatus;
  durationMs: number;
  apiCalls: number;
  retries: number;
  fetchedRows: number;
  newRows: number;
  alertsSent: number;
  queueDepth: number;
  trackedAccounts: number;
  inflightAccounts: number;
  effectiveIntervalMs: number;
  throttleWaitMs: number;
  note?: string | undefined;
}

export interface FreshnessStats {
  trackedAccounts: number;
  initializedAccounts: number;
  newestSeenTimestamp: number;
}

export interface PublicProfile {
  proxyWallet: string;
  name?: string | undefined;
  pseudonym?: string | undefined;
}

export interface ResolvedAccount {
  proxyAddress: string;
  name?: string | undefined;
  pseudonym?: string | undefined;
  profileSlug?: string | undefined;
}

export interface UserActivity {
  legacyActivityKey?: string;
  proxyWallet: string;
  timestamp: number;
  type: string;
  conditionId?: string | undefined;
  transactionHash?: string | undefined;
  asset?: string | undefined;
  side?: string | undefined;
  size?: number | undefined;
  usdcSize?: number | undefined;
  price?: number | undefined;
  outcomeIndex?: number | undefined;
  title?: string | undefined;
  slug?: string | undefined;
  eventSlug?: string | undefined;
  outcome?: string | undefined;
  name?: string | undefined;
  pseudonym?: string | undefined;
}

export interface WatermarkState {
  initialized: boolean;
  lastSeenTimestamp: number;
  lastSeenKeys: string[];
}

export interface WatermarkUpdateResult {
  bootstrap: boolean;
  newActivities: UserActivity[];
  nextState: WatermarkState;
}

export interface ActivityPageResult {
  activities: UserActivity[];
  apiCalls: number;
  retries: number;
  throttleWaitMs: number;
}

export interface AccountLabel {
  title: string;
  subtitle?: string | undefined;
}

export interface SchedulerDeps {
  sendTelegram: (chatId: number, text: string) => Promise<void>;
  log: (line: string) => void;
}

export interface AdminSubscriberStatsRow {
  telegramUserId: number;
  telegramChatId: number;
  subscriptionsCount: number;
}

export interface AdminSubscriberStats {
  rows: AdminSubscriberStatsRow[];
  totalSubscribers: number;
  totalSubscriptions: number;
}

export interface AdminUsersReport {
  totalUsers: number;
  lastUserJoinedAt?: string | undefined;
}

export interface AdminSubscriptionsReport {
  totalSubscriptions: number;
  avgSubscriptionsPerUser: number;
  lastSubscriptionCreatedAt?: string | undefined;
  lastAlertSentAt?: string | undefined;
}

export interface AdminProfileStat {
  proxyAddress: string;
  subscriptionsCount: number;
}

export interface AdminPerformanceReport {
  thresholdMs: number;
  sampleCount: number;
  classifiedSampleCount: number;
  minDelayMs?: number | undefined;
  maxDelayMs?: number | undefined;
  avgDelayMs?: number | undefined;
  freshLocalMaxDelayMs?: number | undefined;
  alertsOverThresholdCount: number;
  upstreamStaleCount: number;
  delayBuckets: DelayBucketCount[];
  delayReasonCounts: DelayReasonCount[];
}

export interface AlertDelaySampleInsert {
  telegramChatId: number;
  proxyAddress: string;
  activityTimestamp: number;
  pollStartedAtMs?: number | undefined;
  fetchCompletedAtMs?: number | undefined;
  sendStartedAtMs: number;
  sendCompletedAtMs?: number | undefined;
  delayMs: number;
  delayReasonCode?: DelayReasonCode | undefined;
}

export interface RuntimeNotifierDeps {
  notifyError: (source: "startup" | "uncaughtException" | "unhandledRejection" | "bot handler", error: unknown) => Promise<void>;
}
