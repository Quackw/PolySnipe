import { splitForTelegram } from "./utils/telegram.js";
import { TelegramRejectedError } from "./apis/telegram.js";
import type { OutboxInsert, OutboxRow } from "./db/repository.js";
import { renderActivityAlert } from "./services/format.js";
import { updateWatermarkState } from "./services/activity.js";
import type {
  AlertDelaySampleInsert,
  AppConfig,
  DelayReasonCode,
  PollRunStatus,
  ResolvedAccount,
  SchedulerDeps,
  TrackedAccountRecord,
  UserActivity
} from "./types.js";
import type { AppRepository } from "./db/repository.js";
import type { PolymarketClient } from "./apis/polymarket.js";

type DelayPhaseMetrics = {
  ageAtPollStartMs: number;
  fetchPhaseMs: number;
  dispatchPhaseMs: number;
  telegramPhaseMs: number;
  endToEndMs: number;
};

function clampDurationMs(startMs: number, endMs: number): number {
  return Math.max(0, endMs - startMs);
}

function calculateDelayPhaseMetrics(
  activityTimestamp: number,
  pollStartedAtMs: number,
  fetchCompletedAtMs: number,
  sendStartedAtMs: number,
  sendCompletedAtMs: number
): DelayPhaseMetrics {
  const activityTimestampMs = activityTimestamp * 1000;

  return {
    ageAtPollStartMs: clampDurationMs(activityTimestampMs, pollStartedAtMs),
    fetchPhaseMs: clampDurationMs(pollStartedAtMs, fetchCompletedAtMs),
    dispatchPhaseMs: clampDurationMs(fetchCompletedAtMs, sendStartedAtMs),
    telegramPhaseMs: clampDurationMs(sendStartedAtMs, sendCompletedAtMs),
    endToEndMs: clampDurationMs(activityTimestampMs, sendCompletedAtMs)
  };
}

function classifyDelayReason(metrics: DelayPhaseMetrics, thresholdMs: number): DelayReasonCode | undefined {
  if (metrics.endToEndMs < thresholdMs) {
    return undefined;
  }
  if (metrics.ageAtPollStartMs >= thresholdMs) {
    return "upstream_stale";
  }
  if (metrics.ageAtPollStartMs + metrics.fetchPhaseMs >= thresholdMs) {
    return "poll_fetch_slow";
  }
  if (metrics.ageAtPollStartMs + metrics.fetchPhaseMs + metrics.dispatchPhaseMs >= thresholdMs) {
    return "dispatch_backlog";
  }
  return "telegram_slow";
}

async function runWithConcurrency<T>(
  limit: number,
  items: readonly T[],
  worker: (item: T) => Promise<void>
): Promise<void> {
  const concurrency = Math.max(1, limit);
  let index = 0;

  const runners = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (index < items.length) {
      const item = items[index];
      index += 1;
      if (item === undefined) {
        return;
      }
      await worker(item);
    }
  });

  const results = await Promise.allSettled(runners);
  const failure = results.find((result) => result.status === "rejected");
  if (failure?.status === "rejected") throw failure.reason;
}

export function calculateEffectiveIntervalMs(
  uniqueTrackedAccounts: number,
  safeRps: number,
  targetIntervalSec: number
): number {
  const adaptiveSeconds = Math.ceil(Math.max(0, uniqueTrackedAccounts) / Math.max(1, safeRps));
  return Math.max(targetIntervalSec, adaptiveSeconds) * 1000;
}

export function computeRetryDelayMs(consecutiveErrors: number, randomValue = Math.random()): number {
  if (consecutiveErrors <= 1) {
    return 2000 + Math.floor(randomValue * 500);
  }
  if (consecutiveErrors === 2) {
    return 5000 + Math.floor(randomValue * 1000);
  }
  if (consecutiveErrors === 3) {
    return 10000 + Math.floor(randomValue * 2000);
  }
  return 30000 + Math.floor(randomValue * 3000);
}

export class PollScheduler {
  private tickTimer: NodeJS.Timeout | null = null;
  private rebalanceTimer: NodeJS.Timeout | null = null;
  private orphanCleanupTimer: NodeJS.Timeout | null = null;
  private staleInflightTimer: NodeJS.Timeout | null = null;
  private walTimer: NodeJS.Timeout | null = null;
  private heartbeatTimer: NodeJS.Timeout | null = null;
  private activePolls = 0;
  private dispatching = false;
  private tickRunning = false;
  private stopped = false;

  public constructor(
    private readonly config: AppConfig,
    private readonly repo: AppRepository,
    private readonly polymarket: PolymarketClient,
    private readonly deps: SchedulerDeps
  ) {}

  public start(): void {
    this.stopped = false;
    const uncertain = this.repo.recoverOutbox();
    if (uncertain) this.deps.log(`[outbox:attention] unknown=${uncertain}`);
    this.tickTimer = setInterval(() => {
      void this.drainDueAccounts();
      void this.dispatchAlerts();
    }, this.config.schedulerTickMs);
    this.rebalanceTimer = setInterval(() => this.runRebalance(), this.config.rebalanceEveryMs);
    this.orphanCleanupTimer = setInterval(() => this.runOrphanCleanup(), this.config.orphanCleanupEveryMs);
    this.staleInflightTimer = setInterval(
      () => this.runStaleInflightRecovery(),
      this.config.staleInflightRecoveryEveryMs
    );
    this.walTimer = setInterval(() => this.runWalCheckpoint(), this.config.walCheckpointEveryMs);
    this.heartbeatTimer = setInterval(() => this.runHeartbeat(), this.config.heartbeatEveryMs);

    void this.drainDueAccounts();
    void this.dispatchAlerts();
  }

  public stop(): void {
    this.stopped = true;
    for (const timer of [
      this.tickTimer,
      this.rebalanceTimer,
      this.orphanCleanupTimer,
      this.staleInflightTimer,
      this.walTimer,
      this.heartbeatTimer
    ]) {
      if (timer) {
        clearInterval(timer);
      }
    }
  }

  public async stopAndDrain(): Promise<void> {
    this.stop();
    while (this.activePolls > 0 || this.dispatching) {
      await new Promise<void>((resolve) => setTimeout(resolve, 25));
    }
  }

  private async drainDueAccounts(): Promise<void> {
    if (this.stopped || this.tickRunning) {
      return;
    }

    this.tickRunning = true;
    try {
      while (!this.stopped && this.activePolls < this.config.activityConcurrency) {
        const nowMs = Date.now();
        const dueAccounts = this.repo.listDueTrackedAccounts(
          nowMs,
          this.config.activityConcurrency - this.activePolls
        );
        if (dueAccounts.length === 0) {
          break;
        }

        let startedAny = false;
        for (const account of dueAccounts) {
          if (this.activePolls >= this.config.activityConcurrency) {
            break;
          }
          if (!this.repo.tryClaimTrackedAccount(account.proxyAddress, nowMs)) {
            continue;
          }

          startedAny = true;
          this.activePolls += 1;
          void this.pollAccount(account.proxyAddress).finally(() => {
            this.activePolls = Math.max(0, this.activePolls - 1);
          });
        }

        if (!startedAny) {
          break;
        }
      }
    } finally {
      this.tickRunning = false;
    }
  }

  private async pollAccount(proxyAddress: string): Promise<void> {
    const startedAt = Date.now();
    const tracked = this.repo.getTrackedAccount(proxyAddress);
    if (!tracked) {
      return;
    }

    const trackedCount = this.repo.countTrackedAccounts();
    const effectiveIntervalMs = calculateEffectiveIntervalMs(
      trackedCount,
      this.config.activitySafeRps,
      this.config.pollTargetIntervalSec
    );

    const nowSec = Math.floor(Date.now() / 1000);
    const startTs = Math.max(0, tracked.lastSeenTimestamp - this.config.activityOverlapSec);
    const pollStartedAtMs = Date.now();

    try {
      const activityResult = await this.polymarket.fetchRecentUserActivity(proxyAddress, startTs, nowSec, {
        bootstrapOnly: tracked.lastSeenTimestamp === 0 && tracked.lastSeenKeys.length === 0
      });
      const watermark = updateWatermarkState(
        {
          initialized: tracked.lastSeenTimestamp > 0 || tracked.lastSeenKeys.length > 0,
          lastSeenTimestamp: tracked.lastSeenTimestamp,
          lastSeenKeys: tracked.lastSeenKeys
        },
        activityResult.activities
      );
      const fetchCompletedAtMs = Date.now();

      const nextPollAtMs = Date.now() + effectiveIntervalMs;
      this.repo.markPollSuccess({
        proxyAddress,
        lastSeenTimestamp: watermark.nextState.lastSeenTimestamp,
        lastSeenKeys: watermark.nextState.lastSeenKeys,
        lastPolledAtMs: Date.now(),
        nextPollAtMs,
        alerts: watermark.bootstrap
          ? []
          : this.prepareAlerts(tracked, watermark.newActivities, { pollStartedAtMs, fetchCompletedAtMs })
      });

      if (!watermark.bootstrap && process.env.HOLY_DESK_EXPORT_MODULE) {
        try {
          const bridge = await import(process.env.HOLY_DESK_EXPORT_MODULE);
          await bridge.exportSnipe(
            watermark.newActivities,
            this.repo.listSubscriptionsForAccount(proxyAddress)
          );
        } catch {
          this.deps.log("[desk] export unavailable");
        }
      }
      const alertsSent = await this.dispatchAlerts();
      const status: PollRunStatus = watermark.bootstrap ? "bootstrap" : "ok";
      this.repo.insertPollRun({
        kind: "account",
        proxyAddress,
        status,
        durationMs: Date.now() - startedAt,
        apiCalls: activityResult.apiCalls,
        retries: activityResult.retries,
        fetchedRows: activityResult.activities.length,
        newRows: watermark.newActivities.length,
        alertsSent,
        queueDepth: this.repo.countDueTrackedAccounts(Date.now()),
        trackedAccounts: trackedCount,
        inflightAccounts: this.repo.countInflightTrackedAccounts(),
        effectiveIntervalMs,
        throttleWaitMs: activityResult.throttleWaitMs
      });

      if (this.config.debug) {
        this.deps.log(
          `[poll:${status}] proxy=${proxyAddress} fetched=${activityResult.activities.length} new=${watermark.newActivities.length} alerts=${alertsSent} duration_ms=${Date.now() - startedAt}`
        );
      }
    } catch (error) {
      const consecutiveErrors = tracked.consecutiveErrors + 1;
      const delayMs = computeRetryDelayMs(consecutiveErrors);
      this.repo.markPollFailure(proxyAddress, Date.now() + delayMs, consecutiveErrors);
      this.repo.insertPollRun({
        kind: "account",
        proxyAddress,
        status: "error",
        durationMs: Date.now() - startedAt,
        apiCalls: 0,
        retries: 0,
        fetchedRows: 0,
        newRows: 0,
        alertsSent: 0,
        queueDepth: this.repo.countDueTrackedAccounts(Date.now()),
        trackedAccounts: trackedCount,
        inflightAccounts: this.repo.countInflightTrackedAccounts(),
        effectiveIntervalMs,
        throttleWaitMs: 0,
        note: error instanceof Error ? error.message : String(error)
      });
      this.deps.log(`[poll:error] proxy=${proxyAddress} delay_ms=${delayMs} error=${String(error)}`);
    }
  }

  private prepareAlerts(
    account: TrackedAccountRecord,
    activities: readonly UserActivity[],
    timing: { pollStartedAtMs: number; fetchCompletedAtMs: number }
  ): OutboxInsert[] {
    const resolvedAccount: ResolvedAccount = {
      proxyAddress: account.proxyAddress,
      name: account.name,
      pseudonym: account.pseudonym,
      profileSlug: account.profileSlug
    };
    return this.repo.listSubscriptionsForAccount(account.proxyAddress).flatMap((subscription) => {
      const visibleActivities = activities.filter(
        (activity) => activity.timestamp >= subscription.notifyFromTs
      );
      if (!visibleActivities.length) return [];
      return splitForTelegram(renderActivityAlert(resolvedAccount, visibleActivities), 3900).map(
        (text, index) => ({
          chatId: subscription.telegramChatId,
          text,
          contextJson: JSON.stringify({ activities: index === 0 ? visibleActivities : [], timing })
        })
      );
    });
  }

  private async dispatchAlerts(): Promise<number> {
    if (this.dispatching || this.stopped) return 0;
    this.dispatching = true;
    let alertsSent = 0;
    try {
      // Persist and acknowledge each Telegram chunk independently. Never replay an ambiguous send.
      const byChat = new Map<number, OutboxRow[]>();
      for (const row of this.repo.listPendingAlerts(Date.now())) {
        const rows = byChat.get(row.chat_id) ?? [];
        rows.push(row);
        byChat.set(row.chat_id, rows);
      }
      await runWithConcurrency(this.config.alertSendConcurrency, [...byChat.values()], async (rows) => {
        for (const row of rows) {
          if (this.stopped) break;
          const { activities: visibleActivities, timing } = JSON.parse(row.context_json) as {
            activities: UserActivity[];
            timing: { pollStartedAtMs: number; fetchCompletedAtMs: number };
          };
          const account = { proxyAddress: row.proxy_address };
          const subscription = { telegramChatId: row.chat_id };
          if (!this.repo.claimAlert(row.id)) continue;
          const sendStartedAtMs = Date.now();
          try {
            await this.deps.sendTelegram(row.chat_id, row.text);
            if (!this.repo.finishAlert(row.id, "DELIVERED")) {
              this.deps.log(`[outbox:finish_conflict] outbox=${row.id}`);
              continue;
            }
          } catch (error) {
            const rejected = error instanceof TelegramRejectedError;
            const retry = rejected && error.code === 429;
            const status = retry ? "PENDING" : rejected ? "REJECTED" : "UNKNOWN";
            const updated = this.repo.finishAlert(
              row.id,
              status,
              error instanceof Error ? error.message : String(error),
              retry ? Date.now() + Math.max(1000, error.retryAfterMs) : 0
            );
            if (updated) {
              this.deps.log(
                `[alert:error] proxy=${row.proxy_address} chat=${row.chat_id} outbox=${row.id} outcome=${retry ? "RATE_LIMIT" : status}`
              );
            } else {
              this.deps.log(`[outbox:finish_conflict] outbox=${row.id}`);
            }
            continue;
          }
          const sendCompletedAtMs = Date.now();
          this.recordDeliverySamples(
            account.proxyAddress,
            subscription.telegramChatId,
            visibleActivities,
            timing,
            sendStartedAtMs,
            sendCompletedAtMs
          );
          alertsSent += 1;
        }
      });
    } catch (error) {
      this.deps.log(`[outbox:error] ${String(error)}`);
    } finally {
      this.dispatching = false;
    }
    return alertsSent;
  }

  private recordDeliverySamples(
    proxyAddress: string,
    chatId: number,
    visibleActivities: UserActivity[],
    timing: { pollStartedAtMs: number; fetchCompletedAtMs: number },
    sendStartedAtMs: number,
    sendCompletedAtMs: number
  ): void {
    const delayThresholdMs = this.config.alertDelayThresholdSec * 1000;
    const staleAgesMs = visibleActivities
      .map(
        (activity) =>
          calculateDelayPhaseMetrics(
            activity.timestamp,
            timing.pollStartedAtMs,
            timing.fetchCompletedAtMs,
            timing.fetchCompletedAtMs,
            timing.fetchCompletedAtMs
          ).ageAtPollStartMs
      )
      .filter((ageAtPollStartMs) => ageAtPollStartMs >= delayThresholdMs);

    if (staleAgesMs.length > 0) {
      this.deps.log(
        `[alert:stale_batch] proxy=${proxyAddress} stale_count=${staleAgesMs.length} freshest_stale_ms=${Math.min(...staleAgesMs)} stalest_ms=${Math.max(...staleAgesMs)}`
      );
    }

    const samples: AlertDelaySampleInsert[] = visibleActivities.map((activity) => {
      const metrics = calculateDelayPhaseMetrics(
        activity.timestamp,
        timing.pollStartedAtMs,
        timing.fetchCompletedAtMs,
        sendStartedAtMs,
        sendCompletedAtMs
      );
      const delayReasonCode = classifyDelayReason(metrics, delayThresholdMs);
      if (delayReasonCode) {
        this.deps.log(
          `[alert:delay] proxy=${proxyAddress} chat=${chatId} reason=${delayReasonCode} end_to_end_ms=${metrics.endToEndMs} age_at_poll_start_ms=${metrics.ageAtPollStartMs} fetch_phase_ms=${metrics.fetchPhaseMs} dispatch_phase_ms=${metrics.dispatchPhaseMs} telegram_phase_ms=${metrics.telegramPhaseMs} activity_ts=${activity.timestamp}`
        );
      }

      return {
        telegramChatId: chatId,
        proxyAddress: proxyAddress,
        activityTimestamp: activity.timestamp,
        pollStartedAtMs: timing.pollStartedAtMs,
        fetchCompletedAtMs: timing.fetchCompletedAtMs,
        sendStartedAtMs,
        sendCompletedAtMs,
        delayMs: Math.max(0, sendStartedAtMs - activity.timestamp * 1000),
        delayReasonCode
      };
    });

    this.repo.insertAlertDelaySamples(samples);
  }

  private runRebalance(): void {
    const intervalMs = calculateEffectiveIntervalMs(
      this.repo.countTrackedAccounts(),
      this.config.activitySafeRps,
      this.config.pollTargetIntervalSec
    );
    const changed = this.repo.rebalanceTrackedAccountIntervals(Date.now(), intervalMs);
    if (this.config.debug) {
      this.deps.log(`[rebalance] interval_ms=${intervalMs} rows=${changed}`);
    }
  }

  private runOrphanCleanup(): void {
    const removed = this.repo.cleanupOrphanTrackedAccounts();
    if (removed > 0 || this.config.debug) {
      this.deps.log(`[cleanup:orphans] removed=${removed}`);
    }
  }

  private runStaleInflightRecovery(): void {
    const recovered = this.repo.clearStaleInflight(Date.now(), this.config.staleInflightAfterMs);
    if (recovered > 0 || this.config.debug) {
      this.deps.log(`[cleanup:stale_inflight] recovered=${recovered}`);
    }
  }

  private runWalCheckpoint(): void {
    this.repo.checkpointWalTruncate();
    if (this.config.debug) {
      this.deps.log(`[maintenance] wal_checkpoint=1`);
    }
  }

  private runHeartbeat(): void {
    const nowMs = Date.now();
    this.deps.log(`[outbox:status] ${JSON.stringify(this.repo.outboxCounts())}`);
    this.repo.cleanupOldAlertDelaySamples(nowMs);
    this.repo.cleanupAlertDelaySamplesOverflow();
    this.repo.cleanupOldPollRuns(nowMs, this.config.pollRunsRetentionHours);

    const trackedAccounts = this.repo.countTrackedAccounts();
    const effectiveIntervalMs = calculateEffectiveIntervalMs(
      trackedAccounts,
      this.config.activitySafeRps,
      this.config.pollTargetIntervalSec
    );
    const freshness = this.repo.getFreshnessStats();
    const nowSec = Math.floor(Date.now() / 1000);
    const lagSec =
      freshness.newestSeenTimestamp > 0 ? Math.max(0, nowSec - freshness.newestSeenTimestamp) : 0;

    this.repo.insertPollRun({
      kind: "heartbeat",
      status: "ok",
      durationMs: 0,
      apiCalls: 0,
      retries: 0,
      fetchedRows: 0,
      newRows: 0,
      alertsSent: 0,
      queueDepth: this.repo.countDueTrackedAccounts(Date.now()),
      trackedAccounts,
      inflightAccounts: this.repo.countInflightTrackedAccounts(),
      effectiveIntervalMs,
      throttleWaitMs: 0,
      note: `freshness_lag_sec=${lagSec};initialized_accounts=${freshness.initializedAccounts}`
    });

    this.deps.log(
      `[heartbeat] tracked=${trackedAccounts} inflight=${this.repo.countInflightTrackedAccounts()} effective_interval_ms=${effectiveIntervalMs} freshness_lag_sec=${lagSec}`
    );
  }
}
