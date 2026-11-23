import { renderActivityAlert } from "./services/format.js";
import { updateWatermarkState } from "./services/activity.js";
function clampDurationMs(startMs, endMs) {
    return Math.max(0, endMs - startMs);
}
function calculateDelayPhaseMetrics(activityTimestamp, pollStartedAtMs, fetchCompletedAtMs, sendStartedAtMs, sendCompletedAtMs) {
    const activityTimestampMs = activityTimestamp * 1000;
    return {
        ageAtPollStartMs: clampDurationMs(activityTimestampMs, pollStartedAtMs),
        fetchPhaseMs: clampDurationMs(pollStartedAtMs, fetchCompletedAtMs),
        dispatchPhaseMs: clampDurationMs(fetchCompletedAtMs, sendStartedAtMs),
        telegramPhaseMs: clampDurationMs(sendStartedAtMs, sendCompletedAtMs),
        endToEndMs: clampDurationMs(activityTimestampMs, sendCompletedAtMs)
    };
}
function classifyDelayReason(metrics, thresholdMs) {
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
async function runWithConcurrency(limit, items, worker) {
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
    await Promise.all(runners);
}
export function calculateEffectiveIntervalMs(uniqueTrackedAccounts, safeRps, targetIntervalSec) {
    const adaptiveSeconds = Math.ceil(Math.max(0, uniqueTrackedAccounts) / Math.max(1, safeRps));
    return Math.max(targetIntervalSec, adaptiveSeconds) * 1000;
}
export function computeRetryDelayMs(consecutiveErrors, randomValue = Math.random()) {
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
    config;
    repo;
    polymarket;
    deps;
    tickTimer = null;
    rebalanceTimer = null;
    orphanCleanupTimer = null;
    staleInflightTimer = null;
    walTimer = null;
    heartbeatTimer = null;
    activePolls = 0;
    tickRunning = false;
    stopped = false;
    constructor(config, repo, polymarket, deps) {
        this.config = config;
        this.repo = repo;
        this.polymarket = polymarket;
        this.deps = deps;
    }
    start() {
        this.stopped = false;
        this.tickTimer = setInterval(() => {
            void this.drainDueAccounts();
        }, this.config.schedulerTickMs);
        this.rebalanceTimer = setInterval(() => this.runRebalance(), this.config.rebalanceEveryMs);
        this.orphanCleanupTimer = setInterval(() => this.runOrphanCleanup(), this.config.orphanCleanupEveryMs);
        this.staleInflightTimer = setInterval(() => this.runStaleInflightRecovery(), this.config.staleInflightRecoveryEveryMs);
        this.walTimer = setInterval(() => this.runWalCheckpoint(), this.config.walCheckpointEveryMs);
        this.heartbeatTimer = setInterval(() => this.runHeartbeat(), this.config.heartbeatEveryMs);
        void this.drainDueAccounts();
    }
    stop() {
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
    async stopAndDrain() {
        this.stop();
        while (this.activePolls > 0) {
            await new Promise((resolve) => setTimeout(resolve, 25));
        }
    }
    async drainDueAccounts() {
        if (this.stopped || this.tickRunning) {
            return;
        }
        this.tickRunning = true;
        try {
            while (!this.stopped && this.activePolls < this.config.activityConcurrency) {
                const nowMs = Date.now();
                const dueAccounts = this.repo.listDueTrackedAccounts(nowMs, this.config.activityConcurrency - this.activePolls);
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
        }
        finally {
            this.tickRunning = false;
        }
    }
    async pollAccount(proxyAddress) {
        const startedAt = Date.now();
        const tracked = this.repo.getTrackedAccount(proxyAddress);
        if (!tracked) {
            return;
        }
        const trackedCount = this.repo.countTrackedAccounts();
        const effectiveIntervalMs = calculateEffectiveIntervalMs(trackedCount, this.config.activitySafeRps, this.config.pollTargetIntervalSec);
        const nowSec = Math.floor(Date.now() / 1000);
        const startTs = Math.max(0, tracked.lastSeenTimestamp - this.config.activityOverlapSec);
        const pollStartedAtMs = Date.now();
        try {
            const activityResult = await this.polymarket.fetchRecentUserActivity(proxyAddress, startTs, nowSec);
            const watermark = updateWatermarkState({
                initialized: tracked.lastSeenTimestamp > 0 || tracked.lastSeenKeys.length > 0,
                lastSeenTimestamp: tracked.lastSeenTimestamp,
                lastSeenKeys: tracked.lastSeenKeys
            }, activityResult.activities);
            const fetchCompletedAtMs = Date.now();
            const nextPollAtMs = Date.now() + effectiveIntervalMs;
            this.repo.markPollSuccess({
                proxyAddress,
                lastSeenTimestamp: watermark.nextState.lastSeenTimestamp,
                lastSeenKeys: watermark.nextState.lastSeenKeys,
                lastPolledAtMs: Date.now(),
                nextPollAtMs
            });
            const alertsSent = watermark.bootstrap ? 0 : await this.sendAlerts(tracked, watermark.newActivities, {
                pollStartedAtMs,
                fetchCompletedAtMs
            });
            const status = watermark.bootstrap ? "bootstrap" : "ok";
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
                this.deps.log(`[poll:${status}] fetched=${activityResult.activities.length} new=${watermark.newActivities.length} alerts=${alertsSent} duration_ms=${Date.now() - startedAt}`);
            }
        }
        catch (error) {
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
            this.deps.log(`[poll:error] delay_ms=${delayMs} error_type=${error instanceof Error ? error.name : "unknown"}`);
        }
    }
    async sendAlerts(account, activities, timing) {
        if (activities.length === 0) {
            return 0;
        }
        const subscriptions = this.repo.listSubscriptionsForAccount(account.proxyAddress);
        if (subscriptions.length === 0) {
            return 0;
        }
        let alertsSent = 0;
        const resolvedAccount = {
            proxyAddress: account.proxyAddress,
            name: account.name,
            pseudonym: account.pseudonym,
            profileSlug: account.profileSlug
        };
        const delayThresholdMs = this.config.alertDelayThresholdSec * 1000;
        const staleAgesMs = activities
            .map((activity) => calculateDelayPhaseMetrics(activity.timestamp, timing.pollStartedAtMs, timing.fetchCompletedAtMs, timing.fetchCompletedAtMs, timing.fetchCompletedAtMs).ageAtPollStartMs)
            .filter((ageAtPollStartMs) => ageAtPollStartMs >= delayThresholdMs);
        if (staleAgesMs.length > 0) {
            this.deps.log(`[alert:stale_batch] stale_count=${staleAgesMs.length} freshest_stale_ms=${Math.min(...staleAgesMs)} stalest_ms=${Math.max(...staleAgesMs)}`);
        }
        await runWithConcurrency(this.config.alertSendConcurrency, subscriptions, async (subscription) => {
            const visibleActivities = activities.filter((activity) => activity.timestamp >= subscription.notifyFromTs);
            if (visibleActivities.length === 0) {
                return;
            }
            const sendStartedAtMs = Date.now();
            try {
                await this.deps.sendTelegram(subscription.telegramChatId, renderActivityAlert(resolvedAccount, visibleActivities));
            }
            catch (error) {
                this.deps.log(`[alert:error] error_type=${error instanceof Error ? error.name : "unknown"}`);
                return;
            }
            const sendCompletedAtMs = Date.now();
            const samples = visibleActivities.map((activity) => {
                const metrics = calculateDelayPhaseMetrics(activity.timestamp, timing.pollStartedAtMs, timing.fetchCompletedAtMs, sendStartedAtMs, sendCompletedAtMs);
                const delayReasonCode = classifyDelayReason(metrics, delayThresholdMs);
                if (delayReasonCode) {
                    this.deps.log(`[alert:delay] reason=${delayReasonCode} end_to_end_ms=${metrics.endToEndMs} age_at_poll_start_ms=${metrics.ageAtPollStartMs} fetch_phase_ms=${metrics.fetchPhaseMs} dispatch_phase_ms=${metrics.dispatchPhaseMs} telegram_phase_ms=${metrics.telegramPhaseMs}`);
                }
                return {
                    telegramChatId: subscription.telegramChatId,
                    proxyAddress: account.proxyAddress,
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
            alertsSent += 1;
        });
        return alertsSent;
    }
    runRebalance() {
        const intervalMs = calculateEffectiveIntervalMs(this.repo.countTrackedAccounts(), this.config.activitySafeRps, this.config.pollTargetIntervalSec);
        const changed = this.repo.rebalanceTrackedAccountIntervals(Date.now(), intervalMs);
        if (this.config.debug) {
            this.deps.log(`[rebalance] interval_ms=${intervalMs} rows=${changed}`);
        }
    }
    runOrphanCleanup() {
        const removed = this.repo.cleanupOrphanTrackedAccounts();
        if (removed > 0 || this.config.debug) {
            this.deps.log(`[cleanup:orphans] removed=${removed}`);
        }
    }
    runStaleInflightRecovery() {
        const recovered = this.repo.clearStaleInflight(Date.now(), this.config.staleInflightAfterMs);
        if (recovered > 0 || this.config.debug) {
            this.deps.log(`[cleanup:stale_inflight] recovered=${recovered}`);
        }
    }
    runWalCheckpoint() {
        this.repo.checkpointWalTruncate();
        if (this.config.debug) {
            this.deps.log(`[maintenance] wal_checkpoint=1`);
        }
    }
    runHeartbeat() {
        const nowMs = Date.now();
        this.repo.cleanupOldAlertDelaySamples(nowMs);
        this.repo.cleanupAlertDelaySamplesOverflow();
        this.repo.cleanupOldPollRuns(nowMs, this.config.pollRunsRetentionHours);
        const trackedAccounts = this.repo.countTrackedAccounts();
        const effectiveIntervalMs = calculateEffectiveIntervalMs(trackedAccounts, this.config.activitySafeRps, this.config.pollTargetIntervalSec);
        const freshness = this.repo.getFreshnessStats();
        const nowSec = Math.floor(Date.now() / 1000);
        const lagSec = freshness.newestSeenTimestamp > 0 ? Math.max(0, nowSec - freshness.newestSeenTimestamp) : 0;
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
        this.deps.log(`[heartbeat] tracked=${trackedAccounts} inflight=${this.repo.countInflightTrackedAccounts()} effective_interval_ms=${effectiveIntervalMs} freshness_lag_sec=${lagSec}`);
    }
}
