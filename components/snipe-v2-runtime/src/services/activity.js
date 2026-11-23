export const ALLOWED_ACTIVITY_TYPES = new Set([
    "TRADE",
    "SPLIT",
    "MERGE",
    "REDEEM",
    "REWARD",
    "CONVERSION",
    "MAKER_REBATE",
    "REFERRAL_REWARD"
]);
const MAX_WATERMARK_KEYS = 500;
function unique(values) {
    return [...new Set(values)];
}
function uniqueActivities(activities) {
    const seen = new Set();
    const out = [];
    for (const activity of activities) {
        const key = buildActivityKey(activity);
        if (seen.has(key)) {
            continue;
        }
        seen.add(key);
        out.push(activity);
    }
    return out;
}
export function buildActivityKey(activity) {
    return [
        activity.type,
        activity.timestamp,
        activity.conditionId ?? "",
        activity.transactionHash ?? "",
        activity.asset ?? "",
        activity.side ?? "",
        activity.size ?? "",
        activity.usdcSize ?? ""
    ].join("|");
}
export function filterAllowedActivities(activities) {
    return activities.filter((activity) => ALLOWED_ACTIVITY_TYPES.has(activity.type.toUpperCase()));
}
export function updateWatermarkState(current, activities) {
    const ordered = uniqueActivities([...activities].sort((a, b) => b.timestamp - a.timestamp));
    const highestTs = ordered[0]?.timestamp ?? current.lastSeenTimestamp;
    const highestKeys = unique(ordered.filter((activity) => activity.timestamp === highestTs).map((activity) => buildActivityKey(activity))).slice(-MAX_WATERMARK_KEYS);
    if (!current.initialized) {
        return {
            bootstrap: true,
            newActivities: [],
            nextState: {
                initialized: true,
                lastSeenTimestamp: highestTs,
                lastSeenKeys: highestKeys
            }
        };
    }
    const seen = new Set(current.lastSeenKeys);
    const newActivities = ordered.filter((activity) => {
        if (activity.timestamp > current.lastSeenTimestamp) {
            return true;
        }
        if (activity.timestamp === current.lastSeenTimestamp) {
            return !seen.has(buildActivityKey(activity));
        }
        return false;
    });
    let nextTimestamp = current.lastSeenTimestamp;
    let nextKeys = [...current.lastSeenKeys];
    if (ordered.length > 0) {
        if (highestTs > current.lastSeenTimestamp) {
            nextTimestamp = highestTs;
            nextKeys = highestKeys;
        }
        else if (highestTs === current.lastSeenTimestamp) {
            nextKeys = unique([...current.lastSeenKeys, ...highestKeys]).slice(-MAX_WATERMARK_KEYS);
        }
    }
    return {
        bootstrap: false,
        newActivities,
        nextState: {
            initialized: true,
            lastSeenTimestamp: nextTimestamp,
            lastSeenKeys: nextKeys
        }
    };
}
