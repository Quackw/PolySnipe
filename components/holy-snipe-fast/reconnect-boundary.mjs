// Only the transport reconnect boundary overlaps. Enable/subscription boundaries stay strict.
export function reconnectBoundary(reconnectMs, enabledMs) {
  return Math.max(reconnectMs-2000,enabledMs);
}
export function inReconnectOverlap(sourceSeconds,reconnectMs,enabledMs) {
  const at=Number(sourceSeconds)*1000;
  return Number.isSafeInteger(Number(sourceSeconds)) && at<reconnectMs && at>=reconnectBoundary(reconnectMs,enabledMs);
}
export function recordOverlap(stats,sourceSeconds,boundaryMs){
 const distance=boundaryMs-Number(sourceSeconds)*1000;
 stats.overlap_accepted++;
 stats.distance_min_ms=Math.min(stats.distance_min_ms??Infinity,distance);
 stats.distance_max_ms=Math.max(stats.distance_max_ms??0,distance);
 stats.distance_sum_ms=(stats.distance_sum_ms??0)+distance;
 const bucket=distance<=10?'le10':distance<=100?'le100':distance<=500?'le500':distance<=1000?'le1000':distance<=1500?'le1500':'le2000';
 stats.distance_buckets??={};stats.distance_buckets[bucket]=(stats.distance_buckets[bucket]??0)+1;
 return distance;
}
