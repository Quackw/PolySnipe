// Call only inside the caller's existing SQLite transaction. The prepared
// statement must be FAST's authoritative activeSubscriptions query; no cached
// recipient map, external request, side effect or fallback is used.
export function createRedeemEligibility({activeSubscriptions,ownerIds,normalizeAddress}){
 if(!activeSubscriptions||typeof activeSubscriptions.all!=='function'||!Array.isArray(ownerIds)||!ownerIds.length||typeof normalizeAddress!=='function')throw Error('REDEEM_ELIGIBILITY_CONFIG_INVALID');
 return (wallet,recipient,sourceTimestamp)=>{
  if(!normalizeAddress(wallet)||!Number.isSafeInteger(recipient)||!Number.isSafeInteger(sourceTimestamp))throw Error('REDEEM_ELIGIBILITY_ARGUMENT_INVALID');
  return activeSubscriptions.all(...ownerIds).some(r=>normalizeAddress(r.wallet)===wallet&&r.chat_id===recipient&&Number.isSafeInteger(r.notify_from_ts)&&sourceTimestamp>=r.notify_from_ts);
 };
}
