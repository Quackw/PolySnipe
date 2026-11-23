// Candidate production adapter, inert. Never opens a DB or sends a message.
// Called only with the existing FAST writer and the existing SEND-ONCE owner.
import {performance} from 'node:perf_hooks';
import {userActionKey,userActionIdentity} from '../holy-snipe-fast/identity.mjs';
export const DIRECT_STATE_LIMIT_MS=2000;
export function createDirectAdmissionGate({db,sendOnce,readCurrentPair,isEnabled,disable,allowNewPairHistoryBlock=false,mono=()=>performance.now()}){
 const age=start=>{const elapsed=mono()-start;if(!Number.isFinite(elapsed)||elapsed<0||elapsed>DIRECT_STATE_LIMIT_MS)throw Error('STATE_TOO_STALE');return elapsed;};
 const hasCoverage=!!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='rtds_legacy_coverage'").get();
 const hasFences=!!db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='rtds_legacy_fences'").get();
 const legacyCovered=hasCoverage?db.prepare('SELECT 1 FROM rtds_legacy_coverage WHERE event_key=? AND chat_id=?'):null;
 const legacyFenced=hasFences?db.prepare('SELECT 1 FROM rtds_legacy_fences WHERE event_key=? AND chat_id=?'):null;
 const boundary=hasFences?db.prepare('SELECT MAX(before_ms) before_ms FROM rtds_legacy_fences WHERE wallet=? AND chat_id=?'):null;
 const unmapped=db.prepare(`SELECT 1 FROM deliveries d WHERE d.wallet=? AND d.chat_id=?
  AND NOT EXISTS(SELECT 1 FROM send_once_actions a WHERE a.first_event_key=d.event_key AND a.chat_id=d.chat_id)
  ${hasCoverage?`AND NOT EXISTS(SELECT 1 FROM rtds_legacy_coverage m JOIN send_once_actions a
   ON a.action_key=m.action_key AND a.chat_id=m.chat_id
   WHERE m.event_key=d.event_key AND m.chat_id=d.chat_id AND m.wallet=d.wallet)`:''}
  ${hasFences?`AND NOT EXISTS(SELECT 1 FROM rtds_legacy_fences f WHERE f.event_key=d.event_key AND f.chat_id=d.chat_id
   AND f.wallet=d.wallet AND f.source_timestamp=d.source_timestamp AND d.source_timestamp*1000<f.before_ms)`:''} LIMIT 1`);
 const admit=function(trade,recipient,fillKey,traceId,html,nextAt,seenAt){
  const started=mono();let begun=false,committed=false;
  try{
   db.exec('BEGIN IMMEDIATE');begun=true;age(started);
   if(!isEnabled()||!sendOnce.applies(trade.wallet,recipient))throw Error('STRICT_ACTION_PROTECTION_REQUIRED');
   const pair=readCurrentPair(trade.wallet,recipient);
   if(!pair||trade.timestamp<pair.notify_from_ts){db.exec('ROLLBACK');begun=false;return {kind:'BLOCKED',reason:'SUBSCRIPTION_INACTIVE'};}
   const before=boundary?.get(trade.wallet,recipient)?.before_ms;
   if(before!=null&&trade.timestamp*1000<before){age(started);db.exec('ROLLBACK');begun=false;return {kind:'BLOCKED',reason:'BEFORE_EXISTING_PROTECTION_BOUNDARY'};}
   const key=userActionKey(trade),prior=db.prepare('SELECT * FROM send_once_actions WHERE action_key=? AND chat_id=?').get(key,recipient);
   if(prior){
    if(prior.identity_json!==userActionIdentity(trade))throw Error('IDENTITY_CONFLICT');
    // All existing states, including UNKNOWN and orphan reservations, suppress.
    age(started);db.exec('COMMIT');begun=false;committed=true;const stateAge=age(started);
    return {kind:'SUPPRESS_SAME_ACTION',actionKey:key,firstEventKey:prior.first_event_key,firstTraceId:prior.first_trace_id,state_age_ms:stateAge};
   }
   if(unmapped.get(trade.wallet,recipient)){
    if(!allowNewPairHistoryBlock||sendOnce.isOriginallyProtected?.(trade.wallet,recipient)!==false)throw Error('UNMAPPED_LEGACY_DELIVERY');
    age(started);db.exec('ROLLBACK');begun=false;
    return {kind:'BLOCKED',reason:'UNMAPPED_LEGACY_DELIVERY'};
   }
   // Existing better-sqlite3 transactions nest as SAVEPOINTs, under this writer.
   const result=sendOnce.admit(trade,recipient,fillKey,traceId,html,nextAt,seenAt);
   if(result.kind!=='SEND_FIRST_ACTION'&&result.kind!=='SUPPRESS_SAME_ACTION')throw Error('STRICT_ACTION_PROTECTION_REQUIRED');
   age(started);db.exec('COMMIT');begun=false;committed=true;const stateAge=age(started);return {...result,state_age_ms:stateAge};
  }catch(error){
   if(begun)try{db.exec('ROLLBACK');}catch{}
   // A post-commit overrun preserves the reservation but disables admissions and
   // sender claims. Never clear history or fall back to fill-level deduplication.
   disable('RTDS_DIRECT_ADMISSION_SAFETY_STOP');
   return {kind:'BLOCKED',reason:/^[A-Z0-9_]+$/.test(error.message)?error.message:'DIRECT_ADMISSION_FAILURE',committed};
  }
 };
 // The caller starts this budget before acquiring its existing claim transaction
 // and checks it again after commit. No migrated historical row is replayable.
 admit.beforeClaim=function(row,started){
  try{
   age(started);
   if(!isEnabled()||!sendOnce.applies(row.wallet,row.chat_id))throw Error('STRICT_ACTION_PROTECTION_REQUIRED');
   const pair=readCurrentPair(row.wallet,row.chat_id);
   if(!pair||row.source_timestamp<pair.notify_from_ts)return {allow:false,reason:'SUBSCRIPTION_INACTIVE'};
   if(legacyCovered?.get(row.event_key,row.chat_id)||legacyFenced?.get(row.event_key,row.chat_id))return {allow:false,reason:'LEGACY_RESERVED_NO_REPLAY'};
   const before=boundary?.get(row.wallet,row.chat_id)?.before_ms;
   if(before!=null&&row.source_timestamp*1000<before)return {allow:false,reason:'BEFORE_EXISTING_PROTECTION_BOUNDARY'};
   const delivery=db.prepare('SELECT wallet,source_timestamp,state FROM deliveries WHERE event_key=? AND chat_id=?').get(row.event_key,row.chat_id);
   if(!delivery||delivery.wallet!==row.wallet||delivery.source_timestamp!==row.source_timestamp)throw Error('CLAIM_DELIVERY_MISMATCH');
   if(!['PENDING','FAILED_RETRYABLE'].includes(delivery.state))return {allow:false,reason:'DELIVERY_NOT_SENDABLE'};
   const prior=db.prepare('SELECT identity_json,first_event_key FROM send_once_actions WHERE first_event_key=? AND chat_id=?').get(row.event_key,row.chat_id);
   if(!prior||JSON.parse(prior.identity_json)[0]!==row.wallet)throw Error('ACTION_RESERVATION_REQUIRED');
   return {allow:true,state_age_ms:age(started)};
  }catch(error){disable('RTDS_DIRECT_CLAIM_SAFETY_STOP');return {allow:false,reason:/^[A-Z0-9_]+$/.test(error.message)?error.message:'DIRECT_CLAIM_FAILURE'};}
 };
 admit.beforeSend=function(row,started){
  let begun=false;
  try{
   age(started);db.exec('BEGIN IMMEDIATE');begun=true;age(started);
   if(!isEnabled()||!sendOnce.applies(row.wallet,row.chat_id))throw Error('STRICT_ACTION_PROTECTION_REQUIRED');
   const pair=readCurrentPair(row.wallet,row.chat_id);
   if(legacyCovered?.get(row.event_key,row.chat_id)||legacyFenced?.get(row.event_key,row.chat_id))throw Error('LEGACY_RESERVED_NO_REPLAY');
   const before=boundary?.get(row.wallet,row.chat_id)?.before_ms;
   if(before!=null&&row.source_timestamp*1000<before)throw Error('BEFORE_EXISTING_PROTECTION_BOUNDARY');
   const delivery=db.prepare('SELECT wallet,source_timestamp,state,claim_token FROM deliveries WHERE event_key=? AND chat_id=?').get(row.event_key,row.chat_id);
   if(!delivery||delivery.wallet!==row.wallet||delivery.source_timestamp!==row.source_timestamp||delivery.state!=='SENDING'||!row.claimToken||delivery.claim_token!==row.claimToken)throw Error('CURRENT_SEND_CLAIM_REQUIRED');
   const action=db.prepare('SELECT identity_json FROM send_once_actions WHERE first_event_key=? AND chat_id=?').get(row.event_key,row.chat_id);
   if(!action||JSON.parse(action.identity_json)[0]!==row.wallet)throw Error('ACTION_RESERVATION_REQUIRED');
   // A normal unsubscribe is not a global safety failure. The caller must
   // cancel this exact claimed row and must never send it or release its
   // reservation; changed claims and stale checks still fail closed above.
   if(!pair||row.source_timestamp<pair.notify_from_ts){age(started);db.exec('ROLLBACK');begun=false;return {allow:false,reason:'SUBSCRIPTION_INACTIVE'};}
   age(started);db.exec('COMMIT');begun=false;
   return {allow:true,state_age_ms:age(started)};
  }catch(error){
   if(begun)try{db.exec('ROLLBACK');}catch{}
   // The existing claim/reservation stays in place. No uncertain send is retried.
   disable('RTDS_DIRECT_BEFORE_SEND_SAFETY_STOP');
   return {allow:false,reason:/^[A-Z0-9_]+$/.test(error.message)?error.message:'DIRECT_BEFORE_SEND_FAILURE'};
  }
 };
 admit.checkAge=age;
 return admit;
}
