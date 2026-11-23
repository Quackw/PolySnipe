import fs from 'node:fs';
import path from 'node:path';
import { createRequire } from 'node:module';
import { randomUUID,randomBytes,createHmac } from 'node:crypto';
import { monitorEventLoopDelay,performance } from 'node:perf_hooks';
import {RtdsPeer} from '../snipe-rtds-resilient/connection.mjs';
import {BufferedRtdsSocket} from '../snipe-rtds-resilient/buffered-socket.mjs';
import {ensureIngressIndexes} from '../snipe-rtds-resilient/ingress-indexes.mjs';
import {createIngressControl} from '../snipe-v3-combined/ingress-control.mjs';
import {createDirectAdmissionGate} from '../snipe-rtds-resilient/fast-direct-gate.mjs';
import {createHash} from 'node:crypto';
import { canonicalTrade, eventKey, formatAlert, normalizeAddress } from './identity.mjs';
import { cacheKeys, findCachedMetadata, lookupGammaMetadata, mergeMetadata, rawMetadata } from './metadata.mjs';
import { recoveryController } from './recovery.mjs';
import {createDeliveryClaimGuard} from './delivery-claim.mjs';
import {createFastShutdown} from './shutdown.mjs';
import {isRetryableSqliteLock} from './sqlite-lock.mjs';
import { telegramRequest,deliveryDecision } from './telegram-transport.mjs';
import {createTelegramCircuit} from './telegram-circuit.mjs';
import {createTrace,projection,parseReason} from './trace.mjs';
import {createSendOnce} from './send-once.mjs';
import {createRedeem} from './redeem.mjs';
import {createRedeemEligibility} from '../snipe-v3-readiness/redeem-eligibility.mjs';
import {createFastBridgeOwner} from '../snipe-v3-combined/fast-bridge-owner.mjs';
import {startFastBridge} from '../snipe-v3-combined/fast-bridge-server.mjs';
import {reconnectBoundary,inReconnectOverlap,recordOverlap} from './reconnect-boundary.mjs';

if (process.env.POLYSNIPE_FAST_ENABLE !== '1') throw Error('FAST_OPERATOR_ENABLE_REQUIRED');
const ROOT = process.env.POLYSNIPE_FAST_STATE_DIR ?? '';
if (!path.isAbsolute(ROOT)) throw Error('FAST_STATE_DIRECTORY_REQUIRED');
const stateDirectory = fs.lstatSync(ROOT);
if (!stateDirectory.isDirectory() || stateDirectory.uid !== process.getuid() || (stateDirectory.mode & 0o077)) {
  throw Error('FAST_PRIVATE_STATE_DIRECTORY_REQUIRED');
}
// Resolve the SQLite dependency from this project. Runtime state stays separate.
const Database = createRequire(import.meta.url)('better-sqlite3');
const DB_PATH = path.join(ROOT, 'state.db');
const ENABLE = path.join(ROOT, 'ENABLE_CANARY');
const STOP = path.join(ROOT, 'FAST_DISABLED');
const HEALTH = path.join(ROOT, 'health.json');
const LABELS = path.join(ROOT, 'wallet-labels.json');
const BACKLOG_CLEARED = path.join(ROOT, 'menu-backlog-cleared');
const ACK_START_MS=Date.now(),ACK_RUN_ID='fast-ack-'+ACK_START_MS,ACK_SALT=randomBytes(32);
const ACK_FILE=path.join(ROOT,ACK_RUN_ID+'.jsonl');
const RTDS = 'wss://ws-live-data.polymarket.com/';
const token = process.env.TELEGRAM_BOT_TOKEN;
const expected = (process.env.EXPECTED_TELEGRAM_USERNAME ?? '').trim().replace(/^@/, '').toLowerCase();
if (!token || !/^[a-z][a-z0-9_]{4,31}$/.test(expected)) throw Error('FAST_BOT_ENV_INVALID');
const recoveryEnabled = process.env.POLYSNIPE_RECOVERY_ENABLE === '1';
if (!recoveryEnabled && fs.existsSync(path.join(ROOT, 'recovery-canary.json'))) {
  throw Error('FAST_RECOVERY_CONFIG_REQUIRES_EXPLICIT_ENABLE');
}

function writeJsonAtomic(file, value) {
  const temp = `${file}.${process.pid}.tmp`;
  fs.writeFileSync(temp, JSON.stringify(value) + '\n', { mode: 0o600 });
  fs.renameSync(temp, file);
}

async function telegram(method, body, timeoutMs = 8000, captureReceipt = false) {
  return telegramRequest(token,method,body,{timeoutMs,captureReceipt,onDiagnostic:method==='sendMessage'?d=>console.log('[telegram_transport] '+JSON.stringify(d)):undefined});
}

async function verifyBot() {
  const result = await telegram('getMe', {});
  if (result.kind !== 'OK' || result.value?.result?.username?.toLowerCase() !== expected) throw Error('bot_identity_mismatch');
}

if (process.argv[2] === 'test-owner1') {
  const owner = process.env.OWNER_TELEGRAM_USER_IDS?.split(',')[0];
  if (!/^\d{4,15}$/.test(owner || '')) throw Error('owner_not_configured');
  await verifyBot();
  const result = await telegram('sendMessage', { chat_id: owner,
    text: 'PolySnipe FAST connection test.' });
  console.log(JSON.stringify({ test_message: result.kind, status: result.status }));
  if (result.kind !== 'OK') process.exitCode = 1;
} else if (process.argv.length > 2) {
  throw Error('invalid_command');
} else {
  const ownerIds = (process.env.OWNER_TELEGRAM_USER_IDS ?? '').split(',')
    .map(id => id.trim()).filter(id => /^\d+$/.test(id)).map(Number)
    .filter(id => Number.isSafeInteger(id) && id > 0);
  if (!ownerIds.length) throw Error('owners_not_configured');
  const ownerPlaceholders = ownerIds.map(() => '?').join(',');
  await verifyBot();
  const db = new Database(DB_PATH);
  const trace=createTrace(ROOT,Database,{policy:'SEND_ONCE_OWNER_SCOPE_RECOVERY_OFF'});
  const boundaryStats={canary:true,overlap_ms:2000,optimality_proven:false,overlap_accepted:0,stale_dropped:0};
  db.pragma('busy_timeout = 5000');
  db.pragma('journal_mode = WAL');
  db.exec(`
    CREATE TABLE IF NOT EXISTS cohort (
      wallet TEXT NOT NULL, chat_id INTEGER NOT NULL, notify_from_ts INTEGER NOT NULL,
      PRIMARY KEY(wallet,chat_id)
    );
    CREATE TABLE IF NOT EXISTS deliveries (
      event_key TEXT NOT NULL, chat_id INTEGER NOT NULL, wallet TEXT NOT NULL,
      source_timestamp INTEGER NOT NULL, first_seen_at_ms INTEGER NOT NULL,
      message_html TEXT NOT NULL, state TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0, ambiguous_retries INTEGER NOT NULL DEFAULT 0,
      possible_duplicate INTEGER NOT NULL DEFAULT 0, next_at_ms INTEGER NOT NULL DEFAULT 0,
      claim_token TEXT, http_status INTEGER, last_error TEXT, delivered_at_ms INTEGER,
      send_started_at_ms INTEGER,
      PRIMARY KEY(event_key,chat_id)
    );
    CREATE INDEX IF NOT EXISTS idx_fast_due ON deliveries(state,next_at_ms);
    CREATE INDEX IF NOT EXISTS idx_fast_delivered_at ON deliveries(delivered_at_ms);
    CREATE TABLE IF NOT EXISTS market_metadata_cache (
      cache_key TEXT PRIMARY KEY, metadata_json TEXT NOT NULL, updated_at_ms INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS rtds_metadata_audit (
      event_key TEXT PRIMARY KEY, received_at_ms INTEGER NOT NULL, topic TEXT NOT NULL,
      type TEXT NOT NULL, raw_frame_json TEXT, raw_truncated INTEGER NOT NULL,
      parsed_title_present INTEGER NOT NULL, parsed_slug_present INTEGER NOT NULL,
      raw_title_present INTEGER NOT NULL, raw_slug_present INTEGER NOT NULL,
      result TEXT NOT NULL
    );
  `);
  if (!db.pragma('table_info(deliveries)').some(column => column.name === 'send_started_at_ms')) {
    db.exec('ALTER TABLE deliveries ADD COLUMN send_started_at_ms INTEGER');
  }
  db.prepare(`UPDATE deliveries SET state='DELIVERY_UNKNOWN',claim_token=NULL,last_error='process_interrupted',
    possible_duplicate=1,next_at_ms=0
    WHERE state='SENDING'`).run();
  const insert = db.prepare(`INSERT OR IGNORE INTO deliveries
    (event_key,chat_id,wallet,source_timestamp,first_seen_at_ms,message_html,state,next_at_ms)
    VALUES (?,?,?,?,?,?,'PENDING',?)`);
  const due = db.prepare(`SELECT * FROM deliveries WHERE
    (state='PENDING' OR state='FAILED_RETRYABLE')
    AND possible_duplicate=0 AND next_at_ms<=? ORDER BY next_at_ms LIMIT 1`);
  const recoveryDue = db.prepare(`SELECT * FROM deliveries WHERE state='DELIVERY_UNKNOWN'
    AND 0 AND next_at_ms<=? ORDER BY next_at_ms LIMIT 1`);
  const claimTransaction = db.transaction((now, allowNew, stateStarted) => {
    const row = (allowNew ? due : recoveryDue).get(now);
    if (!row) return null;
    if(resilientMode&&dualAllowed){
      const allowed=directGate.beforeClaim(row,stateStarted);
      if(!allowed.allow){
        if(allowed.reason==='SUBSCRIPTION_INACTIVE'){
          const cancelled=db.prepare(`UPDATE deliveries SET state='CANCELLED_SUBSCRIPTION',next_at_ms=0,last_error='subscription_removed' WHERE event_key=? AND chat_id=? AND state IN ('PENDING','FAILED_RETRYABLE')`).run(row.event_key,row.chat_id);
          if(cancelled.changes!==1)stopFast('RTDS_CANCEL_CLAIM_CHANGED');
        }else stopFast('RTDS_CLAIM_'+allowed.reason);
        return null;
      }
    }
    const recoveryCheck=recovery.beforeClaim(row);
    if(!recoveryCheck.allow)return null;
    const claimToken = randomUUID();
    db.prepare(`UPDATE deliveries SET state='SENDING',claim_token=?,send_started_at_ms=?,attempts=attempts+1,
      ambiguous_retries=ambiguous_retries+? WHERE event_key=? AND chat_id=?`)
      .run(claimToken, now, row.state === 'DELIVERY_UNKNOWN' ? 1 : 0, row.event_key, row.chat_id);
    return { ...row, claimToken, sendStartedAtMs:now, recovery: row.state === 'DELIVERY_UNKNOWN',isRestRecovery:recoveryCheck.isRest };
  });
  const claim=(now,allowNew)=>{
    const stateStarted=performance.now();
    sendOnce.refreshRollback();
    const row=claimTransaction.immediate(now,allowNew,stateStarted);
    return row&&resilientMode&&dualAllowed?{...row,directStateStartedMs:stateStarted}:row;
  };
  const finish = db.prepare(`UPDATE deliveries SET state=?,claim_token=NULL,http_status=?,last_error=?,
    next_at_ms=?,possible_duplicate=?,delivered_at_ms=?
    WHERE event_key=? AND chat_id=? AND state='SENDING' AND claim_token=?`);
  const count = db.prepare('SELECT state,COUNT(*) n FROM deliveries GROUP BY state');
  const extras = db.prepare(`SELECT SUM(CASE WHEN ambiguous_retries>0 THEN 1 ELSE 0 END) ambiguous_retry,
    SUM(possible_duplicate) possible_duplicate FROM deliveries`);
  const latencyRows = db.prepare(`SELECT source_timestamp,first_seen_at_ms,send_started_at_ms,delivered_at_ms
    FROM deliveries WHERE state='DELIVERED' ORDER BY delivered_at_ms DESC LIMIT 1000`);
  const metadataRead = db.prepare('SELECT metadata_json FROM market_metadata_cache WHERE cache_key=?');
  const metadataWrite = db.prepare(`INSERT INTO market_metadata_cache(cache_key,metadata_json,updated_at_ms)
    VALUES(?,?,?) ON CONFLICT(cache_key) DO UPDATE SET metadata_json=excluded.metadata_json,
    updated_at_ms=excluded.updated_at_ms`);
  const metadataAudit = db.prepare(`INSERT OR IGNORE INTO rtds_metadata_audit
    (event_key,received_at_ms,topic,type,raw_frame_json,raw_truncated,
      parsed_title_present,parsed_slug_present,raw_title_present,raw_slug_present,result)
    VALUES(?,?,?,?,?,?,?,?,?,?,?)`);
  const metadataAuditResult = db.prepare('UPDATE rtds_metadata_audit SET result=? WHERE event_key=?');
  const releaseMetadata = db.prepare(`UPDATE deliveries SET message_html=?,next_at_ms=?
    WHERE event_key=? AND state='PENDING'`);
  const metadataStats = { cacheHit:0,cacheMiss:0,lookupSuccess:0,lookupFail:0,
    metadataMiss:0,parserLoss:0,upstreamMissing:0 };
  let activeMetadataLookups = 0;
  function cachedMetadata(hints) {
    try {
      return findCachedMetadata(hints,key => {
        const row = metadataRead.get(key);
        return row ? JSON.parse(row.metadata_json) : null;
      });
    } catch { console.warn('[metadata] cache_read_failed'); }
    return null;
  }
  function rememberMetadata(hints, metadata) {
    if (!metadata.title || !metadata.slug) return;
    const now = Date.now();
    try {
      for (const key of cacheKeys(hints)) {
        const value = key.startsWith('asset:') || key.startsWith('event_asset:')
          ? metadata : { ...metadata,outcome:'' };
        metadataWrite.run(key,JSON.stringify(value),now);
      }
    } catch { console.warn('[metadata] cache_write_failed'); }
  }
  function setMetadataAudit(result, key) {
    try { metadataAuditResult.run(result,key); } catch { console.warn('[metadata] audit_write_failed'); }
  }
  function releaseMetadataCard(html, key) {
    try { releaseMetadata.run(html,Date.now(),key); }
    catch { console.warn('[metadata] release_failed'); /* The degraded PENDING card remains due. */ }
  }
  const stats = { received:0,direct:0,otherWallet:0,invalid:0,reconnect:0,stale:0,errors:0,duplicates:0,
    delivered:0,deliveryUnknown:0,confirmedTelegramFailures:0,unmapped_history_blocked:0 };
  const lag = monitorEventLoopDelay({ resolution: 20 });
  lag.enable();
  let ws = null, active = true, generation = 0, reconnectTimer = null;
  let connectedAt = 0, lastActivityAt = 0, lastMessageAt = 0, baselineMs = 0;
  let sending = false, reconnectFailures = 0;
  const ambiguousAt = [];
  let watched = new Set();
  let cohortSubscriptions = 0;
  let recipientMap = new Map();
  const labels = JSON.parse(fs.readFileSync(LABELS, 'utf8'));
  let walletLabels = new Map();
  const activeSubscriptions = db.prepare(`SELECT s.proxy_address AS wallet,s.telegram_chat_id AS chat_id,
    s.notify_from_ts,a.name,a.pseudonym,a.profile_slug AS profileSlug
    FROM subscriptions s JOIN subscribers u ON u.id=s.subscriber_id
    JOIN tracked_accounts a ON a.proxy_address=s.proxy_address
    LEFT JOIN access_grants g ON g.telegram_user_id=u.telegram_user_id
    WHERE u.chat_type='private' AND u.telegram_chat_id=u.telegram_user_id
      AND (g.telegram_user_id IS NOT NULL OR u.telegram_user_id IN (${ownerPlaceholders})))`);
  const recovery=recoveryController(db,{root:ROOT,ownerIds,enabledDirect:()=>recoveryEnabled&&!!enabledAt(),getSendOnce:()=>sendOnce,traceRecovery:(stage,value,chat,key,raw)=>{
    if(stage==='begin'){const id=trace.begin(value,'REST_RECOVERY','TRADE',JSON.stringify(value));trace.stage(id,'REST_RECOVERY_ACCEPTED',{source:'REST_RECOVERY'});return id;}
    trace.queue(value,key,chat,projection(raw,'REST_RECOVERY','TRADE'),true);
  }});
  if (recoveryEnabled) recovery.listen();
  const cancelPending = db.prepare(`UPDATE deliveries SET state='CANCELLED_SUBSCRIPTION',next_at_ms=0,
    last_error='subscription_removed' WHERE wallet=? AND chat_id=?
    AND state IN ('PENDING','FAILED_RETRYABLE','DELIVERY_UNKNOWN')`);
  const resilientMode=process.env.FAST_RTDS_RESILIENT==='1';
  let guardCurrentScope=null;
  function refreshRecipients() {
    const rows = activeSubscriptions.all(...ownerIds);
    const next = new Map(), nextLabels = new Map(), valid = new Set();
    for (const row of rows) {
      const wallet = normalizeAddress(row.wallet);
      if (!wallet) continue;
      if (!next.has(wallet)) next.set(wallet, []);
      next.get(wallet).push({ chat_id:row.chat_id,notify_from_ts:row.notify_from_ts });
      nextLabels.set(wallet, { name:row.name,pseudonym:row.pseudonym,profileSlug:row.profileSlug });
      valid.add(wallet);
    }
    for (const [wallet, recipients] of recipientMap) {
      const current = new Set(next.get(wallet)?.map(row => row.chat_id) || []);
      for (const row of recipients) if (!current.has(row.chat_id)) {
        let traceCancelled=[];
        try{traceCancelled=db.prepare("SELECT event_key,chat_id,attempts FROM deliveries WHERE wallet=? AND chat_id=? AND state IN ('PENDING','FAILED_RETRYABLE','DELIVERY_UNKNOWN')").all(wallet,row.chat_id);}catch{trace.stage(null,'TRACE_ERROR',{reason:'TRACE_CANCEL_READ_FAILED'});}
        cancelPending.run(wallet,row.chat_id);
        for(const cancelled of traceCancelled)trace.delivery(cancelled,'CANCELLED_SUBSCRIPTION',{reason:'SUBSCRIPTION_REMOVED',attempt:cancelled.attempts,final_delivery_state:'CANCELLED_SUBSCRIPTION'});
      }
    }
    for (const [wallet, label] of Object.entries(labels)) {
      const normalized = normalizeAddress(wallet);
      if (normalized && valid.has(normalized) && !nextLabels.get(normalized)?.profileSlug)
        nextLabels.set(normalized, label);
    }
    recipientMap=next; watched=valid; walletLabels=nextLabels; cohortSubscriptions=rows.length;
    guardCurrentScope?.();
  }
  refreshRecipients();
  if (!resilientMode && (cohortSubscriptions < 7 || watched.size < 7)) throw Error('initial_subscriptions_missing');
  const sendOnce=createSendOnce(db,{root:ROOT,ownerIds,recipientMap,insert,dynamicProtected:resilientMode});
  // Review-only candidate. Default OFF; deployment requires separate approval.
  const bRequested=resilientMode&&process.env.FAST_RTDS_B_ENABLED==='1';
  const bDisabled=path.join(ROOT,'RTDS_B_DISABLED');
  const sourceBaselines={A:0,B:0};let peerA=null,peerB=null,peerTick=null;
  let dualAllowed=false,scopeFingerprint=null;const dualPairs=new Map();
  let capturedBridgeAdmissions=null,bridge=null,bridgeState='OFF';
  const ingressControl=createIngressControl({mono:()=>performance.now(),sink:row=>console.error(JSON.stringify({rtds_ingress:row}))});
  function currentScopeSafe(){return dualAllowed&&!sendOnce.refreshRollback();}
  if(resilientMode){
    const bytes=fs.readFileSync(path.join(ROOT,'dual-rtds-scope.json'));
    const expected=process.env.FAST_DUAL_SCOPE_SHA256;
    if(!/^[a-f0-9]{64}$/.test(expected??'')||createHash('sha256').update(bytes).digest('hex')!==expected)throw Error('DUAL_SCOPE_PIN');
    const doc=JSON.parse(bytes),pairs=doc.scope;
    if(!Array.isArray(pairs)||!pairs.length||pairs.length>1000||sendOnce.snapshot().rollback)throw Error('ALL_CURRENT_PROTECTION_REQUIRED');
    // Map values include the approved per-pair subscription boundary.
    for(const p of pairs){
      if(normalizeAddress(p.wallet)!==p.wallet||!Number.isSafeInteger(p.recipient)||p.recipient<=0||!Number.isSafeInteger(p.notify_from_ts)||p.notify_from_ts<0)throw Error('DUAL_SCOPE_INVALID');
      if(!sendOnce.isOriginallyProtected(p.wallet,p.recipient))throw Error('ALL_CURRENT_PROTECTION_REQUIRED');
      const k=p.wallet+'|'+p.recipient;if(dualPairs.has(k))throw Error('DUAL_SCOPE_DUPLICATE');dualPairs.set(k,p);
    }
    const sorted=[...pairs].sort((a,b)=>a.wallet.localeCompare(b.wallet)||a.recipient-b.recipient).map(({wallet,recipient,notify_from_ts})=>({wallet,recipient,notify_from_ts}));
    scopeFingerprint=createHash('sha256').update(JSON.stringify(sorted)).digest('hex');
    if(doc.fingerprint!==scopeFingerprint)throw Error('DUAL_SCOPE_FINGERPRINT');
    dualAllowed=true;if(!currentScopeSafe())throw Error('ALL_CURRENT_PROTECTION_REQUIRED');
    let lastPublishedWallets='';
    guardCurrentScope=()=>{try{
      if(!currentScopeSafe()){stopFast('RTDS_CURRENT_SCOPE_CHECK_FAILED');return;}
      const wallets=[...watched].sort(),canonical=JSON.stringify(wallets);
      if(canonical!==lastPublishedWallets){bridge?.syncScope(wallets);lastPublishedWallets=canonical;}
    }catch{stopFast('RTDS_CURRENT_SCOPE_CHECK_FAILED');}};
  }
  if(resilientMode)ensureIngressIndexes(db);
  const directGate=createDirectAdmissionGate({db,sendOnce,
    readCurrentPair:(wallet,recipient)=>activeSubscriptions.all(...ownerIds).find(r=>normalizeAddress(r.wallet)===wallet&&r.chat_id===recipient),
    isEnabled:()=>!!enabledAt()&&!sendOnce.refreshRollback()&&(!resilientMode||dualAllowed),
    disable:reason=>stopFast(reason),allowNewPairHistoryBlock:resilientMode});
  const bridgeGate=(...args)=>{
    const admission=directGate(...args);
    if(capturedBridgeAdmissions){
      let ambiguous=false;
      if(admission.kind==='SUPPRESS_SAME_ACTION'){
        const d=db.prepare('SELECT state FROM deliveries WHERE event_key=? AND chat_id=?').get(admission.firstEventKey,args[1]);
        ambiguous=!d||['DELIVERY_UNKNOWN','SENDING'].includes(d.state);
      }
      capturedBridgeAdmissions.push({kind:admission.kind,reason:admission.reason,ambiguous});
    }
    return admission;
  };
  const bridgeOwner=createFastBridgeOwner({scopeFingerprint:scopeFingerprint??'0'.repeat(64),
    isReady:()=>{try{return resilientMode&&!!enabledAt()&&currentScopeSafe();}catch{return false;}},
    dispatch:(envelope,sourceBaseline)=>{
      if(!active||envelope?.topic!=='activity'||!['trades','orders_matched'].includes(envelope.type)||!envelope.payload||typeof envelope.payload!=='object'||Array.isArray(envelope.payload))return [];
      const raw=JSON.stringify(envelope),id=trace.begin(envelope.payload,envelope.topic,envelope.type,raw);
      trace.stage(id,'ENVELOPE_OK',{rtds_source:'B'});stats.received++;lastMessageAt=Date.now();
      capturedBridgeAdmissions=[];
      try{handle(envelope.payload,envelope.topic,envelope.type,raw,id,'B',sourceBaseline);return capturedBridgeAdmissions;}
      catch{stopFast('bridge_handler_error');throw Error('BRIDGE_HANDLER_FAILURE');}
      finally{capturedBridgeAdmissions=null;}
    }});
  function makeDirectPeer(source){
    return new RtdsPeer({source,continuous:true,mono:()=>performance.now(),wall:Date.now,
      staleAfterMs:8000,
      socketFactory:url=>{sourceBaselines[source]=Date.now();return new BufferedRtdsSocket(url,{onDiagnostic:row=>console.error(JSON.stringify({rtds_A_buffer:row}))});},
      sink:row=>console.log(JSON.stringify({rtds_lifecycle:row})),
      onOpen:({generation:gen})=>{if(source==='A'){generation=gen;baselineMs=sourceBaselines.A;connectedAt=baselineMs;lastActivityAt=0;}},
      onActivity:(envelope,context,raw)=>{
        if(!active)return;
        const traceId=trace.begin(envelope.payload,envelope.topic,envelope.type,raw);
        trace.stage(traceId,'ENVELOPE_OK',{rtds_source:source,source_generation:context.generation,buffer_receipt_id:context.buffer_receipt_id,buffer_wait_ms:context.buffer_wait_ms});
        stats.received++;lastActivityAt=Date.now();lastMessageAt=Date.now();
        try{handle(envelope.payload,envelope.topic,envelope.type,raw,traceId,source,sourceBaselines[source]);}
        catch{trace.stage(traceId,'HANDLER_ERROR',{reason:'MESSAGE_HANDLER_EXCEPTION'});stats.errors++;stopFast('message_handler_error');}
      }});
  }
  const redeem=createRedeem(db,{
    config:JSON.parse(fs.readFileSync(path.join(ROOT,'redeem-config.json'),'utf8')),
    getRecipients:()=>recipientMap,getLabel:wallet=>walletLabels.get(wallet),
    enabled:()=>process.env.POLYSNIPE_REDEEM_ENABLE==='1'&&active&&!!enabledAt()&&!fs.existsSync(path.join(ROOT,'REDEEM_DISABLED')),telegram,
    freshEligibility:createRedeemEligibility({activeSubscriptions,ownerIds,normalizeAddress}),
    onFatal:()=>stopFast('REDEEM_RATE_GUARD_PERSISTENCE_FAILED')
  });
  function enabledAt() {
    try { return fs.existsSync(STOP) ? 0 : fs.statSync(ENABLE).mtimeMs; } catch { return 0; }
  }
  function stopFast(reason) {
    try { fs.writeFileSync(STOP, `${new Date().toISOString()} ${reason}\n`, { flag:'wx',mode:0o600 }); }
    catch (e) { if (e.code !== 'EEXIST') console.error('[fast] disable marker unavailable'); }
    console.error(`[fast] disabled reason=${reason}`);
  }
  function health(final = false) {
    const x = extras.get();
    const latencies = latencyRows.all().map(row => Math.max(0,row.delivered_at_ms-row.source_timestamp*1000)).sort((a,b)=>a-b);
    const latency = { n:latencies.length,median_ms:latencies[Math.floor((latencies.length-1)*.5)]??null,
      p90_ms:latencies[Math.floor((latencies.length-1)*.9)]??null,max_ms:latencies.at(-1)??null };
    writeJsonAtomic(HEALTH, { at_ms:Date.now(),final,enabled:!!enabledAt(),telegram_transport:telegramCircuit.snapshot(),
      cohort_wallets:watched.size,subscriptions:cohortSubscriptions,connected:resilientMode?(peerA?.phase==='OPEN'||bridgeState==='B_AUTHENTICATED'):ws?.readyState===WebSocket.OPEN,
      rtds_peers:resilientMode?{A:peerA?.health()??null,B:{channel:bridgeState,health:bridge?.health()??null}}:null,
      rtds_ingress:resilientMode?ingressControl.sample({A:peerA?.health(),B:bridge?.health(),BChannel:bridgeState}):null,
      rtds_A_buffer:resilientMode?peerA?.session?.socket?.snapshot?.()??null:null,
      generation,baseline_ms:baselineMs,last_activity_at_ms:lastActivityAt,last_message_at_ms:lastMessageAt,
      stats,delivery_states:Object.fromEntries(count.all().map(r=>[r.state,r.n])),
      ambiguous_retry:x.ambiguous_retry||0,possible_duplicate:x.possible_duplicate||0,
      end_to_end_latency_ms:latency,
      ack_verification:{run_id:ACK_RUN_ID,start_ms:ACK_START_MS,audit_file:ACK_FILE,policy:'API_OK_MESSAGE_ID_NO_UNKNOWN_REPLAY'},
      event_loop_p99_ms:lag.count ? lag.percentile(99)/1e6 : null,event_loop_max_ms:lag.max/1e6,
      activity_http_requests:redeem.snapshot().http_requests,redeem:redeem.snapshot(),metadata:metadataStats,recovery_canary:recovery.stats(),
      send_once_canary:sendOnce.snapshot(),reconnect_boundary:boundaryStats,trace:trace.snapshot() });
  }
  function handle(payload, topic, type, rawFrame,traceId,source='A',sourceBaselineMs=baselineMs) {
    const wallet = normalizeAddress(payload?.proxyWallet);
    if (!wallet || !watched.has(wallet)) { trace.stage(traceId,'ATTRIBUTION_REJECT',{reason:wallet?'OTHER_PROXY_WALLET':'INVALID_PROXY_WALLET',monitored_wallet_match:false});stats.otherWallet++; return; }
    if(resilientMode&&!currentScopeSafe()){stopFast('RTDS_CURRENT_SCOPE_CHANGED');return;}
    trace.stage(traceId,'ATTRIBUTION_MATCH',{monitored_wallet_match:true,subscription_recipients:(recipientMap.get(wallet)||[]).map(r=>({recipient:String(r.chat_id),notify_from_ts:r.notify_from_ts}))});
    const enabled = enabledAt();
    if (!enabled) {trace.stage(traceId,'SUBSCRIPTION_REJECT',{reason:'FAST_DIRECT_DISABLED'});return;}
    const effectiveBoundary=reconnectBoundary(sourceBaselineMs,enabled);
    const trade = canonicalTrade(payload, wallet, effectiveBoundary);
    if (!trade) { const reason=parseReason(payload,wallet,effectiveBoundary,Date.now());if(reason==='STALE_AFTER_RECONNECT_OR_ENABLE_BOUNDARY')boundaryStats.stale_dropped++;
      trace.stage(traceId,'PARSE_REJECT',{reason,baseline_ms:sourceBaselineMs,effective_boundary_ms:effectiveBoundary,enabled_at_ms:enabled});stats.invalid++; return; }
    if(inReconnectOverlap(payload.timestamp,sourceBaselineMs,enabled)){
      const distance_ms=recordOverlap(boundaryStats,payload.timestamp,sourceBaselineMs);
      trace.stage(traceId,'RECONNECT_OVERLAP_ACCEPTED',{reason:'RECONNECT_OVERLAP_ACCEPTED',distance_to_boundary_ms:distance_ms,overlap_ms:2000,baseline_ms:sourceBaselineMs,effective_boundary_ms:effectiveBoundary,source_timestamp:trade.timestamp});
    }
    trace.stage(traceId,'PARSE_OK');
    const recipients = recipientMap.get(wallet) || [];
    if (!recipients.length) {trace.stage(traceId,'SUBSCRIPTION_REJECT',{reason:'NO_ACTIVE_RECIPIENT'});return;}
    stats.direct++;
    const now = Date.now(), key = eventKey(trade);
    recovery.onDirect(key,now);
    recovery.onDirectAction?.(trade,now);
    const hints = rawMetadata(payload);
    let enriched = mergeMetadata(trade,hints);
    if (hints.title && hints.slug) rememberMetadata(hints,{ ...hints,marketUrl:`https://polymarket.com/market/${encodeURIComponent(hints.slug)}`,updatedAt:now });
    const parserIncomplete = !trade.market || !trade.slug;
    const parserLoss = (!!hints.title && !trade.market) || (!!hints.slug && !trade.slug);
    let incomplete = !enriched.market || !enriched.slug;
    if (parserIncomplete) {
      if (incomplete) {
        const cached = cachedMetadata(hints);
        if (cached) { enriched=mergeMetadata(enriched,cached);metadataStats.cacheHit++; }
        else metadataStats.cacheMiss++;
        incomplete = !enriched.market || !enriched.slug;
      }
      if (parserLoss) metadataStats.parserLoss++;
      else metadataStats.upstreamMissing++;
      const bounded = Buffer.byteLength(rawFrame) <= 65536 ? rawFrame : null;
      try {
        metadataAudit.run(key,now,topic,type,bounded,bounded ? 0 : 1,
          trade.market ? 1 : 0,trade.slug ? 1 : 0,hints.title ? 1 : 0,hints.slug ? 1 : 0,
          incomplete ? 'PENDING_LOOKUP' : parserLoss ? 'PARSER_RECOVERED' : 'CACHE_HIT');
      } catch { console.warn('[metadata] audit_write_failed'); }
    }
    const html = formatAlert(enriched,walletLabels.get(wallet));
    let inserted = 0;
    for (const recipient of recipients) {
      if(resilientMode&&!sendOnce.applies(wallet,recipient.chat_id)){stopFast('RTDS_CURRENT_PAIR_UNPROTECTED');continue;}
      if (trade.timestamp < recipient.notify_from_ts) {trace.stage(traceId,'SUBSCRIPTION_REJECT',{reason:'BEFORE_SUBSCRIPTION_START',recipient:String(recipient.chat_id),notify_from_ts:recipient.notify_from_ts});continue;}
      trace.stage(traceId,'SUBSCRIPTION_MATCH',{recipient:String(recipient.chat_id)});
      let result;
      if(sendOnce.applies(wallet,recipient.chat_id)){
        let admission;
        try{admission=(resilientMode?bridgeGate:sendOnce.admit)(trade,recipient.chat_id,key,traceId,html,
          incomplete ? now+2400 : now,now);}
        catch{trace.stage(traceId,'SEND_ONCE_ADMISSION_ERROR',{reason:'ACTION_ADMISSION_ERROR'});continue;}
        if(admission.kind==='BLOCKED'){if(admission.reason==='UNMAPPED_LEGACY_DELIVERY')stats.unmapped_history_blocked++;trace.stage(traceId,'DIRECT_ADMISSION_BLOCKED',{reason:admission.reason});continue;}
        if(admission.kind==='SUPPRESS_SAME_ACTION'){
          recovery.onSuppressed?.(trade,recipient.chat_id,admission);
          trace.stage(traceId,'SUPPRESS_SAME_ACTION',{action_key:admission.actionKey,
            canonical_key:key,recipient:String(recipient.chat_id),
            occupying_trace_id:admission.firstTraceId,first_event_key:admission.firstEventKey});
          stats.duplicates++;continue;
        }
        if(admission.kind==='FALSE_USER_ACTION_MERGE'||admission.kind==='FIRST_DIRECT_EVENT_DROPPED'){
          trace.stage(traceId,admission.kind,{action_key:admission.actionKey,
            canonical_key:key,recipient:String(recipient.chat_id)});
          result=insert.run(key,recipient.chat_id,wallet,trade.timestamp,now,html,
            incomplete ? now+2400 : now);
        }else{
          result=admission.kind==='SEND_FIRST_ACTION'?{changes:1}:
            insert.run(key,recipient.chat_id,wallet,trade.timestamp,now,html,
              incomplete ? now+2400 : now);
        }
      }else result=insert.run(key,recipient.chat_id,wallet,trade.timestamp,now,html,
        incomplete ? now+2400 : now);
      trace.queue(traceId,key,recipient.chat_id,projection(payload,topic,type),!!result.changes);
      if (!result.changes) stats.duplicates++;
      else inserted++;
    }
    if (!incomplete || !inserted) return;
    if (activeMetadataLookups >= 2) {
      metadataStats.lookupFail++;metadataStats.metadataMiss++;
      setMetadataAudit('METADATA_MISS_CONCURRENCY',key);
      console.warn('[metadata] METADATA_MISS reason=concurrency');
      releaseMetadataCard(html,key);
      return;
    }
    activeMetadataLookups++;
    void lookupGammaMetadata(hints,{ timeoutMs:2000 }).then(metadata => {
      if (metadata) {
        rememberMetadata(hints,metadata);
        const card=formatAlert(mergeMetadata(enriched,metadata),walletLabels.get(wallet));
        releaseMetadataCard(card,key);
        metadataStats.lookupSuccess++;setMetadataAudit('GAMMA_SUCCESS',key);
      } else {
        releaseMetadataCard(html,key);
        metadataStats.lookupFail++;metadataStats.metadataMiss++;
        setMetadataAudit('METADATA_MISS',key);
        console.warn('[metadata] METADATA_MISS reason=lookup');
      }
    }).catch(() => {
      releaseMetadataCard(html,key);setMetadataAudit('METADATA_MISS_ERROR',key);
      metadataStats.lookupFail++;metadataStats.metadataMiss++;
      console.warn('[metadata] METADATA_MISS reason=error');
    }).finally(() => { activeMetadataLookups--; });
  }
  function reconnect(gen, reason) {
    if (!active || gen !== generation || reconnectTimer) return;
    stats.reconnect++; reconnectFailures++;
    console.log(`[rtds] reconnect reason=${reason}`);
    try { ws?.close(); } catch {}
    reconnectTimer = setTimeout(() => { reconnectTimer=null; connect(); }, Math.min(30_000,1000*2**Math.min(reconnectFailures,5)));
  }
  function connect() {
    if (!active) return;
    const gen = ++generation;
    baselineMs = Date.now(); connectedAt = baselineMs; lastActivityAt = 0;
    try { ws = new WebSocket(RTDS); } catch { stats.errors++; reconnect(gen,'construct'); return; }
    ws.addEventListener('open', () => {
      if (!active || gen !== generation) return;
      reconnectFailures = 0;
      try { ws.send(JSON.stringify({ action:'subscribe',subscriptions:[
        { topic:'activity',type:'trades' },{ topic:'activity',type:'orders_matched' }
      ] })); } catch { stats.errors++; reconnect(gen,'subscribe'); }
    });
    ws.addEventListener('message', event => {
      const raw = String(event.data);
      if (!active || gen !== generation) {const id=trace.begin(null,null,null,raw);trace.stage(id,'SOURCE_REJECT',{reason:!active?'PROCESS_STOPPING':'OBSOLETE_CONNECTION_GENERATION'});return;}
      lastMessageAt = Date.now();
      if (raw.toLowerCase() === 'pong' || raw.toLowerCase() === 'ping') return;
      let envelope;
      try { envelope = JSON.parse(raw); }
      catch {const id=trace.begin(null,null,null,raw);trace.stage(id,'PARSE_REJECT',{reason:'INVALID_JSON'});stats.invalid++; return; }
      const traceId=trace.begin(envelope?.payload,envelope?.topic,envelope?.type,raw);
      if (!envelope || typeof envelope !== 'object') {trace.stage(traceId,'PARSE_REJECT',{reason:'INVALID_ENVELOPE'});stats.invalid++; return; }
      try {
        if (envelope.topic !== 'activity' || !['trades','orders_matched'].includes(envelope.type)) {trace.stage(traceId,'SOURCE_REJECT',{reason:'UNSUBSCRIBED_TOPIC_TYPE'});return;}
        trace.stage(traceId,'ENVELOPE_OK');
        stats.received++; lastActivityAt = Date.now(); handle(envelope.payload,envelope.topic,envelope.type,raw,traceId);
      } catch {trace.stage(traceId,'HANDLER_ERROR',{reason:'MESSAGE_HANDLER_EXCEPTION'});stats.errors++; stopFast('message_handler_error'); }
    });
    ws.addEventListener('error', () => { stats.errors++; reconnect(gen,'error'); });
    ws.addEventListener('close', () => reconnect(gen,'close'));
  }
  const telegramCircuit=createTelegramCircuit();
  const claimGuard=createDeliveryClaimGuard(db,{onClaimLost:row=>{
    if(row.isRestRecovery)recovery.stop('RECOVERY_CLAIM_LOST');
    else stopFast('delivery_claim_lost');
  }});
  function beforeTelegramRequest(row) {
    if(!active||!enabledAt())return 'FAST_DISABLED_BEFORE_SEND';
    console.log(`[telegram] send_started_at=${new Date(row.sendStartedAtMs).toISOString()} rtds_received_at=${new Date(row.first_seen_at_ms).toISOString()} recovery=${row.recovery}`);
    trace.delivery(row,'SEND_ATTEMPT',{send_started_ms:row.sendStartedAtMs,source:row.isRestRecovery?'REST_RECOVERY':'FAST_DIRECT'});
    if(row.directStateStartedMs!==undefined){
      const current=directGate.beforeSend(row,row.directStateStartedMs);
      if(!current.allow){
        trace.delivery(row,'SEND_SKIPPED',{reason:'RTDS_FINAL_'+current.reason});
        if(current.reason!=='SUBSCRIPTION_INACTIVE'&&!isRetryableSqliteLock({code:current.reason}))stopFast('RTDS_FINAL_'+current.reason);
        return current.reason;
      }
    }
    if(!active||!enabledAt())return 'FAST_DISABLED_BEFORE_SEND';
    if(!recovery.beforeSend(row)){
      trace.delivery(row,'SEND_SKIPPED',{reason:'RECOVERY_BEFORE_SEND_REJECT'});
      return 'RECOVERY_BEFORE_SEND_REJECT';
    }
    if(row.directStateStartedMs!==undefined)try{directGate.checkAge(row.directStateStartedMs);}
    catch{stopFast('RTDS_FINAL_STATE_TOO_STALE');return 'STATE_TOO_STALE';}
  }
  function requestTelegram(row) {
    const result=telegram('sendMessage',{chat_id:String(row.chat_id),text:row.message_html,
      parse_mode:'HTML',disable_web_page_preview:true},8000,row.isRestRecovery);
    // Write recovery attempt evidence only after handing the request to transport.
    try{recovery.markSendStarted(row);}
    catch{
      console.error('[recovery] send_audit_failed');
      try{recovery.stop('RECOVERY_SEND_AUDIT_ERROR');}catch{stopFast('RECOVERY_STOP_FAILED');}
    }
    return result;
  }
  async function sendDue() {
    if (sending || !claimGuard.flush() || !active || !telegramCircuit.ready()) return;
    const row = claim(Date.now(),!!enabledAt());
    if (!row) {
      sending=true;
      try {await redeem.sendDue();} finally {sending=false;}
      return;
    }
    sending = true;
    try {
      await claimGuard.run(row,beforeTelegramRequest,requestTelegram,(result,settled)=>{
        const transportHealth=telegramCircuit.observe(result);
        if(result.kind==='AMBIGUOUS')console.error('[telegram_circuit] '+JSON.stringify(transportHealth));
        const now = Date.now();
        const {state,next,error,possible,delivered}=deliveryDecision(result,row,now);
        if (result.kind === 'OK') { stats.delivered++; }
        else if (result.kind === 'AMBIGUOUS') {
          stats.deliveryUnknown++;
          if(!row.isRestRecovery) {
            ambiguousAt.push(now);
            while (ambiguousAt[0] < now-3600000) ambiguousAt.shift();
            // UNKNOWN is terminal; pause only consecutive transport failures, keep intake active.
          }
        } else if(state==='FAILED_FINAL'){stats.confirmedTelegramFailures++;}
        const updated=finish.run(state,result.status,error,next,possible,delivered,row.event_key,row.chat_id,row.claimToken);
        if (updated.changes === 1) {
          settled();
          sendOnce.observeAck(row,result);
        }
        trace.delivery(row,result.kind==='OK'?'TELEGRAM_ACK':result.kind==='AMBIGUOUS'?'DELIVERY_UNKNOWN':'SEND_ERROR',{http_status:result.status,telegram_ok:result.value?.ok??null,message_id:result.value?.result?.message_id??null,recipient_matches:result.kind==='OK'?String(result.value.result.chat?.id)===String(row.chat_id):null,reason:result.reason??null,error_code:result.error_code??null,final_delivery_state:state,delivery_row_updated:updated.changes===1,finished_ms:now});
        const tag=x=>createHmac('sha256',ACK_SALT).update(String(x)).digest('hex');
        fs.appendFileSync(ACK_FILE,JSON.stringify({run_id:ACK_RUN_ID,at_ms:now,event:tag(row.event_key),recipient:tag(row.chat_id),attempt:row.attempts+1,state,kind:result.kind,http_status:result.status,error_code:result.error_code??null,reason:result.reason??null,retry_after_ms:result.retry_after_ms??null,ack_ok:result.value?.ok===true,message_id_present:Number.isSafeInteger(result.value?.result?.message_id),receipt:result.kind==='OK'?tag(row.chat_id+'|'+result.value.result.message_id):null,recipient_matches:result.kind==='OK'?String(result.value.result.chat?.id)===String(row.chat_id):null,source:row.isRestRecovery?'REST_RECOVERY':'FAST_DIRECT',first_seen_ms:row.first_seen_at_ms,source_timestamp:row.source_timestamp,send_started_ms:row.sendStartedAtMs,latency_ms:Math.max(0,now-row.source_timestamp*1000)})+'\n',{mode:0o600});
        if(row.isRestRecovery) recovery.afterSend(row,{kind:result.kind,status:result.status,
          message_id:result.value?.result?.message_id??null,telegram_ok:result.value?.ok===true,delivery_row_updated:updated.changes===1,
          recipient_matches:result.kind==='OK'?String(result.value?.result?.chat?.id)===String(row.chat_id):null},state,delivered);
        if (result.kind === 'OK') console.log(`[telegram] success_at=${new Date(now).toISOString()} end_to_end_ms=${Math.max(0,now-row.source_timestamp*1000)} duplicate_suppressed=${stats.duplicates}`);
        if (result.kind !== 'OK') console.error(`[telegram] state=${state} status=${result.status ?? 'unknown'} reason=${result.reason??'UNCLASSIFIED'}`);
      });
    } catch(e) {
      trace.delivery(row,'SEND_EXCEPTION',{reason:'SEND_WORKER_EXCEPTION'});
      if(row.isRestRecovery)recovery.stop('RECOVERY_SEND_WORKER_ERROR');else throw e;
    } finally { sending=false; }
  }
  process.env.SQLITE_PATH = DB_PATH;
  const [{ AppRepository }, { createTelegramBot }, { DefaultPolymarketClient }, { loadConfig }] = await Promise.all([
    import('../snipe-v2-runtime/src/db/repository.js'),
    import('../snipe-v2-runtime/src/bot.js'),
    import('../snipe-v2-runtime/src/apis/polymarket.js'),
    import('../snipe-v2-runtime/src/config.js')
  ]);
  const menuConfig = loadConfig();
  if (menuConfig.expectedTelegramUsername !== expected || menuConfig.sqlitePath !== DB_PATH)
    throw Error('menu_config_invalid');
  const menuRepo = new AppRepository(db);
  const profileClient = new DefaultPolymarketClient(menuConfig);
  const menuClient = {
    resolveAccountInput: input => profileClient.resolveAccountInput(input),
    fetchRecentUserActivity: () => { throw Error('fast_activity_forbidden'); }
  };
  const bot = createTelegramBot(menuConfig,menuRepo,menuClient,undefined,refreshRecipients);
  await bot.init();
  if (bot.botInfo.username.toLowerCase() !== expected) throw Error('menu_bot_identity_mismatch');
  const webhook = await bot.api.getWebhookInfo();
  if (webhook.url) throw Error('webhook_conflict');
  if (!fs.existsSync(BACKLOG_CLEARED)) {
    await bot.api.deleteWebhook({ drop_pending_updates:true });
    fs.writeFileSync(BACKLOG_CLEARED,`${new Date().toISOString()} discarded_pending=${webhook.pending_update_count || 0}\n`,
      { flag:'wx',mode:0o600 });
    console.log(`[menu] backlog_discarded count=${webhook.pending_update_count || 0}`);
  }
  if(resilientMode){
    peerA=makeDirectPeer('A');peerA.start();
    if(bRequested&&dualAllowed&&!fs.existsSync(bDisabled))try{
      const cfg=JSON.parse(fs.readFileSync(path.join(ROOT,'rtds-b-bridge.json'),'utf8'));
      if(cfg.scope_fingerprint!==scopeFingerprint)throw Error('BRIDGE_SCOPE_MISMATCH');
      const directory=path.join(ROOT,'rtds-b-private');
      bridge=startFastBridge({directory,keyPath:path.join(directory,'bridge.key'),runId:cfg.run_id,scopeFingerprint,owner:bridgeOwner,getWallets:()=>[...watched].sort(),healthEnabled:true,onState:value=>{bridgeState=value;console.log('[rtdsB] channel='+value);}});
      bridge.ready.catch(()=>{bridgeState='B_CHANNEL_ERROR';});
    }catch{bridgeState='B_CHANNEL_ERROR';}
    let scopeCheckedAt=0;peerTick=setInterval(()=>{peerA.tick();const at=Date.now();if(at-scopeCheckedAt>=1000){scopeCheckedAt=at;try{refreshRecipients();}catch{stopFast('RTDS_CURRENT_SCOPE_CHECK_FAILED');}}if(bridge&&fs.existsSync(bDisabled)){bridge.close();bridge=null;bridgeState='OFF';}ingressControl.sample({A:peerA.health(),B:bridge?.health(),BChannel:bridgeState});},250);
  }else connect();
  const menuTask = bot.start({ onStart: () => console.log('[menu] polling_active') })
    .catch(() => { console.error('[menu] polling_failed'); stopFast('menu_polling_failed'); });
  const worker=setInterval(() => {
    void sendDue().catch(error => {
      if(!isRetryableSqliteLock(error))stopFast('send_worker_error');
    });
  },250);
  const watchdog=setInterval(() => {
    if(resilientMode)return;
    const now=Date.now();
    if (ws?.readyState===WebSocket.OPEN) {
      if (now-(lastActivityAt||connectedAt)>30000) { stats.stale++;reconnect(generation,'stale'); }
      else { try { ws.send('ping'); } catch { stats.errors++;reconnect(generation,'ping'); } }
    } else if (now-connectedAt>15000) reconnect(generation,'connect_timeout');
  },5000);
  const reporter=setInterval(() => { try { health(); } catch { console.error('[health] write_failed'); } },5000);
  const redeemPoller=setInterval(()=>{void redeem.poll().catch(()=>console.error('[redeem] poll_failed'));},10000);
  void redeem.poll().catch(()=>console.error('[redeem] poll_failed'));
  let lastOtherWalletLog=0;
  const diagnostics=setInterval(() => {
    const delta=stats.otherWallet-lastOtherWalletLog;
    lastOtherWalletLog=stats.otherWallet;
    if (delta>0) console.log(`[rtds] RTDS_OTHER_PARTY count=${delta}`);
  },60000);
  health();
  const shutdown=createFastShutdown([worker,watchdog,reporter,diagnostics,redeemPoller],async()=>{
    active=false;
    await redeem.stop();
    try { bot.stop(); } catch {}
    if (reconnectTimer) clearTimeout(reconnectTimer);
    try { ws?.close(); } catch {}
    const deadline=Date.now()+10000;
    await Promise.race([menuTask,new Promise(resolve=>setTimeout(resolve,3000))]);
    while (sending && Date.now()<deadline) await new Promise(resolve=>setTimeout(resolve,50));
    if (sending || !claimGuard.flush()) stopFast('shutdown_send_unresolved');
    clearInterval(peerTick);peerA?.stop('FAST_SHUTDOWN');bridge?.close();
    recovery.close();trace.close();health(true);db.close();
  });
  let exiting=false;
  function onSignal(){
    if(exiting)return;
    exiting=true;
    void shutdown().then(()=>process.exit(0),()=>{
      console.error('[fast] shutdown_failed');
      process.exit(1);
    });
  }
  process.on('SIGTERM',onSignal);
  process.on('SIGINT',onSignal);
}
