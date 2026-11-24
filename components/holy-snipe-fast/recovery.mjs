// Recovery is an additional input to the EXISTING single sender and canonical PK.
// This module never performs HTTP and never changes direct-alert eligibility.
import fs from 'node:fs';
import net from 'node:net';
import { performance } from 'node:perf_hooks';
import { createHmac } from 'node:crypto';
import { canonicalTrade, eventKey, formatAlert, userActionKey, userActionIdentity, normalizeAddress } from './identity.mjs';

export function recoveryController(db, {root, ownerIds, enabledDirect, getSendOnce, traceRecovery, now=Date.now, mono=()=>performance.now()}) {
  const configFile=root+'/recovery-canary.json', stopFile=root+'/RECOVERY_DISABLED';
  let config=null, lease=-Infinity;
  if (fs.existsSync(configFile)) config=JSON.parse(fs.readFileSync(configFile,'utf8'));
  db.exec(`CREATE TABLE IF NOT EXISTS recovery_canary_audit (
    event_key TEXT NOT NULL,chat_id INTEGER NOT NULL,source TEXT NOT NULL DEFAULT 'REST_RECOVERY',
    trade_json TEXT NOT NULL,rest_first_seen_ms INTEGER NOT NULL,grace_deadline_ms INTEGER NOT NULL,
    deadline_check_ms INTEGER NOT NULL,deadline_state_age_ms REAL NOT NULL,
    final_check_ms INTEGER,final_state_age_ms REAL,send_started_ms INTEGER,
    response_json TEXT,delivered_at_ms INTEGER,late_fast_ms INTEGER,
    state TEXT NOT NULL,origin TEXT NOT NULL,attempt_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(event_key,chat_id));`);
  const actionMode=config?.version===2;
  if(actionMode)for(const column of ['action_key','reservation_trace_id','late_fast_suppressed_ms'])if(!db.prepare('PRAGMA table_info(recovery_canary_audit)').all().some(r=>r.name===column))db.exec('ALTER TABLE recovery_canary_audit ADD COLUMN '+column+(column.endsWith('_ms')?' INTEGER':' TEXT'));
  if(actionMode)db.exec('CREATE INDEX IF NOT EXISTS idx_recovery_action_run ON recovery_canary_audit(action_key,chat_id,rest_first_seen_ms)');
  const audit=(key,chat)=>db.prepare('SELECT * FROM recovery_canary_audit WHERE event_key=? AND chat_id=?').get(key,chat);
  const directRows=()=>db.prepare(`SELECT s.proxy_address AS wallet,s.telegram_chat_id AS chat_id,s.notify_from_ts,
    a.name,a.pseudonym,a.profile_slug AS profileSlug FROM subscriptions s
    JOIN subscribers u ON u.id=s.subscriber_id JOIN tracked_accounts a ON a.proxy_address=s.proxy_address
    WHERE u.chat_type='private' AND u.telegram_chat_id=u.telegram_user_id ${actionMode?`AND (u.telegram_user_id IN (${ownerIds.map(()=>'?').join(',')||'NULL'}) OR EXISTS(SELECT 1 FROM access_grants g WHERE g.telegram_user_id=u.telegram_user_id))`:''}`).all(...(actionMode?ownerIds:[]));
  function validConfig() {
    if(actionMode)return typeof config.run_id==='string' && Array.isArray(config.subscriptions) && config.subscriptions.length>0 &&
      config.subscriptions.length===config.expected_subscriptions &&
      new Set(config.subscriptions.map(s=>s.wallet+'|'+s.recipient)).size===config.subscriptions.length &&
      config.subscriptions.every(s=>normalizeAddress(s.wallet)===s.wallet&&Number.isSafeInteger(s.recipient)&&s.recipient>0) && typeof getSendOnce==='function';
    return config?.version===1 && config.recipients?.length>=1 && config.recipients.length<=3 &&
      config.recipients.every(x=>ownerIds.includes(x)) && Array.isArray(config.wallets) && config.wallets.length>0;
  }
  function scope() {return validConfig()?directRows().filter(r=>actionMode
    ?config.subscriptions.some(s=>s.wallet===r.wallet.toLowerCase()&&s.recipient===r.chat_id)&&getSendOnce().applies(r.wallet.toLowerCase(),r.chat_id)
    :config.recipients.includes(r.chat_id)&&config.wallets.includes(r.wallet.toLowerCase())):[];}
  function claimFor(action,chat){return db.prepare('SELECT * FROM send_once_actions WHERE action_key=? AND chat_id=?').get(action,chat);}
  function actionAudit(action,chat){return db.prepare('SELECT * FROM recovery_canary_audit WHERE action_key=? AND chat_id=? AND rest_first_seen_ms>=? ORDER BY rest_first_seen_ms DESC LIMIT 1').get(action,chat,config.start_ms);}
  function releaseUnsent(a){
    if(a.send_started_ms||['RECOVERY_CANCELLED_BEFORE_SEND','RECOVERY_STOPPED'].includes(a.state))return;
    const claim=claimFor(a.action_key,a.chat_id);
    if(!claim||claim.first_event_key!==a.event_key||claim.first_trace_id!==a.reservation_trace_id)return;
    const released=db.prepare("DELETE FROM deliveries WHERE event_key=? AND chat_id=? AND state='PENDING'").run(a.event_key,a.chat_id);
    if(released.changes!==1)return;
    db.prepare('DELETE FROM send_once_actions WHERE action_key=? AND chat_id=? AND first_event_key=?').run(a.action_key,a.chat_id,a.event_key);
  }
  function stop(reason) {
    if (!config) return;
    try {fs.writeFileSync(stopFile,JSON.stringify({at:now(),reason})+'\n',{flag:'wx',mode:0o600});}
    catch(e) {if(e.code!=='EEXIST') throw e;}
    db.transaction(()=>{
      if(actionMode){
        for(const a of db.prepare('SELECT * FROM recovery_canary_audit WHERE rest_first_seen_ms>=? AND send_started_ms IS NULL').all(config.start_ms??Number.MAX_SAFE_INTEGER)){
          releaseUnsent(a);
          db.prepare("UPDATE recovery_canary_audit SET state='RECOVERY_STOPPED' WHERE event_key=? AND chat_id=? AND state NOT IN ('RECOVERY_CANCELLED_BEFORE_SEND','DELIVERED')").run(a.event_key,a.chat_id);
        }
        return;
      }
      db.prepare(`UPDATE deliveries SET state='RECOVERY_STOPPED',next_at_ms=0 WHERE
        EXISTS(SELECT 1 FROM recovery_canary_audit a WHERE a.event_key=deliveries.event_key AND a.chat_id=deliveries.chat_id
          AND a.state!='RECOVERY_CANCELLED_BEFORE_SEND')
        AND state IN ('PENDING','FAILED_RETRYABLE')`).run();
      db.prepare("UPDATE recovery_canary_audit SET state='RECOVERY_STOPPED' WHERE state='PENDING' AND send_started_ms IS NULL").run();
    }).immediate();
  }
  function allowed() {
    if (!validConfig() || !config.start_ms || fs.existsSync(stopFile)) return false;
    if (mono()-lease>45000) {stop('MAC_HEARTBEAT_EXPIRED');return false;}
    if (!enabledDirect()) {stop('FAST_DIRECT_NOT_ENABLED');return false;}
    if(actionMode&&scope().length!==config.expected_subscriptions){stop('CANARY_SCOPE_CHANGED');return false;}
    return true;
  }
  function present(key, chat, excludeOwn=false) {
    if(actionMode){
      const own=excludeOwn?audit(key,chat):null;
      const action=own?.action_key??key,claim=claimFor(action,chat);
      if(claim&&(!own||claim.first_event_key!==own.event_key))return true;
      if(!excludeOwn&&claim)return true;
      const fill=own?.event_key??key;
      return !!((!own&&db.prepare('SELECT 1 FROM deliveries WHERE event_key=? AND chat_id=?').get(fill,chat))||own?.late_fast_ms);
    }
    const r=db.prepare('SELECT state FROM deliveries WHERE event_key=? AND chat_id=?').get(key,chat);
    const a=excludeOwn?audit(key,chat):null;
    return !!((r && !a) || (!excludeOwn && r) || a?.late_fast_ms ||
      db.prepare('SELECT 1 FROM rtds_metadata_audit WHERE event_key=?').get(key));
  }
  function stats() {
    const rows=db.prepare('SELECT * FROM recovery_canary_audit WHERE rest_first_seen_ms>=?').all(config?.start_ms??Number.MAX_SAFE_INTEGER);
    const latency=rows.filter(r=>r.delivered_at_ms).map(r=>r.delivered_at_ms-r.rest_first_seen_ms).sort((a,b)=>a-b);
    const q=p=>latency.length?latency[Math.ceil(latency.length*p)-1]:null;
    const e2e=rows.filter(r=>r.delivered_at_ms).map(r=>r.delivered_at_ms-JSON.parse(r.trade_json).timestamp*1000).sort((a,b)=>a-b);
    const qe=p=>e2e.length?e2e[Math.ceil(e2e.length*p)-1]:null;
    return {start_ms:config?.start_ms??null,protocol_version:config?.version??null,run_id:config?.run_id??null,recipients_count:actionMode?new Set(config.subscriptions.map(s=>s.recipient)).size:config?.recipients?.length??0,wallets_count:actionMode?new Set(config.subscriptions.map(s=>s.wallet)).size:config?.wallets?.length??0,
      enabled:validConfig()&&!!config.start_ms&&!fs.existsSync(stopFile)&&mono()-lease<=45000,
      stopped:fs.existsSync(stopFile),stop_reason:fs.existsSync(stopFile)?JSON.parse(fs.readFileSync(stopFile,'utf8')).reason:null,
      late_fast_after_recovery_ack_suppressed:rows.filter(r=>r.delivered_at_ms&&Number(r.late_fast_suppressed_ms)>=r.delivered_at_ms).length,
      late_fast_suppressed:rows.filter(r=>r.late_fast_suppressed_ms).length,
      recovery_candidates:rows.length,cancelled_final_check:rows.filter(r=>r.state==='RECOVERY_CANCELLED_BEFORE_SEND').length,
      recovery_sends:rows.filter(r=>r.send_started_ms).length,send_attempts:rows.reduce((s,r)=>s+r.attempt_count,0),
      successful_deliveries:rows.filter(r=>r.delivered_at_ms).length,
      telegram_failures:rows.filter(r=>r.state==='FAILED_FINAL').length,
      telegram_failed_attempts:rows.reduce((n,r)=>n+JSON.parse(r.response_json||'[]').filter(x=>x.kind!=='OK').length,0),
      delivery_unknown:rows.filter(r=>r.state==='DELIVERY_UNKNOWN').length,
      confirmed_duplicates:Math.max(rows.filter(r=>r.state==='CONFIRMED_DUPLICATE').length,actionMode?getSendOnce().snapshot().duplicate_telegram_alerts:0),
      wrong_attribution:rows.filter(r=>r.state==='WRONG_ATTRIBUTION').length,
      late_fast_after_recovery_send:rows.filter(r=>r.late_fast_ms&&r.send_started_ms&&r.late_fast_ms>=r.send_started_ms).length,
      reconnect_origin:rows.filter(r=>r.send_started_ms&&r.origin==='reconnect').length,
      periodic_origin:rows.filter(r=>r.send_started_ms&&r.origin==='periodic').length,
      rest_to_delivery_ms:{median:q(.5),p90:q(.9),max:latency.at(-1)??null},
      recovery_end_to_end_ms:{median:qe(.5),p90:qe(.9),max:e2e.at(-1)??null}};
  }
  function call(req) {
    if (typeof req.nonce!=='string'||req.nonce.length>64) throw Error('BAD_NONCE');
    if(req.op==='meta') return {nonce:req.nonce,metrics:stats(),scope:scope().map(r=>({wallet:r.wallet,recipient:createHmac('sha256',req.salt).update(String(r.chat_id)).digest('hex').slice(0,24)}))};
    if(req.op==='activate') {
      if(actionMode){
        if(!validConfig()||config.start_ms||req.run_id!==config.run_id||!enabledDirect()||scope().length!==config.expected_subscriptions)throw Error('ACTIVATION_REJECTED');
        if(!fs.existsSync(stopFile)||JSON.parse(fs.readFileSync(stopFile,'utf8')).reason!=='PREPARED_NEW_ACTION_CANARY')throw Error('NEW_RUN_MARKER_REQUIRED');
        config.start_ms=now();lease=mono();fs.writeFileSync(configFile+'.tmp',JSON.stringify(config)+'\n',{mode:0o600});fs.renameSync(configFile+'.tmp',configFile);fs.unlinkSync(stopFile);
        return {nonce:req.nonce,metrics:stats()};
      }

      if(!validConfig()||config.start_ms||fs.existsSync(stopFile)||!enabledDirect()) throw Error('ACTIVATION_REJECTED');
      if(scope().length!==config.expected_subscriptions) throw Error('SCOPE_CHANGED');
      config.start_ms=now();lease=mono();fs.writeFileSync(configFile+'.tmp',JSON.stringify(config)+'\n',{mode:0o600});fs.renameSync(configFile+'.tmp',configFile);
      return {nonce:req.nonce,metrics:stats()};
    }
    if(req.op==='stop') {stop(req.reason||'MANUAL_STOP');return {nonce:req.nonce,metrics:stats()};}
    if(req.op==='heartbeat') {lease=mono();return {nonce:req.nonce,metrics:stats()};}
    if(actionMode&&req.op==='live'){
      if(!Array.isArray(req.entries)||req.entries.length>200)throw Error('BAD_ENTRIES');
      const active=scope();
      return {nonce:req.nonce,results:req.entries.map(e=>{
        const rows=active.filter(s=>s.wallet.toLowerCase()===e.wallet&&e.recipients.includes(createHmac('sha256',req.salt).update(String(s.chat_id)).digest('hex').slice(0,24))&&e.timestamp>=s.notify_from_ts);
        const delivery_rows=rows.flatMap(s=>{const cl=claimFor(e.key,s.chat_id);if(!cl)return [];const d=db.prepare('SELECT state,source_timestamp,first_seen_at_ms,delivered_at_ms FROM deliveries WHERE event_key=? AND chat_id=?').get(cl.first_event_key,s.chat_id);return d?[{...d,recipient:createHmac('sha256',req.salt).update(String(s.chat_id)).digest('hex').slice(0,24),source:actionAudit(e.key,s.chat_id)?.event_key===cl.first_event_key&&actionAudit(e.key,s.chat_id)?.send_started_ms?'REST_RECOVERY':'FAST_DIRECT'}]:[{state:'ACTION_RESERVED_WITHOUT_DELIVERY'}];});
        return {key:e.key,eligible:rows.length===e.recipients.length&&rows.length>0,present:delivery_rows.length>0,delivery_rows};
      })};
    }
    if(req.op==='followup') {
      if(!Array.isArray(req.keys)||req.keys.length>200)throw Error('BAD_KEYS');
      return {nonce:req.nonce,metrics:stats(),results:req.keys.map(key=>({key,rows:(actionMode?[...new Set(config.subscriptions.map(s=>s.recipient))].map(chat=>actionAudit(key,chat)):config.recipients.map(chat=>audit(key,chat))).filter(Boolean).map(r=>{
        const {chat_id,trade_json,...safe}=r;return safe;
      })}))};
    }
    if(req.op!=='enqueue'||!allowed()) throw Error('RECOVERY_NOT_ACTIVE');
    const t=canonicalTrade(req.raw,req.wallet,config.start_ms,now());
    if(actionMode)return enqueueAction(req,t);

    if(!t||req.raw.type!=='TRADE'||eventKey(t)!==req.key||!config.wallets.includes(t.wallet)) {stop('ATTRIBUTION_OR_CANONICAL_INVALID');throw Error('INVALID_TRADE');}
    if(!Number.isFinite(req.deadline_state_age_ms)||req.deadline_state_age_ms>2000||req.deadline_state_age_ms<0||
      !Number.isFinite(req.grace_elapsed_ms)||req.grace_elapsed_ms<20000||!['reconnect','periodic'].includes(req.origin)||
      !Number.isFinite(req.rest_first_seen_ms)||req.rest_first_seen_ms<config.start_ms||req.rest_first_seen_ms>now()+5000) throw Error('INVALID_DEADLINE');
    const checkStart=mono();
    return db.transaction(()=>{
      const selected=scope().filter(r=>r.wallet.toLowerCase()===t.wallet&&t.timestamp>=r.notify_from_ts);
      if(selected.length!==1)throw Error('CANARY_SCOPE_CHANGED');
      const recipient=selected[0];
      if(present(req.key,recipient.chat_id))return {nonce:req.nonce,key:req.key,status:'RECOVERY_CANCELLED_BEFORE_SEND'};
      const elapsed=mono()-checkStart;if(elapsed>2000)throw Error('FINAL_STATE_TOO_STALE');
      db.prepare(`INSERT INTO recovery_canary_audit(event_key,chat_id,trade_json,rest_first_seen_ms,grace_deadline_ms,
        deadline_check_ms,deadline_state_age_ms,final_check_ms,final_state_age_ms,state,origin)
        VALUES(?,?,?,?,?,?,?,?,?,'PENDING',?)`).run(req.key,recipient.chat_id,JSON.stringify(t),req.rest_first_seen_ms,
          req.rest_first_seen_ms+20000,req.deadline_check_ms,req.deadline_state_age_ms,now(),elapsed,req.origin);
      db.prepare(`INSERT INTO deliveries(event_key,chat_id,wallet,source_timestamp,first_seen_at_ms,message_html,state,next_at_ms)
        VALUES(?,?,?,?,?,?,'PENDING',?)`).run(req.key,recipient.chat_id,t.wallet,t.timestamp,req.rest_first_seen_ms,
          formatAlert(t,recipient),now());
      if(mono()-checkStart>2000)throw Error('FINAL_STATE_TOO_STALE');
      return {nonce:req.nonce,key:req.key,status:'RECOVERY_QUEUED'};
    }).immediate();
  }
  function enqueueAction(req,t){
    if(!t||req.raw.type!=='TRADE'||userActionKey(t)!==req.key||!config.subscriptions.some(s=>s.wallet===t.wallet)){
      stop('ATTRIBUTION_OR_CANONICAL_INVALID');throw Error('INVALID_TRADE');
    }
    if(!Number.isFinite(req.deadline_state_age_ms)||req.deadline_state_age_ms<0||req.deadline_state_age_ms>2000||
      !Number.isFinite(req.grace_elapsed_ms)||req.grace_elapsed_ms<20000||
      !Number.isFinite(req.rest_first_seen_ms)||req.rest_first_seen_ms<config.start_ms||req.rest_first_seen_ms>now()||
      now()-req.rest_first_seen_ms<20000||!['reconnect','periodic'].includes(req.origin)||
      !Array.isArray(req.recipients)||req.recipients.length===0||new Set(req.recipients).size!==req.recipients.length)throw Error('INVALID_DEADLINE_OR_SCOPE');
    const check=mono();
    const sendOnce=getSendOnce();
    sendOnce.refreshRollback();
    return db.transaction(()=>{
      const selected=scope().filter(s=>s.wallet.toLowerCase()===t.wallet&&t.timestamp>=s.notify_from_ts&&req.recipients.includes(createHmac('sha256',req.salt).update(String(s.chat_id)).digest('hex').slice(0,24)));
      if(selected.length!==req.recipients.length)throw Error('CANARY_SCOPE_CHANGED');
      const results=[];
      for(const recipient of selected){
        const chat=recipient.chat_id;
        const token=createHmac('sha256',req.salt).update(String(chat)).digest('hex').slice(0,24);
        if(present(req.key,chat)){results.push({recipient:token,status:'RECOVERY_CANCELLED_BEFORE_SEND'});continue;}
        const fill=eventKey(t),identity=userActionIdentity(t);
        const existing=db.prepare('SELECT state FROM deliveries WHERE event_key=? AND chat_id=?').get(fill,chat);
        if(existing){results.push({recipient:token,status:'RECOVERY_CANCELLED_BEFORE_SEND'});continue;}
        if(mono()-check>2000)throw Error('FINAL_STATE_TOO_STALE');
        const traceId=traceRecovery?.('begin',req.raw,chat,fill)??('rest-recovery:'+config.run_id+':'+req.key);
        const admitted=sendOnce.admit(t,chat,fill,traceId,formatAlert(t,recipient),now(),req.rest_first_seen_ms);
        if(admitted.kind!=='SEND_FIRST_ACTION'){stop('SHARED_ACTION_ADMISSION_FAILED');throw Error('SHARED_ACTION_ADMISSION_FAILED');}
        const cl=claimFor(req.key,chat);
        if(!cl||cl.identity_json!==identity||cl.first_event_key!==fill)throw Error('SHARED_ACTION_RESERVATION_FAILED');
        db.prepare(`INSERT INTO recovery_canary_audit(event_key,chat_id,trade_json,rest_first_seen_ms,grace_deadline_ms,deadline_check_ms,deadline_state_age_ms,final_check_ms,final_state_age_ms,state,origin,action_key,reservation_trace_id)
          VALUES(?,?,?,?,?,?,?,?,?,'PENDING',?,?,?)`).run(fill,chat,JSON.stringify(t),req.rest_first_seen_ms,req.rest_first_seen_ms+20000,req.deadline_check_ms,req.deadline_state_age_ms,now(),mono()-check,req.origin,req.key,traceId);
        traceRecovery?.('queue',traceId,chat,fill,req.raw);
        results.push({recipient:token,status:'RECOVERY_QUEUED'});
      }
      if(mono()-check>2000)throw Error('FINAL_STATE_TOO_STALE');
      return {nonce:req.nonce,key:req.key,status:results.some(r=>r.status==='RECOVERY_QUEUED')?'RECOVERY_QUEUED':'RECOVERY_CANCELLED_BEFORE_SEND',results};
    }).immediate();
  }
  // Called synchronously inside the same transaction as the existing sender claim.
  function beforeClaim(row) {
    const a=audit(row.event_key,row.chat_id);
    if(!a)return {allow:true,isRest:false};
    if(!a.send_started_ms&&['RECOVERY_CANCELLED_BEFORE_SEND','RECOVERY_STOPPED'].includes(a.state)){
      // An existing audit can describe a recovery row already replaced by FAST.
      const claim=actionMode?claimFor(a.action_key,row.chat_id):null;
      const stillRecovery=actionMode
        ?claim?.first_event_key===a.event_key&&claim.first_trace_id===a.reservation_trace_id
        :row.first_seen_at_ms===a.rest_first_seen_ms;
      if(!stillRecovery)return {allow:true,isRest:false};
      db.prepare("UPDATE deliveries SET state=?,next_at_ms=0 WHERE event_key=? AND chat_id=? AND state IN ('PENDING','FAILED_RETRYABLE')").run(a.state,row.event_key,row.chat_id);
      return {allow:false,isRest:true};
    }
    if(!allowed()){
      db.prepare("UPDATE deliveries SET state='RECOVERY_STOPPED',next_at_ms=0 WHERE event_key=? AND chat_id=? AND state IN ('PENDING','FAILED_RETRYABLE')").run(row.event_key,row.chat_id);
      db.prepare("UPDATE recovery_canary_audit SET state='RECOVERY_STOPPED' WHERE event_key=? AND chat_id=? AND send_started_ms IS NULL").run(row.event_key,row.chat_id);
      return {allow:false,isRest:true};
    }
    const started=mono(), s=scope().find(r=>r.chat_id===row.chat_id&&r.wallet.toLowerCase()===row.wallet&&row.source_timestamp>=r.notify_from_ts);
    if(!s||present(row.event_key,row.chat_id,true)) {
      if(actionMode)releaseUnsent(a);
      db.prepare("UPDATE recovery_canary_audit SET state='RECOVERY_CANCELLED_BEFORE_SEND',final_check_ms=? WHERE event_key=? AND chat_id=?").run(now(),row.event_key,row.chat_id);
      db.prepare("UPDATE deliveries SET state='RECOVERY_CANCELLED_BEFORE_SEND',next_at_ms=0 WHERE event_key=? AND chat_id=?").run(row.event_key,row.chat_id);
      return {allow:false,isRest:true};
    }
    const age=mono()-started;if(age>2000){stop('FINAL_STATE_TOO_STALE');return {allow:false,isRest:true};}
    db.prepare(`UPDATE recovery_canary_audit SET final_check_ms=?,final_state_age_ms=?,
      state='CLAIMED' WHERE event_key=? AND chat_id=?`).run(now(),age,row.event_key,row.chat_id);
    return {allow:true,isRest:true};
  }
  function beforeSend(row) {
    if(!row.isRestRecovery)return true;
    const start=mono();
    if(actionMode){
      const a=audit(row.event_key,row.chat_id),cl=a&&claimFor(a.action_key,row.chat_id);
      const t=a&&JSON.parse(a.trade_json);
      if(!a||!cl||cl.first_event_key!==row.event_key||cl.first_trace_id!==a.reservation_trace_id||cl.identity_json!==userActionIdentity(t)||row.wallet!==t.wallet||row.source_timestamp!==t.timestamp){stop('WRONG_RECOVERY_ACTION');return false;}
    }
    const selected=scope().some(s=>s.chat_id===row.chat_id&&s.wallet.toLowerCase()===row.wallet&&row.source_timestamp>=s.notify_from_ts);
    if(!allowed()||!selected||present(row.event_key,row.chat_id,true))return false;
    const age=mono()-start;
    if(age>2000)return false;
    row.recoveryFinalCheck={at:now(),age};
    return true;
  }
  function markSendStarted(row) {
    if(!row.isRestRecovery)return;
    const {at,age}=row.recoveryFinalCheck;
    db.prepare(`UPDATE recovery_canary_audit SET final_check_ms=?,final_state_age_ms=?,send_started_ms=COALESCE(send_started_ms,?),
      attempt_count=attempt_count+1,state='SENDING' WHERE event_key=? AND chat_id=?`).run(at,age,now(),row.event_key,row.chat_id);
  }
  function onDirect(key,received,trade) {
    if(!config)return;
    if(actionMode){
      if(!trade)return;
      const action=userActionKey(trade);
      db.transaction(()=>{
        for(const a of db.prepare('SELECT * FROM recovery_canary_audit WHERE action_key=? AND rest_first_seen_ms>=?').all(action,config.start_ms??Number.MAX_SAFE_INTEGER)){
          db.prepare('UPDATE recovery_canary_audit SET late_fast_ms=COALESCE(late_fast_ms,?) WHERE event_key=? AND chat_id=?').run(received,a.event_key,a.chat_id);
          if(!a.send_started_ms){releaseUnsent(a);db.prepare("UPDATE recovery_canary_audit SET state='RECOVERY_CANCELLED_BEFORE_SEND' WHERE event_key=? AND chat_id=?").run(a.event_key,a.chat_id);}
        }
      }).immediate();return;
    }
    db.transaction(()=>{
      for(const a of db.prepare('SELECT * FROM recovery_canary_audit WHERE event_key=?').all(key)) {
        db.prepare('UPDATE recovery_canary_audit SET late_fast_ms=COALESCE(late_fast_ms,?) WHERE event_key=? AND chat_id=?').run(received,key,a.chat_id);
        if(!a.send_started_ms) {
          db.prepare("DELETE FROM deliveries WHERE event_key=? AND chat_id=? AND state='PENDING'").run(key,a.chat_id);
          db.prepare("UPDATE recovery_canary_audit SET state='RECOVERY_CANCELLED_BEFORE_SEND' WHERE event_key=? AND chat_id=?").run(key,a.chat_id);
        }
        // A claimed or ambiguous delivery keeps its canonical reservation.
      }
    }).immediate();
  }
  function afterSend(row,result,state,delivered) {
    if(!row.isRestRecovery)return;
    const a=audit(row.event_key,row.chat_id);if(!a) {stop('CANONICAL_AUDIT_MISSING');return;}
    if(actionMode&&state==='DELIVERED'&&!(result.kind==='OK'&&result.status===200&&result.telegram_ok===true&&Number.isSafeInteger(result.message_id)&&result.message_id>0&&result.recipient_matches===true&&result.delivery_row_updated===true)){
      stop('INVALID_RECOVERY_ACK');state='DELIVERY_UNKNOWN';delivered=null;
    }
    const responses=JSON.parse(a.response_json||'[]');responses.push({at_ms:now(),...result});
    db.prepare('UPDATE recovery_canary_audit SET response_json=?,state=?,delivered_at_ms=? WHERE event_key=? AND chat_id=?')
      .run(JSON.stringify(responses),state,delivered,row.event_key,row.chat_id);
    const ids=new Set(responses.filter(r=>r.kind==='OK'&&r.message_id).map(r=>r.message_id));
    if(ids.size>1) {
      db.prepare("UPDATE recovery_canary_audit SET state='CONFIRMED_DUPLICATE' WHERE event_key=? AND chat_id=?").run(row.event_key,row.chat_id);
      stop('CONFIRMED_DUPLICATE');
    }
    if(result.recipient_matches===false) {
      db.prepare("UPDATE recovery_canary_audit SET state='WRONG_ATTRIBUTION' WHERE event_key=? AND chat_id=?").run(row.event_key,row.chat_id);
      stop('WRONG_ATTRIBUTION');
    }
    if(actionMode&&state!=='DELIVERED')stop(state==='DELIVERY_UNKNOWN'?'RECOVERY_DELIVERY_UNKNOWN':'RECOVERY_TELEGRAM_ERROR');
    if(state==='DELIVERY_UNKNOWN'&&responses.filter(r=>r.kind==='AMBIGUOUS').length>=2)stop('RECOVERY_AMBIGUOUS_RETRY_EXHAUSTED');
  }
  function onSuppressed(trade,chat,admission){
    if(!actionMode||admission.kind!=='SUPPRESS_SAME_ACTION')return;
    const a=actionAudit(userActionKey(trade),chat);
    if(a?.send_started_ms&&a.event_key===admission.firstEventKey)db.prepare('UPDATE recovery_canary_audit SET late_fast_suppressed_ms=? WHERE event_key=? AND chat_id=?').run(now(),a.event_key,chat);
  }
  let server=null;
  function listen() {
    if(!validConfig())return;
    const socket=root+'/recovery.sock';if(fs.existsSync(socket))fs.unlinkSync(socket);
    server=net.createServer(client=>{
      let data='',done=false;client.setTimeout(5000,()=>client.destroy());
      client.on('data',chunk=>{
        if(done)return;
        data+=chunk.toString();if(Buffer.byteLength(data)>262144){client.destroy();return;}
        if(!data.includes('\n'))return;
        done=true;
        try {const result=call(JSON.parse(data.split('\n')[0]));client.end(JSON.stringify(result)+'\n');}
        catch(e) {client.end(JSON.stringify({error:e.message})+'\n');}
      });
      client.on('error',()=>{});
    });server.listen(socket,()=>fs.chmodSync(socket,0o600));
  }
  function observeSafely(fn,...args){try{return fn(...args);}catch{try{stop('RECOVERY_OBSERVATION_ERROR');}catch{}}}
  return {call,listen,close:()=>server?.close(),beforeClaim,beforeSend,markSendStarted,onDirect,onDirectAction:actionMode?(trade,received)=>observeSafely(onDirect,eventKey(trade),received,trade):undefined,onSuppressed:(...args)=>observeSafely(onSuppressed,...args),afterSend,stop,stats};
}
