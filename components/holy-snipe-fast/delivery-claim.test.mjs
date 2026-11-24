import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import Database from 'better-sqlite3';
import {createDeliveryClaimGuard,createSubscriptionCancellation} from './delivery-claim.mjs';
import {createSendOnce} from './send-once.mjs';
import {recoveryController} from './recovery.mjs';
import {eventKey,userActionKey} from './identity.mjs';

const wallet='0x'+'1'.repeat(40);
const recipient=42;
function fixture(t,{onDisk=false,version=2}={}) {
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'fast-claim-'));
  const db=new Database(onDisk?path.join(root,'state.db'):':memory:',{timeout:0});
  t.after(()=>{db.close();fs.rmSync(root,{recursive:true,force:true});});
  db.exec(`CREATE TABLE deliveries(
    event_key TEXT,chat_id INTEGER,wallet TEXT,source_timestamp INTEGER,first_seen_at_ms INTEGER,
    message_html TEXT,state TEXT DEFAULT 'PENDING',next_at_ms INTEGER,delivered_at_ms INTEGER,
    claim_token TEXT,attempts INTEGER DEFAULT 0,ambiguous_retries INTEGER DEFAULT 0,
    possible_duplicate INTEGER DEFAULT 0,send_started_at_ms INTEGER,last_error TEXT,
    PRIMARY KEY(event_key,chat_id));
    CREATE TABLE subscriptions(proxy_address TEXT,telegram_chat_id INTEGER,subscriber_id INTEGER,notify_from_ts INTEGER);
    CREATE TABLE subscribers(id INTEGER,telegram_user_id INTEGER,telegram_chat_id INTEGER,chat_type TEXT);
    CREATE TABLE tracked_accounts(proxy_address TEXT,name TEXT,pseudonym TEXT,profile_slug TEXT);
    CREATE TABLE access_grants(telegram_user_id INTEGER);
    CREATE TABLE rtds_metadata_audit(event_key TEXT PRIMARY KEY);`);
  db.prepare('INSERT INTO subscriptions VALUES(?,?,?,0)').run(wallet,recipient,1);
  db.prepare("INSERT INTO subscribers VALUES(1,?,?,'private')").run(recipient,recipient);
  db.prepare('INSERT INTO tracked_accounts(proxy_address) VALUES(?)').run(wallet);
  const config={version:2,start_ms:1,subscriptions:[{wallet,recipient}]};
  fs.writeFileSync(path.join(root,'send-once-canary.json'),JSON.stringify(config));
  const recoveryConfig=version===2
    ? {...config,run_id:'fixture',expected_subscriptions:1}
    : {version:1,start_ms:1,wallets:[wallet],recipients:[recipient]};
  fs.writeFileSync(path.join(root,'recovery-canary.json'),JSON.stringify(recoveryConfig));
  const insert=db.prepare(`INSERT INTO deliveries
    (event_key,chat_id,wallet,source_timestamp,first_seen_at_ms,message_html,next_at_ms)
    VALUES(?,?,?,?,?,?,?)`);
  const sendOnce=createSendOnce(db,{root,ownerIds:[recipient],recipientMap:new Map([[wallet,[{chat_id:recipient}]]]),insert});
  const recovery=recoveryController(db,{root,ownerIds:[recipient],enabledDirect:()=>true,getSendOnce:()=>sendOnce,mono:()=>0,now:()=>4_000});
  recovery.call({op:'heartbeat',nonce:'fixture'});
  const lost=[];
  const guard=createDeliveryClaimGuard(db,{onClaimLost:row=>lost.push(row.claimToken),now:()=>4_000});

  function seed(n) {
    const trade={wallet,hash:'0x'+String(n).padStart(64,'0'),asset:'asset',side:'BUY',size:'5',price:.5,timestamp:2};
    const key=eventKey(trade),actionKey=userActionKey(trade);
    const traceId='recovery-'+n;
    assert.equal(sendOnce.admit(trade,recipient,key,traceId,'card',1,3_000).kind,'SEND_FIRST_ACTION');
    db.prepare(`INSERT INTO recovery_canary_audit
      (event_key,chat_id,trade_json,rest_first_seen_ms,grace_deadline_ms,deadline_check_ms,deadline_state_age_ms,state,origin)
      VALUES(?,?,?,3000,23000,23000,1,'PENDING','reconnect')`).run(key,recipient,JSON.stringify(trade));
    if(version===2)db.prepare('UPDATE recovery_canary_audit SET action_key=?,reservation_trace_id=? WHERE event_key=?').run(actionKey,traceId,key);
    return {key,trade};
  }
  const delivery=key=>db.prepare('SELECT * FROM deliveries WHERE event_key=?').get(key);
  const audit=key=>db.prepare('SELECT * FROM recovery_canary_audit WHERE event_key=?').get(key);
  function claim(key) {
    const row=delivery(key);
    db.prepare("UPDATE deliveries SET state='SENDING',claim_token='owned',attempts=attempts+1,send_started_at_ms=4000 WHERE event_key=?").run(key);
    db.prepare("UPDATE recovery_canary_audit SET state='CLAIMED' WHERE event_key=?").run(key);
    return {...row,claimToken:'owned',isRestRecovery:true};
  }
  return {db,recovery,guard,lost,seed,claim,delivery,audit};
}

const forbidden=()=>assert.fail('transport was called after a pre-send refusal');

test('recovery.beforeSend denial returns the exact owned claim to PENDING without recording a send',async t=>{
  const f=fixture(t), {key,trade}=f.seed(1);
  const row=f.claim(key);
  f.recovery.onDirect(key,4_000,trade);
  await f.guard.run(row,current=>{
    assert.equal(f.recovery.beforeSend(current),false);
    return 'RECOVERY_BEFORE_SEND_REJECT';
  },forbidden,forbidden);
  assert.equal(f.delivery(key).state,'PENDING');
  assert.equal(f.delivery(key).claim_token,null);
  assert.equal(f.delivery(key).attempts,0);
  assert.equal(f.audit(key).send_started_ms,null);
  const next=f.db.transaction(()=>f.recovery.beforeClaim(f.delivery(key))).immediate();
  assert.deepEqual(next,{allow:false,isRest:true});
  assert.equal(f.delivery(key).state,'RECOVERY_CANCELLED_BEFORE_SEND');
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM send_once_actions').get().n,1);
});

test('recovery stop releases only unclaimed PENDING actions and leaves the active finish intact', t=>{
  const f=fixture(t), pending=f.seed(1), inFlight=f.seed(2);
  f.claim(inFlight.key);
  f.recovery.stop('FIXTURE_STOP');
  assert.equal(f.delivery(pending.key),undefined);
  assert.equal(f.delivery(inFlight.key).state,'SENDING');
  assert.equal(f.delivery(inFlight.key).claim_token,'owned');
  assert.deepEqual(f.db.prepare('SELECT first_event_key FROM send_once_actions').pluck().all(),[inFlight.key]);
  assert.equal(f.db.prepare("UPDATE deliveries SET state='DELIVERED',claim_token=NULL WHERE event_key=? AND state='SENDING' AND claim_token='owned'").run(inFlight.key).changes,1);
});

test('legacy recovery stop rolls back both SQLite updates when the audit update fails', t=>{
  const f=fixture(t,{version:1}), {key}=f.seed(1);
  f.db.exec("CREATE TRIGGER fail_audit BEFORE UPDATE ON recovery_canary_audit BEGIN SELECT RAISE(ABORT,'fixture'); END");
  assert.throws(()=>f.recovery.stop('FIXTURE_STOP'),/fixture/);
  assert.equal(f.delivery(key).state,'PENDING');
  assert.equal(f.audit(key).state,'PENDING');
});

test('a stale pre-request check restores the row without spending another Telegram attempt',async t=>{
  const f=fixture(t), {key}=f.seed(1);
  await assert.rejects(f.guard.run(f.claim(key),()=>{throw Error('STATE_TOO_STALE');},forbidden,forbidden),/STATE_TOO_STALE/);
  assert.equal(f.delivery(key).state,'PENDING');
  assert.equal(f.delivery(key).attempts,0);
  assert.equal(f.delivery(key).send_started_at_ms,null);
  assert.equal(f.delivery(key).claim_token,null);
});

test('an exception after the request starts marks the row UNKNOWN and keeps its reservation',async t=>{
  const f=fixture(t), {key}=f.seed(1);
  await assert.rejects(f.guard.run(f.claim(key),()=>undefined,async()=>{throw Error('fixture transport lost');},forbidden),/fixture transport lost/);
  assert.equal(f.delivery(key).state,'DELIVERY_UNKNOWN');
  assert.equal(f.delivery(key).possible_duplicate,1);
  assert.equal(f.delivery(key).attempts,1);
  assert.equal(f.delivery(key).claim_token,null);
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM send_once_actions').get().n,1);
});

test('a committed acknowledgement survives a later logging exception',async t=>{
  const f=fixture(t), {key}=f.seed(1);
  await assert.rejects(f.guard.run(f.claim(key),()=>undefined,async()=>({kind:'OK'}),(result,settled)=>{
    assert.equal(result.kind,'OK');
    f.db.prepare("UPDATE deliveries SET state='DELIVERED',claim_token=NULL WHERE event_key=?").run(key);
    settled();
    throw Error('fixture log failed');
  }),/fixture log failed/);
  assert.equal(f.delivery(key).state,'DELIVERED');
});

test('returning a claim does not overwrite another token',async t=>{
  const f=fixture(t), {key}=f.seed(1);
  const row=f.claim(key);
  f.db.prepare("UPDATE deliveries SET claim_token='new-owner' WHERE event_key=?").run(key);
  await f.guard.run(row,()=> 'FAST_DISABLED_BEFORE_SEND',forbidden,forbidden);
  assert.equal(f.delivery(key).claim_token,'new-owner');
  assert.deepEqual(f.lost,['owned']);
});

test('a temporary writer lock retries only the settlement before any new network work',async t=>{
  const f=fixture(t,{onDisk:true}), {key}=f.seed(1);
  const row=f.claim(key);
  const competing=new Database(f.db.name);
  try {
    competing.exec('BEGIN IMMEDIATE');
    await f.guard.run(row,()=> 'FAST_DISABLED_BEFORE_SEND',forbidden,forbidden);
    assert.equal(f.guard.flush(),false);
    assert.equal(f.delivery(key).state,'SENDING');
    competing.exec('ROLLBACK');
    assert.equal(f.guard.flush(),true);
    assert.equal(f.delivery(key).state,'PENDING');
    assert.equal(f.delivery(key).claim_token,null);
  } finally {
    competing.close();
  }
});

for (const caseData of [
  {messageId: 0, matches: true, state: 'DELIVERY_UNKNOWN', audit: 'DELIVERY_UNKNOWN', reason: 'INVALID_RECOVERY_ACK'},
  {messageId: 1, matches: false, state: 'DELIVERY_UNKNOWN', audit: 'WRONG_ATTRIBUTION', reason: 'WRONG_ATTRIBUTION'},
  {messageId: 1, matches: true, state: 'DELIVERED', audit: 'DELIVERED', reason: null}
]) {
  test('recovery persists and returns the validated send state: ' + caseData.audit, t => {
    const f = fixture(t);
    const {key} = f.seed(1);
    const row = f.claim(key);
    assert.equal(f.recovery.beforeSend(row), true);
    f.recovery.markSendStarted(row);
    const finalize = f.db.prepare(
      "UPDATE deliveries SET state=?,claim_token=NULL,possible_duplicate=?,last_error=?,delivered_at_ms=? " +
      "WHERE event_key=? AND chat_id=? AND state='SENDING' AND claim_token=?"
    );
    const result = {
      kind: 'OK', status: 200, telegram_ok: true,
      message_id: caseData.messageId, recipient_matches: caseData.matches
    };
    const state = f.recovery.onResult(row, result,
      {state:'DELIVERED', next:0, error:null, possible:0, delivered:4100},
      decision => finalize.run(decision.state, decision.possible, decision.error, decision.delivered,
        row.event_key, row.chat_id, row.claimToken).changes === 1);
    assert.equal(state, caseData.state);
    assert.equal(f.delivery(key).state, state);
    assert.equal(f.delivery(key).claim_token, null);
    assert.equal(f.delivery(key).last_error, caseData.reason);
    assert.equal(f.delivery(key).possible_duplicate, state === 'DELIVERY_UNKNOWN' ? 1 : 0);
    assert.equal(f.audit(key).state, caseData.audit);
    assert.equal(f.audit(key).delivered_at_ms, state === 'DELIVERED' ? 4100 : null);
    assert.equal(f.recovery.stats().stop_reason, caseData.reason);
  });
}

test('recovery observation failures and failed safety-stop writes both surface safe diagnostics', t => {
  const f = fixture(t);
  const {trade, key} = f.seed(1);
  f.db.exec("CREATE TRIGGER fail_observation BEFORE UPDATE ON recovery_canary_audit " +
    "BEGIN SELECT RAISE(ABORT,'fixture private diagnostic'); END");
  const log = t.mock.method(console, 'error', () => {});
  assert.doesNotThrow(() => f.recovery.onDirectAction(trade, 4000));
  assert.deepEqual(log.mock.calls.map(call => call.arguments[0]), [
    '[recovery] RECOVERY_OBSERVATION_ERROR', '[recovery] RECOVERY_OBSERVATION_STOP_ERROR'
  ]);
  assert.equal(f.recovery.stats().stop_reason, 'RECOVERY_OBSERVATION_ERROR');
  assert.equal(f.delivery(key).state, 'PENDING');
});

test('FAST takes the writer lock before selecting the rows to cancel and preserves active sends', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'fast-cancel-'));
  const filename = path.join(root, 'state.db');
  const db = new Database(filename, {timeout:0});
  db.pragma('journal_mode = WAL');
  const competing = new Database(filename, {timeout:0});
  t.after(() => {
    competing.close();
    db.close();
    fs.rmSync(root, {recursive:true, force:true});
  });
  let verifyLock = false;
  let lockedReads = 0;
  db.function('probe_writer_lock', {deterministic:true}, () => {
    if (verifyLock) {
      assert.equal(db.inTransaction, true);
      assert.throws(() => competing.exec('UPDATE probe SET touched=1'), {code:'SQLITE_BUSY'});
      lockedReads++;
    }
    return 2;
  });
  db.exec("CREATE TABLE probe(touched INTEGER); INSERT INTO probe VALUES(0); " +
    "CREATE TABLE deliveries(event_key TEXT, wallet TEXT, chat_id INTEGER, state TEXT, " +
    "next_at_ms INTEGER, last_error TEXT, attempts INTEGER AS (probe_writer_lock()) VIRTUAL)");
  const insert = db.prepare('INSERT INTO deliveries(event_key,wallet,chat_id,state) VALUES(?,?,?,?)');
  insert.run('pending',wallet,recipient,'PENDING');
  insert.run('inflight',wallet,recipient,'SENDING');
  insert.run('other-chat',wallet,recipient+1,'PENDING');

  const cancel = createSubscriptionCancellation(db);
  verifyLock = true;
  const cancelled = cancel(wallet,recipient);
  verifyLock = false;
  assert.ok(lockedReads > 0);
  assert.deepEqual(cancelled,[{event_key:'pending',chat_id:recipient,attempts:2}]);
  assert.deepEqual(db.prepare('SELECT event_key,state FROM deliveries ORDER BY rowid').all(), [
    {event_key:'pending',state:'CANCELLED_SUBSCRIPTION'},
    {event_key:'inflight',state:'SENDING'},
    {event_key:'other-chat',state:'PENDING'}
  ]);
  assert.equal(competing.prepare('SELECT touched FROM probe').pluck().get(), 0);
});
