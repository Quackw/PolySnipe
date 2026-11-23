import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {createRequire} from 'node:module';
import {createSendOnce} from './send-once.mjs';
import {userActionKey,userActionIdentity,eventKey,canonicalTrade} from './identity.mjs';
import {reconnectBoundary,inReconnectOverlap} from './reconnect-boundary.mjs';

let Database;
try{Database=(await import('node:sqlite')).DatabaseSync;}
catch{Database=createRequire(import.meta.url)('better-sqlite3');}
const wallet=n=>'0x'+n.toString(16).padStart(40,'0');
const hash=n=>'0x'+n.toString(16).padStart(64,'0');
function fixture(expanded=false){
 const root=fs.mkdtempSync(path.join(os.tmpdir(),'send-once-test-'));
 const wallets=Array.from({length:6},(_,i)=>wallet(i+1));
 fs.writeFileSync(path.join(root,'send-once-canary.json'),JSON.stringify(expanded?{version:2,start_ms:1000,subscriptions:wallets.flatMap(wallet=>[42,43].map(recipient=>({wallet,recipient})))}:{version:1,start_ms:1000,recipient:42,wallets}));
 const db=new Database(':memory:');
 if(!db.transaction)db.transaction=fn=>(...args)=>{
  db.exec('BEGIN');try{const result=fn(...args);db.exec('COMMIT');return result;}
  catch(e){db.exec('ROLLBACK');throw e;}
 };
 db.exec(`CREATE TABLE deliveries(event_key TEXT,chat_id INTEGER,wallet TEXT,
   source_timestamp INTEGER,first_seen_at_ms INTEGER,message_html TEXT,
   state TEXT DEFAULT 'PENDING',next_at_ms INTEGER,delivered_at_ms INTEGER,PRIMARY KEY(event_key,chat_id))`);
 const insert=db.prepare(`INSERT OR IGNORE INTO deliveries
   (event_key,chat_id,wallet,source_timestamp,first_seen_at_ms,message_html,next_at_ms)
   VALUES(?,?,?,?,?,?,?)`);
 const recipients=new Map(wallets.map(w=>[w,(expanded?[42,43]:[42]).map(chat_id=>({chat_id,notify_from_ts:0}))]));
 const canary=createSendOnce(db,{root,ownerIds:[42],recipientMap:recipients,insert,now:()=>5000});
 const trade={wallet:wallets[0],hash:hash(1),asset:'token-1',side:'BUY',size:'5',price:.5,timestamp:2};
 const admit=(t,traceId='trace-1')=>canary.admit(t,42,eventKey(t),traceId,'card',1000,1000);
 const count=()=>db.prepare('SELECT COUNT(*) n FROM deliveries').get().n;
 return {root,db,canary,trade,admit,count,close(){db.close();fs.rmSync(root,{recursive:true,force:true});}};
}
test('user-action key excludes fill size but includes wallet, transaction, token and side',()=>{
 const f=fixture();try{
  const t=f.trade;
  assert.equal(userActionKey(t),userActionKey({...t,size:'6',price:.7}));
  for(const other of [{...t,wallet:wallet(2)},{...t,hash:hash(2)},
    {...t,asset:'token-2'},{...t,side:'SELL'}])assert.notEqual(userActionKey(t),userActionKey(other));
  assert.notEqual(userActionIdentity(t),userActionIdentity({...t,side:'SELL'}));
 }finally{f.close();}
});
test('first action inserts one original fill card and later fills suppress atomically',()=>{
 const f=fixture();try{
  assert(f.canary.applies(f.trade.wallet,42));
  assert.equal(f.admit(f.trade).kind,'SEND_FIRST_ACTION');
  const next=f.admit({...f.trade,size:'6',price:.6},'trace-2');
  assert.equal(next.kind,'SUPPRESS_SAME_ACTION');
  assert.equal(next.firstTraceId,'trace-1');
  assert.equal(f.count(),1);
  assert.equal(f.db.prepare('SELECT message_html FROM deliveries').get().message_html,'card');
  const snap=f.canary.snapshot();assert.equal(snap.user_actions_seen,1);
  assert.equal(snap.send_first_action,1);assert.equal(snap.suppress_same_action,1);
 }finally{f.close();}
});
test('accepted reconnect overlap repeats enter the real atomic SEND-ONCE without another queue',()=>{
 const f=fixture();try{
  assert.equal(f.admit(f.trade).kind,'SEND_FIRST_ACTION');
  const payload={proxyWallet:f.trade.wallet,transactionHash:f.trade.hash,asset:f.trade.asset,side:f.trade.side,size:f.trade.size,price:f.trade.price,timestamp:f.trade.timestamp};
  for(const distance of [2,500,1900,2000]){
   assert(inReconnectOverlap(payload.timestamp,2000+distance,1));
   const repeated=canonicalTrade(payload,f.trade.wallet,reconnectBoundary(2000+distance,1),5000);
   assert.ok(repeated);assert.equal(f.admit(repeated,'reconnect-'+distance).kind,'SUPPRESS_SAME_ACTION');
  }
  assert.equal(f.count(),1);assert.equal(f.canary.snapshot().suppress_same_action,4);
 }finally{f.close();}
});
test('different user actions remain separate and recipient scope is frozen',()=>{
 const f=fixture();try{
  assert.equal(f.canary.applies(wallet(9),42),false);
  assert.equal(f.canary.applies(f.trade.wallet,43),false);
  assert.equal(f.admit(f.trade).kind,'SEND_FIRST_ACTION');
  assert.equal(f.admit({...f.trade,side:'SELL'},'trace-2').kind,'SEND_FIRST_ACTION');
  assert.equal(f.count(),2);
 }finally{f.close();}
});
test('rollback returns all actions to original fill dedup, preserving claims',()=>{
 const f=fixture();try{
  assert.equal(f.admit(f.trade).kind,'SEND_FIRST_ACTION');
  f.canary.rollback('TEST');
  assert.equal(f.admit({...f.trade,size:'6'},'trace-2').kind,'LEGACY_AFTER_ROLLBACK');
  assert.equal(f.admit({...f.trade,hash:hash(3)},'trace-3').kind,'LEGACY_AFTER_ROLLBACK');
  assert.equal(f.count(),1);
  assert.equal(f.canary.snapshot().rollback,true);
 }finally{f.close();}
});
test('expanded frozen pairs include non-owner recipient and isolate recipient claims',()=>{
 const f=fixture(true);try{
  assert(f.canary.applies(f.trade.wallet,43));assert(!f.canary.applies(wallet(9),43));
  assert.equal(f.admit(f.trade).kind,'SEND_FIRST_ACTION');
  assert.equal(f.canary.admit(f.trade,43,eventKey(f.trade),'other','card',1000,1000).kind,'SEND_FIRST_ACTION');
  assert.equal(f.count(),2);assert.equal(f.canary.snapshot().scope_subscriptions,12);
 }finally{f.close();}
});
test('second distinct valid ACK stops only send-once; repeated same ACK does not',()=>{
 const f=fixture(true);try{
  f.admit(f.trade);const row={wallet:f.trade.wallet,chat_id:42,event_key:eventKey(f.trade)};
  const ack=id=>({kind:'OK',value:{result:{message_id:id}}});
  f.canary.observeAck(row,ack(1));f.canary.observeAck(row,ack(1));assert(!f.canary.snapshot().rollback);
  f.canary.observeAck(row,ack(2));assert.equal(f.canary.snapshot().duplicate_telegram_alerts,1);
  assert(f.canary.snapshot().rollback);assert(!fs.existsSync(path.join(f.root,'FAST_DISABLED')));
 }finally{f.close();}
});
test('existing delivery without claim is a safety stop, never an untracked first send',()=>{
 const f=fixture();try{
  f.db.prepare(`INSERT INTO deliveries(event_key,chat_id,wallet,source_timestamp,first_seen_at_ms,message_html)
    VALUES(?,?,?,?,?,?)`).run(eventKey(f.trade),42,f.trade.wallet,2,1000,'old');
  assert.equal(f.admit(f.trade).kind,'FIRST_DIRECT_EVENT_DROPPED');
  assert.equal(f.canary.snapshot().first_direct_event_dropped,1);
  assert.equal(f.canary.snapshot().rollback,true);
  assert.equal(f.count(),1);
 }finally{f.close();}
});

test('deleted frozen subscription permits restart while keeping its dedup scope and claims',()=>{
 const f=fixture(true);try{
  f.db.exec('CREATE TABLE subscriptions(proxy_address TEXT,telegram_chat_id INTEGER)');
  f.admit(f.trade);
  const config=JSON.parse(fs.readFileSync(path.join(f.root,'send-once-canary.json'),'utf8'));
  const recipients=new Map();
  for(const s of config.subscriptions){
   if(s.wallet===f.trade.wallet&&s.recipient===42)continue;
   f.db.prepare('INSERT INTO subscriptions VALUES(?,?)').run(s.wallet,s.recipient);
   const rows=recipients.get(s.wallet)||[];rows.push({chat_id:s.recipient,notify_from_ts:0});recipients.set(s.wallet,rows);
  }
  const insert=f.db.prepare('INSERT OR IGNORE INTO deliveries(event_key,chat_id,wallet,source_timestamp,first_seen_at_ms,message_html,next_at_ms) VALUES(?,?,?,?,?,?,?)');
  const restarted=createSendOnce(f.db,{root:f.root,ownerIds:[42],recipientMap:recipients,insert});
  assert(restarted.applies(f.trade.wallet,42));
  assert.equal(restarted.snapshot().scope_subscriptions,12);
  assert.equal(restarted.admit({...f.trade,size:'6'},42,eventKey({...f.trade,size:'6'}),'repeat','card',1000,1000).kind,'SUPPRESS_SAME_ACTION');
  assert.equal(f.count(),1);
  assert.deepEqual(JSON.parse(fs.readFileSync(path.join(f.root,'send-once-canary.json'),'utf8')),config);
 }finally{f.close();}
});

test('stored subscription absent from eligible recipients still fails closed',()=>{
 const f=fixture(true);try{
  f.db.exec('CREATE TABLE subscriptions(proxy_address TEXT,telegram_chat_id INTEGER)');
  f.db.prepare('INSERT INTO subscriptions VALUES(?,?)').run(f.trade.wallet,42);
  assert.throws(()=>createSendOnce(f.db,{root:f.root,ownerIds:[42],recipientMap:new Map(),insert:null}),/send_once_scope_missing/);
  const config=JSON.parse(fs.readFileSync(path.join(f.root,'send-once-canary.json'),'utf8'));
  const recipients=new Map(config.subscriptions.filter(s=>!(s.wallet===f.trade.wallet&&s.recipient===42)).map(s=>[s.wallet,[{chat_id:s.recipient}]]));
  assert.throws(()=>createSendOnce(f.db,{root:f.root,ownerIds:[42],recipientMap:recipients,insert:null}),/send_once_scope_missing/);
 }finally{f.close();}
});

test('all frozen subscriptions missing and duplicate configured pairs remain startup failures',()=>{
 const f=fixture(true);try{
  f.db.exec('CREATE TABLE subscriptions(proxy_address TEXT,telegram_chat_id INTEGER)');
  assert.throws(()=>createSendOnce(f.db,{root:f.root,ownerIds:[42],recipientMap:new Map(),insert:null}),/send_once_scope_missing/);
  const file=path.join(f.root,'send-once-canary.json'),config=JSON.parse(fs.readFileSync(file,'utf8'));
  config.subscriptions.push(config.subscriptions[0]);fs.writeFileSync(file,JSON.stringify(config));
  assert.throws(()=>createSendOnce(f.db,{root:f.root,ownerIds:[42],recipientMap:new Map(),insert:null}),/send_once_scope_missing/);
 }finally{f.close();}
});
