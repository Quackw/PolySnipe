import test from 'node:test';import assert from 'node:assert/strict';
import {createRemoteB} from './b-receiver.mjs';import {safeDiagnostic} from './b-diagnostics.mjs';
const BASE=1800000000000,w='0x'+'1'.repeat(40),other='0x'+'3'.repeat(40);
class Socket{
 constructor(){this.listeners=new Map();this.sent=[];this.bufferedAmount=0;}
 addEventListener(k,fn){if(!this.listeners.has(k))this.listeners.set(k,new Set());this.listeners.get(k).add(fn);}
 removeEventListener(k,fn){this.listeners.get(k)?.delete(fn);}
 emit(k,v={}){for(const fn of [...this.listeners.get(k)??[]])fn(v);}
 open(){this.emit('open');}send(x){this.sent.push(x);}frame(x){this.emit('message',{data:typeof x==='string'?x:JSON.stringify(x)});}close(){this.emit('close',{code:1000,reason:''});}
}
const settle=()=>new Promise(r=>setImmediate(r));
function fixture({outcome={status:'QUEUED_OR_DEDUPLICATED',owner_status:'ACCEPTED',blocked_pairs:false},throws=false,onDiagnostic=null,enqueue=null}={}){
 const clock={m:0,w:BASE},rows=[],sockets=[],forwarded=[];let channel='READY',scopeUpdate;
 const relay={status:()=>({state:channel}),close(){channel='CLOSED';},enqueue:e=>{forwarded.push(e);return enqueue?enqueue(e):Promise.resolve(outcome);}};
 const receiver=createRemoteB({scope:[{wallet:w,recipient:1,notify_from_ts:0}],ssh:{start:({onScope})=>{scopeUpdate=onScope;onScope([w]);return relay;},close(){}},socketFactory:()=>{const s=new Socket();sockets.push(s);return s;},mono:()=>clock.m,wall:()=>clock.w,onDiagnostic:row=>{rows.push(safeDiagnostic(row));if(throws)throw Error('secret observer exception');onDiagnostic?.(row);}});
 return {receiver,scopeUpdate:values=>scopeUpdate(values),rows,sockets,clock,forwarded,async start(){await receiver.start();sockets[0].open();},step(n){clock.m+=n;clock.w+=n;receiver.tick();},lose(){channel='CLOSED';},event(extra={}){return {topic:'activity',type:'trades',payload:{proxyWallet:w,transactionHash:'0x'+'2'.repeat(64),asset:'fixture',side:'BUY',size:1,price:.5,timestamp:Math.floor(clock.w/1000),...extra}};}};
}
test('every valid activity has a correlated receive/drop/forward decision without private payload',async()=>{
 const f=fixture();await f.start();try{
  f.sockets[0].frame(f.event({title:'secret market'}));f.sockets[0].frame(f.event({proxyWallet:other}));f.sockets[0].frame(f.event({size:0}));f.sockets[0].frame(f.event({timestamp:BASE/1000-3}));f.sockets[0].frame(f.event({timestamp:BASE/1000+6}));await settle();
  assert.equal(f.forwarded.length,1);const received=f.rows.find(x=>x.kind==='B_EVENT_RECEIVED'),request=f.rows.find(x=>x.kind==='B_FORWARD_REQUESTED'),result=f.rows.find(x=>x.kind==='B_FORWARD_RESULT');
  assert.ok(received.action_key);assert.ok(received.raw_sha256);assert.equal(received.frame_id,request.frame_id);assert.equal(result.frame_id,request.frame_id);assert.equal(result.owner_status,'ACCEPTED');assert.equal(result.delivery_authorized,false);
  assert.deepEqual(f.rows.filter(x=>x.kind==='B_EVENT_DROPPED').map(x=>x.reason),['OUTSIDE_ACTIVE_SCOPE','INVALID_PAYLOAD','BEFORE_RECONNECT_BOUNDARY','SOURCE_TIMESTAMP_IN_FUTURE']);assert.equal(f.rows.find(x=>x.reason==='INVALID_PAYLOAD').validation_code,'INVALID_SIZE');assert.equal(f.receiver.status().stale,3);
  for(const value of [w,other,'secret market'])assert.ok(!JSON.stringify(f.rows).includes(value));
 }finally{f.receiver.stop();}
});
test('invalid JSON, non-text, oversized and unsubscribed frames leave payload-free rejection evidence',async()=>{
 const f=fixture();await f.start();try{f.sockets[0].frame('secret invalid JSON');f.sockets[0].emit('message',{data:Buffer.from('secret binary')});f.sockets[0].frame('x'.repeat(262145));f.sockets[0].frame({topic:'secret-topic',payload:{secret:'hidden'}});f.sockets[0].frame('');
  const rows=f.rows.filter(x=>x.kind==='B_FRAME_DECISION');assert.deepEqual(rows.map(x=>x.decision),['INVALID_JSON','NON_TEXT','FRAME_BUDGET','UNSUBSCRIBED_TOPIC','EMPTY_FRAME']);assert.ok(rows[0].raw_sha256);assert.equal(rows[2].raw_sha256,null);assert.equal(f.forwarded.length,0);assert.ok(!JSON.stringify(f.rows).includes('secret'));
 }finally{f.receiver.stop();}
});
test('pong is recorded but does not mask an 8s data freeze; reconnect has old/new generation and stale/obsolete rejections',async()=>{
 const f=fixture();await f.start();try{const old=f.sockets[0],late=[...old.listeners.get('message')][0];old.frame(f.event());for(let i=0;i<7;i++){f.step(1000);old.frame('pong');}f.step(1000);
  assert.ok(f.rows.some(x=>x.kind==='B_RTDS_LIFECYCLE'&&x.lifecycle_kind==='DATA_PLANE_STALE'&&x.silence_ms===8000));assert.ok(f.rows.some(x=>x.lifecycle_kind==='SOCKET_CLOSE_REQUEST'&&x.reason==='DATA_PLANE_STALE'));assert.ok(f.rows.some(x=>x.lifecycle_kind==='SOCKET_CLOSE'&&x.initiator==='LOCAL'&&x.local_trigger==='DATA_PLANE_STALE'));
  late({data:JSON.stringify(f.event())});assert.ok(f.rows.some(x=>x.kind==='B_FRAME_DECISION'&&x.decision==='OBSOLETE_CONNECTION_GENERATION'));
  f.step(500);assert.equal(f.sockets.length,2);f.sockets[1].open();f.sockets[1].frame(f.event({timestamp:BASE/1000}));f.sockets[1].frame(f.event());await settle();
  const receives=f.rows.filter(x=>x.kind==='B_EVENT_RECEIVED');assert.deepEqual(receives.map(x=>x.generation),[1,2]);assert.notEqual(receives[0].connection_id,receives[1].connection_id);assert.equal(f.forwarded.length,2);assert.ok(f.rows.some(x=>x.kind==='B_EVENT_DROPPED'&&x.generation===2&&x.reason==='BEFORE_RECONNECT_BOUNDARY'));
 }finally{f.receiver.stop();}
});
test('remote close versus local socket error remains distinct and never exposes a raw close reason',async()=>{
 for(const mode of ['remote','error']){const f=fixture();await f.start();try{if(mode==='remote')f.sockets[0].emit('close',{code:1011,reason:'secret server address'});else f.sockets[0].emit('error',{message:'secret error'});
  assert.ok(f.rows.some(x=>x.lifecycle_kind===(mode==='remote'?'SOCKET_CLOSE':'SOCKET_ERROR')));const closed=f.rows.find(x=>x.lifecycle_kind==='SOCKET_CLOSE');assert.equal(closed.initiator,mode==='remote'?'REMOTE_OR_TRANSPORT_UNKNOWN':'LOCAL');if(mode==='remote')assert.ok(closed.reason_sha256);assert.ok(!JSON.stringify(f.rows).includes('secret'));
 }finally{f.receiver.stop();}}
});
test('scope changes record exact hashed membership/version and explain later scope drops',async()=>{
 const f=fixture();await f.start();try{f.scopeUpdate([other]);f.sockets[0].frame(f.event());f.sockets[0].frame(f.event({proxyWallet:other}));await settle();const scopes=f.rows.filter(x=>x.kind==='B_SCOPE');assert.deepEqual(scopes.map(x=>x.scope_version),[0,1,2]);assert.notDeepEqual(scopes[1].wallet_tags,scopes[2].wallet_tags);assert.equal(f.rows.find(x=>x.kind==='B_EVENT_DROPPED').scope_version,2);assert.equal(f.forwarded.length,1);}finally{f.receiver.stop();}
});
test('ambiguous/duplicate owner decisions are logged as queue results and never Telegram delivery',async()=>{
 const f=fixture({outcome:{status:'PROCESSED_WITH_ATTENTION',owner_status:'DUPLICATE',blocked_pairs:true,attention:true}});await f.start();try{f.sockets[0].frame(f.event());await settle();const r=f.rows.find(x=>x.kind==='B_FORWARD_RESULT');assert.equal(r.owner_status,'DUPLICATE');assert.equal(r.blocked_pairs,true);assert.equal(r.delivery_authorized,false);assert.equal(f.receiver.status().attention,1);assert.equal(f.forwarded.length,1);}finally{f.receiver.stop();}
});
test('queue rejection and channel loss reconnect only the relay and preserve no-replay behavior',async()=>{
 const f=fixture({outcome:{status:'OBSERVATION_GAP',reason:'BRIDGE_ACK_TIMEOUT'}});await f.start();f.sockets[0].frame(f.event());await settle();assert.equal(f.receiver.status().channel,'RECONNECTING');assert.equal(f.rows.find(x=>x.kind==='B_FORWARD_RESULT').reason,'BRIDGE_ACK_TIMEOUT');assert.equal(f.forwarded.length,1);assert.ok(f.rows.some(x=>x.kind==='B_STATE'&&x.reason==='REMOTE_B_CHANNEL_LOST'));
 const g=fixture();await g.start();g.lose();g.receiver.tick();assert.equal(g.receiver.status().channel,'RECONNECTING');assert.ok(g.rows.some(x=>x.kind==='B_STATE'&&x.reason==='REMOTE_B_CHANNEL_LOST'));
});
test('observer exceptions do not change admission, cause a reconnect or release reservations',async()=>{
 const f=fixture({throws:true});await f.start();try{f.sockets[0].frame(f.event());await settle();assert.equal(f.forwarded.length,1);assert.equal(f.receiver.status().state,'RUNNING');assert.equal(f.receiver.status().peer.generation,1);assert.ok(f.receiver.status().diagnostic_errors>0);}finally{f.receiver.stop();}
});
test('synchronous and async relay exceptions degrade the channel without closing the RTDS socket',async()=>{
 const f=fixture({enqueue(){throw Error('private relay failure');}});await f.start();try{f.sockets[0].frame(f.event());assert.ok(f.rows.some(x=>x.kind==='B_FORWARD_RESULT'&&x.reason==='RELAY_ENQUEUE_THROW'));assert.equal(f.receiver.status().peer.phase,'OPEN');assert.equal(f.receiver.status().channel,'RECONNECTING');}finally{f.receiver.stop();}
 const g=fixture({enqueue:async()=>{throw Error('private relay failure');}});await g.start();g.sockets[0].frame(g.event());await settle();assert.equal(g.receiver.status().channel,'RECONNECTING');assert.ok(g.rows.some(x=>x.reason==='RELAY_ENQUEUE_FAILURE'));assert.ok(!JSON.stringify(g.rows).includes('private'));
});
