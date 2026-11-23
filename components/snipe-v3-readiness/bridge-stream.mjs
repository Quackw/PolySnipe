import {StringDecoder} from 'node:string_decoder';
import {HEALTH_PERIOD_MS} from './bridge-health.mjs';
// Framed transport adapter for an already authenticated, encrypted duplex.
// No sockets or external processes are created here. A transport failure is a
// visible observation gap; pending events are never silently replayed.
const MAX_WIRE=600000,AUTH_MS=2000;
function lines(stream,onLine,onFailure){
 const decoder=new StringDecoder('utf8');let pending='',closed=false;
 const fail=reason=>{if(closed)return;closed=true;stream.removeListener('data',data);onFailure(reason);};
 function data(chunk){
  try{pending+=decoder.write(Buffer.isBuffer(chunk)?chunk:Buffer.from(chunk));if(Buffer.byteLength(pending)>MAX_WIRE)throw Error('BRIDGE_WIRE_LIMIT');let n;while((n=pending.indexOf('\n'))>=0){const line=pending.slice(0,n);pending=pending.slice(n+1);if(!line)throw Error('BRIDGE_WIRE_EMPTY');onLine(JSON.parse(line));if(closed)return;}}
  catch{fail('BRIDGE_WIRE_INVALID');}
 }
 stream.on('data',data);stream.on('error',()=>fail('BRIDGE_TRANSPORT_ERROR'));stream.on('end',()=>fail('BRIDGE_TRANSPORT_ENDED'));stream.on('close',()=>fail('BRIDGE_TRANSPORT_CLOSED'));
 return {fail};
}
const send=(stream,obj)=>stream.write(JSON.stringify(obj)+'\n');
export function serveBChannel({stream,receiver,timeoutMs=AUTH_MS,onState=()=>{},getWallets=null,healthEnabled=false}){
 let state='AUTH',finished=false,timer,framer,healthTimer,healthErrors=0;
 function finish(reason){if(finished)return;finished=true;try{onState('CLOSED');}catch{}clearTimeout(timer);clearInterval(healthTimer);try{receiver.disconnect();}catch{}try{stream.destroy();}catch{}}
 function heartbeat(){if(finished||state!=='READY'||!receiver.healthSupported?.())return;try{if((stream.writableLength??0)>MAX_WIRE)throw Error('BRIDGE_HEALTH_BACKPRESSURE');send(stream,receiver.healthChallenge());}catch{finish('BRIDGE_HEALTH_FAILED');}}
 try{send(stream,receiver.challenge());}catch{try{stream.destroy();}catch{}return {close(){},status:()=>({state:'BLOCKED',reason:'BRIDGE_OFFER_FAILED'})};}
 framer=lines(stream,frame=>{
  if(state==='AUTH'){const r=receiver.authenticate(frame);if(r.status!=='CONNECTED'){finish(r.reason);return;}clearTimeout(timer);state='READY';try{onState('READY');}catch{}send(stream,{kind:'READY'});if(getWallets)send(stream,receiver.scopeUpdate(getWallets()));if(healthEnabled){heartbeat();healthTimer=setInterval(heartbeat,HEALTH_PERIOD_MS);healthTimer.unref?.();}return;}
  if(state==='READY'&&healthEnabled&&frame?.kind==='HEALTH_REPLY'){try{receiver.receiveHealth(frame);}catch{healthErrors++;}return;}
  if(state!=='READY')throw Error('BRIDGE_INVALID_STATE');const ack=receiver.receive(frame);if(ack.kind!=='ACK'){finish(ack.reason);return;}send(stream,ack);
 },finish);
 timer=setTimeout(()=>finish('BRIDGE_AUTH_TIMEOUT'),timeoutMs);timer.unref?.();
 return {close:()=>finish('BRIDGE_CLOSED'),syncScope:wallets=>{if(finished||state!=='READY')return false;try{if((stream.writableLength??0)>MAX_WIRE)throw Error('BRIDGE_SCOPE_BACKPRESSURE');send(stream,receiver.scopeUpdate(wallets));return true;}catch{finish('BRIDGE_SCOPE_SYNC_FAILED');return false;}},status:()=>({state:finished?'CLOSED':state,peer_health:{...receiver.peerHealth?.()??{state:'UNKNOWN'},evidence_errors:healthErrors}})};
}
export function createBRelay({stream,producer,wallets,maxQueue=128,timeoutMs=10000,now=()=>performance.now(),onScope=()=>{},getHealth=null}){
 if(!Array.isArray(wallets)||!wallets.length||!Number.isInteger(maxQueue)||maxQueue<1||maxQueue>10000)throw Error('BRIDGE_RELAY_CONFIG_INVALID');
 let allowed=new Set(wallets.map(x=>String(x).toLowerCase()));let state='CHALLENGE',queue=[],inflight=null,timer,closed=false,gaps=0,accepted=0,attention=0,closeReason=null;
 const clear=()=>{clearTimeout(timer);timer=null;};
 function stop(reason){if(closed)return;closed=true;state='CLOSED';closeReason=reason;clear();const pending=[...(inflight?[inflight]:[]),...queue];gaps+=pending.length;inflight=null;queue=[];for(const x of pending)x.resolve({status:'OBSERVATION_GAP',reason});try{producer.close();}catch{}try{stream.destroy();}catch{}}
 function deadline(){clear();timer=setTimeout(()=>stop('BRIDGE_ACK_TIMEOUT'),timeoutMs);timer.unref?.();}
 function pump(){if(closed||state!=='READY'||inflight||!queue.length)return;const item=queue.shift(),age=now()-item.at;if(!Number.isFinite(age)||age<0||age>timeoutMs){item.resolve({status:'OBSERVATION_GAP',reason:'BRIDGE_QUEUE_EXPIRED'});gaps++;stop('BRIDGE_QUEUE_EXPIRED');return;}try{inflight=item;send(stream,producer.frame(item.envelope));deadline();}catch{stop('BRIDGE_FRAME_FAILED');}}
 lines(stream,frame=>{
  if(state==='CHALLENGE'){send(stream,producer.auth(frame));state='AUTH';clear();timer=setTimeout(()=>stop('BRIDGE_AUTH_TIMEOUT'),AUTH_MS);timer.unref?.();return;}
  if(state==='AUTH'){if(frame?.kind!=='READY')throw Error('BRIDGE_READY_INVALID');clear();state='READY';pump();return;}
  if(state==='READY'&&frame?.kind==='SCOPE_UPDATE'){producer.receiveScope(frame,values=>{onScope(values);allowed=new Set(values);});return;}
  if(state==='READY'&&frame?.kind==='HEALTH_CHALLENGE'&&getHealth){send(stream,producer.answerHealth(frame,getHealth()));return;}
  if(state!=='READY'||!inflight)throw Error('BRIDGE_UNEXPECTED_ACK');const result=producer.acknowledge(frame);clear();const item=inflight;inflight=null;accepted++;if(result.attention)attention++;item.resolve({...result,owner_status:frame.status,blocked_pairs:frame.blocked_pairs});pump();
 },stop);
 timer=setTimeout(()=>stop('BRIDGE_AUTH_TIMEOUT'),AUTH_MS);timer.unref?.();
 function enqueue(envelope){
  const wallet=envelope?.payload?.proxyWallet;if(closed||state!=='READY'){gaps++;return Promise.resolve({status:'OBSERVATION_GAP',reason:'BRIDGE_NOT_READY'});}
  if(typeof wallet!=='string'||!allowed.has(wallet.toLowerCase()))return Promise.resolve({status:'REJECTED',reason:'OUTSIDE_FROZEN_SCOPE'});
  try{const json=JSON.stringify(envelope);if(typeof json!=='string'||Buffer.byteLength(json)>262144)throw Error('BRIDGE_FRAME_LIMIT');}catch{gaps++;stop('BRIDGE_FRAME_LIMIT');return Promise.resolve({status:'OBSERVATION_GAP',reason:'BRIDGE_FRAME_LIMIT'});}
  if(queue.length+(inflight?1:0)>=maxQueue){gaps++;stop('BRIDGE_QUEUE_FULL');return Promise.resolve({status:'OBSERVATION_GAP',reason:'BRIDGE_QUEUE_FULL'});}
  return new Promise(resolve=>{queue.push({envelope,at:now(),resolve});pump();});
 }
 return Object.freeze({enqueue,close:()=>stop('BRIDGE_CLOSED'),status:()=>({state,reason:closeReason,pending:queue.length+(inflight?1:0),accepted,attention,gaps})});
}
