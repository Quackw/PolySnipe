// Review-only, network-free B ingress protocol. The caller must supply an
// already authenticated, encrypted SSH channel pinned to the target host.
// All committed receipts go through the one supplied queue owner. This module
// never creates a socket, SSH process, DB, Telegram sender or REST client.
import {createHmac,createHash,randomBytes,randomUUID,timingSafeEqual} from 'node:crypto';
import {createScopePublisher,createScopeSubscriber} from '../snipe-v3-combined/scope-sync.mjs';
import {createHealthOwner,createHealthPeer} from './bridge-health.mjs';
const MAX_FRAME=262144,CHALLENGE_MAX_MS=2000;
const sha=s=>createHash('sha256').update(s).digest('hex');
const validRun=s=>typeof s==='string'&&/^[a-z0-9][a-z0-9-]{7,95}$/.test(s);
const hex=s=>typeof s==='string'&&/^[a-f0-9]{64}$/.test(s);
const mac=(key,parts)=>createHmac('sha256',key).update(JSON.stringify(parts)).digest('hex');
const verify=(key,parts,provided)=>hex(provided)&&timingSafeEqual(Buffer.from(mac(key,parts),'hex'),Buffer.from(provided,'hex'));
function configured({runId,scopeFingerprint,key}){
 if(!validRun(runId)||!hex(scopeFingerprint)||!Buffer.isBuffer(key)||key.length!==32)throw Error('BRIDGE_CONFIGURATION_INVALID');
 return {runId,scopeFingerprint,key:Buffer.from(key)};
}
const authParts=(c,challenge,health=false)=>['B_BRIDGE_AUTH_V1',c.runId,c.scopeFingerprint,challenge,...(health?['HEALTH_V1']:[])];
const frameParts=(c,challenge,seq,id,payload)=>['B_BRIDGE_FRAME_V1',c.runId,c.scopeFingerprint,challenge,seq,id,sha(payload)];
const ackParts=(c,nonce,sequence,id,status,blocked)=>['B_BRIDGE_ACK_V1',c.runId,c.scopeFingerprint,nonce,sequence,id,status,blocked];
export function createBBridgeReceiver({runId,scopeFingerprint,key,owner,mono=()=>performance.now()}){
 const c=configured({runId,scopeFingerprint,key});if(!owner||!['openSource','submit','disconnectSource'].every(k=>typeof owner[k]==='function'))throw Error('BRIDGE_OWNER_REQUIRED');
 let offer=null,session=null,previous=null,closed=false,scopePublisher=null,healthOwner=null,healthSupported=false;
 function disconnect(){offer=null;previous=null;healthSupported=false;scopePublisher?.close();scopePublisher=null;healthOwner?.close();healthOwner=null;const s=session;session=null;if(s)owner.disconnectSource(s);}
 function fail(reason){try{disconnect();}catch{}return {status:'BLOCKED',reason,delivery_authorized:false};}
 function challenge(){
  if(closed||offer||session)throw Error('BRIDGE_SESSION_ALREADY_OPEN');
  const created=mono(),nonce=randomBytes(32).toString('hex');if(!Number.isFinite(created))throw Error('BRIDGE_CLOCK_INVALID');
  offer={nonce,created};return {kind:'CHALLENGE',run_id:c.runId,scope_fingerprint:c.scopeFingerprint,nonce,health_protocol:1};
 }
 function authenticate(frame){
  if(session)return {status:'BLOCKED',reason:'BRIDGE_ALREADY_CONNECTED',delivery_authorized:false};
  if(closed||!offer||frame?.kind!=='AUTH'||frame.nonce!==offer.nonce)return fail('BRIDGE_AUTH_INVALID');
  const age=mono()-offer.created;
  if(![undefined,1].includes(frame.health_protocol)||!Number.isFinite(age)||age<0||age>CHALLENGE_MAX_MS||!verify(c.key,authParts(c,offer.nonce,frame.health_protocol===1),frame.mac))return fail('BRIDGE_AUTH_INVALID');
  healthSupported=frame.health_protocol===1;
  const nonce=offer.nonce;offer=null;
  try{session=owner.openSource('B');if(session.status!=='CONNECTED'||session.scope_fingerprint!==c.scopeFingerprint){disconnect();return {status:'BLOCKED',reason:'BRIDGE_SCOPE_MISMATCH',delivery_authorized:false};}}
  catch{return fail('BRIDGE_OWNER_CONNECT_FAILED');}
  previous={nonce,sequence:0,id:null,payloadHash:null};scopePublisher=createScopePublisher({runId:c.runId,nonce,key:c.key});healthOwner=createHealthOwner({...c,nonce},mono);return {status:'CONNECTED',delivery_authorized:false};
 }
 function receive(frame){
  if(closed||!session||!previous)return fail('BRIDGE_NOT_CONNECTED');
  if(!frame||frame.kind!=='FRAME'||!Number.isSafeInteger(frame.sequence)||frame.sequence<1||typeof frame.message_id!=='string'||!/^[A-Za-z0-9_-]{1,96}$/.test(frame.message_id)||typeof frame.payload_json!=='string'||Buffer.byteLength(frame.payload_json)>MAX_FRAME||!verify(c.key,frameParts(c,previous.nonce,frame.sequence,frame.message_id,frame.payload_json),frame.mac))return fail('BRIDGE_FRAME_INVALID');
  const digest=sha(frame.payload_json),repeat=frame.sequence===previous.sequence;
  if(repeat&&(frame.message_id!==previous.id||digest!==previous.payloadHash))return fail('BRIDGE_REPLAY_CONFLICT');
  if(!repeat&&frame.sequence!==previous.sequence+1)return fail('BRIDGE_SEQUENCE_GAP');
  let envelope;try{envelope=JSON.parse(frame.payload_json);}catch{return fail('BRIDGE_INVALID_JSON');}
  let result;try{result=owner.submit({...session,message_id:frame.message_id,sequence:frame.sequence,envelope});}catch{return fail('BRIDGE_OWNER_SUBMIT_FAILED');}
  if(result.status==='BLOCKED'&&['SOURCE_DISCONNECTED_OR_OBSOLETE','STATE_TOO_STALE','ADMISSIONS_HALTED','SQLITE_BUSY','PERSISTENCE_FAILURE','FROZEN_SCOPE_CHANGED','ACTIVE_SCOPE_CHANGED','IDENTITY_CONFLICT','QUEUE_CONFLICT'].includes(result.reason))return fail(result.reason);
  if(!['ACCEPTED','DUPLICATE','ALREADY_COMMITTED','REJECTED','BLOCKED'].includes(result.status))return fail('BRIDGE_OWNER_RESPONSE_INVALID');
  const blocked=result.status==='ALREADY_COMMITTED'&&repeat?previous.blocked:(result.results??[]).some(r=>r.status==='BLOCKED');
  if(!repeat){previous.sequence=frame.sequence;previous.id=frame.message_id;previous.payloadHash=digest;previous.blocked=blocked;}
  // An ACK covers only the queue decision, never Telegram delivery. Per-pair
  // UNKNOWN remains blocked by the common owner and is not replayed.
  return {kind:'ACK',sequence:frame.sequence,message_id:frame.message_id,status:result.status,blocked_pairs:blocked,delivery_authorized:false,
   mac:mac(c.key,ackParts(c,previous.nonce,frame.sequence,frame.message_id,result.status,blocked))};
 }
 function close(){if(closed)return;try{disconnect();}finally{closed=true;c.key.fill(0);}}
 function scopeUpdate(wallets){if(!scopePublisher||!session)throw Error('BRIDGE_NOT_CONNECTED');return scopePublisher.update(wallets);}
 return Object.freeze({challenge,authenticate,receive,scopeUpdate,disconnect,close,healthSupported:()=>healthSupported,healthChallenge:()=>{if(!healthOwner||!healthSupported)throw Error('BRIDGE_HEALTH_NOT_NEGOTIATED');return healthOwner.challenge();},receiveHealth:frame=>{if(!healthOwner||!healthSupported)throw Error('BRIDGE_HEALTH_NOT_NEGOTIATED');return healthOwner.receive(frame);},peerHealth:()=>healthSupported?healthOwner?.snapshot()??{state:'UNKNOWN',reason:'B_NOT_CONNECTED'}:{state:'UNKNOWN',reason:session?'B_HEALTH_CAPABILITY_MISSING':'B_NOT_CONNECTED'}});
}
export function createBBridgeProducer({runId,scopeFingerprint,key}){
 const c=configured({runId,scopeFingerprint,key});let nonce=null,sequence=0,last=null,closed=false,pending=false,scopeSubscriber=null,healthPeer=null;
 function auth(challenge){
  if(closed||nonce||challenge?.kind!=='CHALLENGE'||challenge.run_id!==c.runId||challenge.scope_fingerprint!==c.scopeFingerprint||!hex(challenge.nonce))throw Error('BRIDGE_CHALLENGE_INVALID');
  const health=challenge.health_protocol===1;nonce=challenge.nonce;if(health)healthPeer=createHealthPeer({...c,nonce});return {kind:'AUTH',nonce,...(health?{health_protocol:1}:{}),mac:mac(c.key,authParts(c,nonce,health))};
 }
 function frame(envelope){
  if(closed||!nonce)throw Error('BRIDGE_NOT_AUTHENTICATED');if(pending)throw Error('BRIDGE_PENDING_ACK');let payload;try{payload=JSON.stringify(envelope);}catch{throw Error('BRIDGE_INVALID_JSON');}
  if(typeof payload!=='string'||Buffer.byteLength(payload)>MAX_FRAME)throw Error('BRIDGE_FRAME_INVALID');
  const seq=++sequence,id=randomUUID();last=Object.freeze({kind:'FRAME',sequence:seq,message_id:id,payload_json:payload,mac:mac(c.key,frameParts(c,nonce,seq,id,payload))});pending=true;return last;
 }
 function acknowledge(ack){
  if(!pending||!last||ack?.kind!=='ACK'||ack.sequence!==last.sequence||ack.message_id!==last.message_id||!['ACCEPTED','DUPLICATE','ALREADY_COMMITTED','REJECTED','BLOCKED'].includes(ack.status)||typeof ack.blocked_pairs!=='boolean'||ack.delivery_authorized!==false||!verify(c.key,ackParts(c,nonce,ack.sequence,ack.message_id,ack.status,ack.blocked_pairs),ack.mac))throw Error('BRIDGE_ACK_INVALID');
  // An ambiguous pair remains fenced in the owner, but must not block unrelated
  // future actions. Surface a per-frame diagnostic and never retry it blindly.
  const attention=ack.blocked_pairs||['REJECTED','BLOCKED'].includes(ack.status);
  pending=false;return {status:attention?'PROCESSED_WITH_ATTENTION':'QUEUED_OR_DEDUPLICATED',ready:true,attention};
 }
 function retransmit(){if(!last||!pending)throw Error('BRIDGE_NO_PENDING_FRAME');return last;}
 function receiveScope(frame,apply){if(closed||!nonce)throw Error('BRIDGE_NOT_AUTHENTICATED');if(!scopeSubscriber)scopeSubscriber=createScopeSubscriber({runId:c.runId,nonce,key:c.key,apply});return scopeSubscriber.receive(frame);}
 function close(){nonce=null;last=null;pending=false;closed=true;scopeSubscriber?.close();healthPeer?.close();c.key.fill(0);}
 return Object.freeze({auth,frame,acknowledge,receiveScope,retransmit,close,answerHealth:(frame,value)=>{if(!healthPeer||closed)throw Error('BRIDGE_NOT_CONNECTED');return healthPeer.answer(frame,value);}});
}
