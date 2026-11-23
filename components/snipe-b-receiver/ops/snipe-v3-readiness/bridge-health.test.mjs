import test from 'node:test';import assert from 'node:assert/strict';
import {createHealthOwner,createHealthPeer,HEALTH_LIMIT_MS} from './bridge-health.mjs';
import {createBBridgeReceiver,createBBridgeProducer} from './b-bridge.mjs';
const config={runId:'health-test-local',scopeFingerprint:'a'.repeat(64),nonce:'b'.repeat(64),key:Buffer.alloc(32,7)},health={phase:'OPEN',generation:1,activity_frames:100,last_valid_activity_age_ms:10,stop_reason:null,journal_ok:true};
test('health is nonce-bound evidence with strict 2000ms edges, no send authority',()=>{
 for(const ms of [2000,2000.001,-1]){let t=0;const a=createHealthOwner(config,()=>t),b=createHealthPeer(config),c=a.challenge(),reply=b.answer(c,health);t=ms;if(ms===2000){const h=a.receive(reply);assert.equal(h.state,'FRESH');assert.equal(h.delivery_authorized,undefined);}else assert.throws(()=>a.receive(reply),/STALE/);a.close();b.close();}assert.equal(HEALTH_LIMIT_MS,2000);
});
test('replayed, altered and previous-session health cannot mark B alive',()=>{
 let t=0;const a=createHealthOwner(config,()=>t),b=createHealthPeer(config);const c=a.challenge(),r=b.answer(c,health);assert.throws(()=>a.receive({...r,health:{...health,activity_frames:999}}),/MAC/);a.receive(r);assert.throws(()=>a.receive(r),/REPLAY/);assert.throws(()=>b.answer(c,health),/CHALLENGE/);
 const other=createHealthOwner({...config,nonce:'c'.repeat(64)},()=>t);other.challenge();assert.throws(()=>other.receive(r));t=7001;assert.equal(a.snapshot().state,'UNKNOWN');a.close();b.close();other.close();
});
test('health cannot stand in for a frame ACK or affect frame/owner sequence',()=>{
 let submits=0;const owner={openSource:()=>({status:'CONNECTED',scope_fingerprint:config.scopeFingerprint}),disconnectSource(){},submit(){submits++;return {status:'ACCEPTED',results:[]};}};
 const a=createBBridgeReceiver({...config,owner}),b=createBBridgeProducer(config);a.authenticate(b.auth(a.challenge()));
 const frame=b.frame({fixture:true});const reply=b.answerHealth(a.healthChallenge(),health);a.receiveHealth(reply);assert.equal(submits,0);assert.throws(()=>b.acknowledge(reply),/ACK/);
 assert.equal(b.acknowledge(a.receive(frame)).status,'QUEUED_OR_DEDUPLICATED');assert.equal(submits,1);a.close();b.close();
});
test('old B remains usable during an upgrade but never receives a false healthy RTDS verdict',()=>{
 let submits=0;const owner={openSource:()=>({status:'CONNECTED',scope_fingerprint:config.scopeFingerprint}),disconnectSource(){},submit(){submits++;return {status:'ACCEPTED',results:[]};}};
 const a=createBBridgeReceiver({...config,owner}),b=createBBridgeProducer(config),challenge=a.challenge();delete challenge.health_protocol;
 assert.equal(a.authenticate(b.auth(challenge)).status,'CONNECTED');assert.equal(a.healthSupported(),false);assert.equal(a.peerHealth().state,'UNKNOWN');
 assert.equal(b.acknowledge(a.receive(b.frame({fixture:true}))).status,'QUEUED_OR_DEDUPLICATED');assert.equal(submits,1);a.close();b.close();
});
test('health negotiation is MAC-bound and cannot be stripped from an AUTH frame',()=>{
 const owner={openSource:()=>({status:'CONNECTED',scope_fingerprint:config.scopeFingerprint}),disconnectSource(){},submit(){throw Error('must not submit');}};
 const a=createBBridgeReceiver({...config,owner}),b=createBBridgeProducer(config),auth=b.auth(a.challenge());delete auth.health_protocol;assert.equal(a.authenticate(auth).status,'BLOCKED');assert.equal(a.peerHealth().state,'UNKNOWN');a.close();b.close();
});
