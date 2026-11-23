import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import {telegramRequest,deliveryDecision} from './telegram-transport.mjs';
import {safeErrorCodes} from './telegram-diagnostics.mjs';
async function fixture(t,handler){
 const server=http.createServer(handler);await new Promise(r=>server.listen(0,'127.0.0.1',r));
 t.after(()=>{server.closeAllConnections();server.close();});
 const url=`http://127.0.0.1:${server.address().port}`;
 return (unused,opts)=>fetch(url,opts);
}
const body={chat_id:'PRIVATE_RECIPIENT',text:'PRIVATE_PAYLOAD'};
const ack=res=>{res.setHeader('content-type','application/json');res.end(JSON.stringify({ok:true,result:{message_id:1}}));};
test('bounded nested causes redact arbitrary codes, messages, addresses and cycles',()=>{
 const e={code:'PRIVATE_SECRET',message:'PRIVATE_PAYLOAD',errors:[{code:'ETIMEDOUT'},{code:'ECONNRESET'}]};e.cause=e;
 assert.deepEqual(safeErrorCodes(e),['ETIMEDOUT','ECONNRESET']);
});
test('real local HTTP captures request/header/body phases without secrets',async t=>{
 let hits=0,diag;const fetchImpl=await fixture(t,(req,res)=>{hits++;req.resume();req.on('end',()=>ack(res));});
 const r=await telegramRequest('PRIVATE_TOKEN','sendMessage',body,{fetchImpl,onDiagnostic:d=>diag=d});
 assert.equal(r.kind,'OK');assert.equal(hits,1);assert.equal(diag.phase,'RESPONSE_BODY_PARSED');
 for(const k of ['request_created_ms','headers_write_observed_ms','body_sent_observed_ms','response_headers_ms'])assert.equal(typeof diag[k],'number',k);
 assert.equal(diag.pre_send_failure_proven,false);assert.ok(!JSON.stringify(diag).includes('PRIVATE'));
});
test('server accepts POST then resets: UNKNOWN, exactly one POST, never replay',async t=>{
 let accepted=0,diag;const fetchImpl=await fixture(t,(req,res)=>{req.resume();req.on('end',()=>{accepted++;res.socket.destroy();});});
 const r=await telegramRequest('TEST','sendMessage',body,{fetchImpl,onDiagnostic:d=>diag=d});
 assert.equal(r.kind,'AMBIGUOUS');assert.equal(accepted,1);assert.equal(diag.pre_send_failure_proven,false);
 assert.equal(deliveryDecision(r,{attempts:0},0).state,'DELIVERY_UNKNOWN');assert.equal(deliveryDecision(r,{attempts:0},0).next,0);
});
test('response delayed: waiting for same response reduces UNKNOWN without second POST',async t=>{
 let accepted=0;const fetchImpl=await fixture(t,(req,res)=>{req.resume();req.on('end',()=>{accepted++;setTimeout(()=>ack(res),120);});});
 const short=await telegramRequest('TEST','sendMessage',body,{fetchImpl,timeoutMs:40,onDiagnostic:()=>{}});
 assert.equal(short.kind,'AMBIGUOUS');assert.equal(accepted,1);
 // A separate synthetic action, never retry the ambiguous action.
 const long=await telegramRequest('TEST','sendMessage',{...body,text:'SECOND_SYNTHETIC_ACTION'},{fetchImpl,timeoutMs:500,onDiagnostic:()=>{}});
 assert.equal(long.kind,'OK');assert.equal(accepted,2);
});
test('headers received, body lost: UNKNOWN with correct phase',async t=>{
 let diag;const fetchImpl=await fixture(t,(req,res)=>{req.resume();req.on('end',()=>{res.writeHead(200,{'content-type':'application/json','content-length':'100'});res.write('{');setTimeout(()=>res.destroy(),20);});});
 const r=await telegramRequest('TEST','sendMessage',body,{fetchImpl,onDiagnostic:d=>diag=d});
 assert.equal(r.reason,'RESPONSE_INVALID_JSON');assert.equal(diag.phase,'RESPONSE_HEADERS_RECEIVED');
});
test('no observable hooks never proves pre-send; diagnostics failure cannot change outcome',async()=>{
 let diag,calls=0;
 const r=await telegramRequest('TEST','sendMessage',body,{fetchImpl:async()=>{calls++;throw {cause:{code:'ETIMEDOUT',errors:[{code:'ECONNRESET'}]}};},onDiagnostic:d=>{diag=d;throw Error('logger unavailable');}});
 assert.equal(calls,1);assert.equal(r.kind,'AMBIGUOUS');assert.equal(diag.pre_send_failure_proven,false);assert.deepEqual(diag.error_codes,['ETIMEDOUT','ECONNRESET']);
});
test('parallel requests keep diagnostics isolated',async t=>{
 const ds=[];const fetchImpl=await fixture(t,(req,res)=>{req.resume();req.on('end',()=>ack(res));});
 await Promise.all(Array.from({length:8},()=>telegramRequest('TEST','sendMessage',body,{fetchImpl,onDiagnostic:d=>ds.push(d)})));
 assert.equal(ds.length,8);assert.equal(new Set(ds).size,8);assert.ok(ds.every(d=>d.phase==='RESPONSE_BODY_PARSED'));
});
test('250ms nested connection timeout is measured but never retried or declared safe',async()=>{
 let calls=0,diag;
 const r=await telegramRequest('TEST','sendMessage',body,{fetchImpl:async()=>{calls++;await new Promise(r=>setTimeout(r,250));throw new TypeError('fetch failed',{cause:new AggregateError([Object.assign(Error('private address'),{code:'ETIMEDOUT'}),Object.assign(Error('private address'),{code:'ECONNREFUSED'})])});},onDiagnostic:d=>diag=d});
 assert.equal(calls,1);assert.equal(r.kind,'AMBIGUOUS');assert.ok(diag.elapsed_ms>=240);assert.deepEqual(diag.error_codes,['ETIMEDOUT','ECONNREFUSED']);assert.equal(diag.pre_send_failure_proven,false);
});
test('keepalive lifecycle: server closes idle connection, later action uses a connection safely',async t=>{
 let hits=0;const ds=[];
 const fetchImpl=await fixture(t,(req,res)=>{hits++;req.resume();req.on('end',()=>{res.setHeader('connection','close');ack(res);});});
 for(let i=0;i<2;i++)assert.equal((await telegramRequest('TEST','sendMessage',{...body,text:`synthetic ${i}`},{fetchImpl,onDiagnostic:d=>ds.push(d)})).kind,'OK');
 assert.equal(hits,2);assert.ok(ds.every(d=>d.socket_seen_by_telegram_transport===false));
});
