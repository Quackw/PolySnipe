import test from 'node:test';import assert from 'node:assert/strict';
import https from 'node:https';import fs from 'node:fs';import os from 'node:os';import path from 'node:path';import {execFileSync} from 'node:child_process';
import {createTelegramHTTPSFetch} from './telegram-https.mjs';import {telegramRequest,deliveryDecision} from './telegram-transport.mjs';import {createTelegramCircuit} from './telegram-circuit.mjs';
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'telegram-tls-fixture-'));process.on('exit',()=>fs.rmSync(dir,{recursive:true,force:true}));
execFileSync('openssl',['req','-x509','-newkey','rsa:2048','-nodes','-keyout',dir+'/key','-out',dir+'/cert','-days','1','-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost'],{stdio:'ignore'});
const key=fs.readFileSync(dir+'/key'),cert=fs.readFileSync(dir+'/cert');
async function fixture(t,handler){
 const agent=new https.Agent({keepAlive:true,maxSockets:1,maxFreeSockets:1,autoSelectFamily:true,autoSelectFamilyAttemptTimeout:1000});
 const server=https.createServer({key,cert},handler);await new Promise(r=>server.listen(0,'127.0.0.1',r));
 t.after(()=>{agent.destroy();server.closeAllConnections();server.close();});
 return createTelegramHTTPSFetch({agent,requestImpl:(url,opts,callback)=>https.request(`https://127.0.0.1:${server.address().port}`,{...opts,ca:cert,servername:'localhost'},callback)});
}
const body={chat_id:'fixture',text:'private'};
function ack(res){res.end(JSON.stringify({ok:true,result:{message_id:3,chat:{id:1}}}));}
test('dedicated TLS connection reuses a verified socket, captures phases and sends once',async t=>{
 let posts=0;const diagnostics=[];const fetchImpl=await fixture(t,(req,res)=>{req.resume();req.on('end',()=>{posts++;ack(res);});});
 for(let i=0;i<2;i++)assert.equal((await telegramRequest('PRIVATE','sendMessage',body,{fetchImpl,onDiagnostic:d=>diagnostics.push(d)})).kind,'OK');
 assert.equal(posts,2);assert.equal(diagnostics[0].reused_socket,false);assert.equal(diagnostics[1].reused_socket,true);assert.equal(typeof diagnostics[0].tls_ms,'number');assert.equal(diagnostics[0].family_attempt_ms,1000);
 assert.ok(!JSON.stringify(diagnostics).includes('PRIVATE'));assert.ok(diagnostics.every(x=>x.pre_send_failure_proven===false));
});
test('accept POST then lose response: UNKNOWN and one send only',async t=>{
 let posts=0;const fetchImpl=await fixture(t,(req,res)=>{req.resume();req.on('end',()=>{posts++;res.socket.destroy();});});
 const r=await telegramRequest('TEST','sendMessage',body,{fetchImpl});assert.equal(r.kind,'AMBIGUOUS');assert.equal(posts,1);assert.equal(deliveryDecision(r,{attempts:0},0).next,0);
});
test('server closes idle TLS socket: next action creates a fresh connection without replay',async t=>{
 let posts=0;const fetchImpl=await fixture(t,(req,res)=>{req.resume();req.on('end',()=>{posts++;res.setHeader('Connection','close');ack(res);});});
 for(let i=0;i<2;i++)assert.equal((await telegramRequest('TEST','sendMessage',body,{fetchImpl})).kind,'OK');assert.equal(posts,2);
});
test('lost body and delayed response are ambiguous without a second request',async t=>{
 let posts=0;const fetchImpl=await fixture(t,(req,res)=>{req.resume();req.on('end',()=>{posts++;res.writeHead(200,{'Content-Length':'1000'});res.write('{');setTimeout(()=>res.destroy(),40);});});
 const r=await telegramRequest('TEST','sendMessage',body,{fetchImpl,timeoutMs:500});assert.equal(r.kind,'AMBIGUOUS');assert.equal(posts,1);
});
test('response held longer than deadline: abort once, no replay',async t=>{
 let posts=0;const fetchImpl=await fixture(t,(req,res)=>{req.resume();req.on('end',()=>{posts++;setTimeout(()=>ack(res),150);});});
 const r=await telegramRequest('TEST','sendMessage',body,{fetchImpl,timeoutMs:70});assert.equal(r.kind,'AMBIGUOUS');assert.equal(posts,1);
});
test('untrusted certificate fails closed before any HTTP request',async t=>{
 let posts=0;const fetchImpl=await fixture(t,(req,res)=>{posts++;ack(res);});
 // Separate request with no fixture CA; TLS verification must remain enabled.
 let custom;const server=https.createServer({key,cert},(req,res)=>{posts++;ack(res);});await new Promise(r=>server.listen(0,'127.0.0.1',r));t.after(()=>{server.closeAllConnections();server.close();});
 custom=createTelegramHTTPSFetch({requestImpl:(url,opts,cb)=>https.request(`https://127.0.0.1:${server.address().port}`,{...opts,servername:'localhost',agent:false},cb)});
 assert.equal((await telegramRequest('TEST','sendMessage',body,{fetchImpl:custom})).kind,'AMBIGUOUS');assert.equal(posts,0);
});
test('three spaced UNKNOWN never disable new actions; consecutive failures cool down at most 60s',()=>{
 let at=0;const c=createTelegramCircuit({now:()=>at});
 for(let i=0;i<3;i++){c.observe({kind:'AMBIGUOUS'});assert.equal(c.ready(),true);c.observe({kind:'OK'});at+=60000;}
 for(let i=0;i<3;i++)c.observe({kind:'AMBIGUOUS'});assert.equal(c.ready(),false);assert.equal(c.snapshot().cooldown_remaining_ms,5000);at+=5000;assert.equal(c.ready(),true);
 for(let i=0;i<20;i++){c.observe({kind:'AMBIGUOUS'});assert.ok(c.snapshot().cooldown_remaining_ms<=60000);at+=60000;}
 c.observe({kind:'OK'});assert.equal(c.snapshot().state,'READY');assert.equal(c.snapshot().unknown_replay,false);
});
test('pool always has bounded concurrency and address-selection interval 1000ms',async()=>{
 let checked=false;const fetchImpl=createTelegramHTTPSFetch({requestImpl:(url,opts)=>{checked=true;assert.equal(opts.agent.maxSockets,1);assert.equal(opts.agent.options.autoSelectFamilyAttemptTimeout,1000);assert.equal(opts.agent.options.autoSelectFamily,true);throw Object.assign(Error('connect failed'),{code:'ETIMEDOUT'});}});
 assert.equal((await telegramRequest('TEST','sendMessage',body,{fetchImpl})).kind,'AMBIGUOUS');assert.ok(checked);
});
