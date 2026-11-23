import test from 'node:test';import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {ManagedRtdsSocket} from './managed-socket.mjs';import {RtdsPeer,RTDS_URL} from './connection.mjs';
const settle=()=>new Promise(r=>setImmediate(r));
class Socket{
 constructor(){this.listeners=new Map();this.confirm=true;this.exited=false;this.alive=true;}
 addEventListener(k,f){if(!this.listeners.has(k))this.listeners.set(k,new Set());this.listeners.get(k).add(f);}
 removeEventListener(k,f){this.listeners.get(k)?.delete(f);}
 emit(k,v={}){for(const f of [...this.listeners.get(k)??[]])f(v);}
 open(){this.emit('open');}send(){}close(){if(this.confirm){this.alive=false;this.emit('close',{code:1000});}}
}
function fixture(extra={}){let at=0;const sockets=[],rows=[],frames=[];const p=new RtdsPeer({source:'A',mono:()=>at,wall:()=>1800000000000+at,random:()=>1,sink:r=>rows.push(r),onActivity:e=>frames.push(e),socketFactory:()=>{const s=new Socket();sockets.push(s);return s;},...extra});p.start();sockets[0].open();return {p,sockets,rows,frames,step(ms){at+=ms;p.tick();}};}
test('hung close is recovered only after a positive termination witness; obsolete callbacks stay fenced',async()=>{
 const f=fixture({continuous:true});const old=f.sockets[0],late=[...old.listeners.get('message')][0];let confirm;old.confirm=false;old.terminate=()=>new Promise(r=>confirm=()=>{old.alive=false;r(true);});
 f.p.reconnect('TEST');f.step(2000);await settle();assert.equal(f.p.phase,'WAIT_ABORT');assert.equal(f.sockets.length,1);
 late({data:JSON.stringify({topic:'activity',type:'trades',payload:{late:true}})});assert.equal(f.frames.length,0);
 confirm();await settle();f.step(0);assert.equal(f.sockets.length,2);assert.equal(old.alive,false);f.sockets[1].open();f.sockets[1].emit('message',{data:JSON.stringify({topic:'activity',type:'trades',payload:{fresh:true}})});
 assert.equal(f.frames.length,1);assert.ok(f.rows.some(r=>r.kind==='ABORT_CONFIRMED'));assert.equal(f.p.health().stop_reason,null);f.p.stop();
});
for(const mode of ['reject','false','hang'])test(`failed hard close (${mode}) opens no replacement`,async()=>{
 const f=fixture({continuous:true});const s=f.sockets[0];s.confirm=false;s.terminate=()=>mode==='reject'?Promise.reject(Error('private error')):mode==='false'?Promise.resolve(false):new Promise(()=>{});
 f.p.reconnect('TEST');f.step(2000);await settle();f.step(2000);await settle();assert.equal(f.p.phase,'STOPPED');assert.match(f.p.reason,/ABORT/);assert.equal(f.sockets.length,1);assert.ok(!JSON.stringify(f.rows).includes('private error'));
});
test('operator stop during hard close cannot be undone by a late termination promise',async()=>{
 const f=fixture({continuous:true});let resolve;f.sockets[0].confirm=false;f.sockets[0].terminate=()=>new Promise(r=>resolve=r);f.p.reconnect('TEST');f.step(2000);await settle();f.p.stop('OPERATOR_STOP');await settle();resolve(true);await settle();f.step(600000);assert.equal(f.p.phase,'STOPPED');assert.equal(f.p.reason,'OPERATOR_STOP');assert.equal(f.sockets.length,1);
});
test('continuous mode retains the 20/10min budget and resumes after cooldown without lifetime exhaustion',()=>{
 const f=fixture({continuous:true,maxConnections:1});for(let i=0;i<20;i++){f.p.reconnect('TEST');f.step(5000);if(f.p.phase==='CONNECTING')f.sockets.at(-1).open();}
 assert.equal(f.p.phase,'COOLDOWN');assert.equal(f.sockets.length,20);assert.ok(f.p.health().next_connect_in_ms>0);f.step(600000);assert.equal(f.sockets.length,21);f.sockets.at(-1).open();
 for(let i=0;i<150;i++){f.p.reconnect('TEST');f.step(610000);f.sockets.at(-1).open();}
 assert.equal(f.p.phase,'OPEN');assert.equal(f.p.sessions.length,64);assert.ok(f.p.generation>120);f.p.stop();
});
test('real disposable worker exits before reconnection; no network is used',async()=>{
 let at=0;const sockets=[];const peer=new RtdsPeer({source:'A',continuous:true,mono:()=>at,socketFactory:url=>{const s=new ManagedRtdsSocket(url,{workerURL:new URL('./socket-worker-fixture.mjs',import.meta.url)});sockets.push(s);return s;}});
 peer.start();try{
  await new Promise(resolve=>sockets[0].addEventListener('open',resolve));peer.reconnect('TEST_HUNG_CLOSE');at=2000;peer.tick();assert.equal(sockets.length,1);
  await sockets[0].exit;await settle();assert.equal(sockets[0].exited,true);peer.tick();assert.equal(sockets.length,2);await new Promise(resolve=>sockets[1].addEventListener('open',resolve));assert.equal(peer.phase,'OPEN');
 }finally{peer.stop();await Promise.all(sockets.map(s=>s.terminate()));}
});
test('a worker terminate promise without its exit event is not sufficient evidence',async()=>{
 class Worker extends EventEmitter{postMessage(){}terminate(){return Promise.resolve(1);}}
 const s=new ManagedRtdsSocket(RTDS_URL,{WorkerClass:Worker});let done=false;s.terminate().then(()=>done=true);await settle();assert.equal(done,false);s.worker.emit('exit',1);await s.exit;await settle();assert.equal(done,true);
});
test('actual offline worker enforces frame size and bounds its outstanding messages',async()=>{
 for(const command of ['fixture-overflow','fixture-large']){
  const socket=new ManagedRtdsSocket(RTDS_URL,{workerURL:new URL('./socket-worker-fixture.mjs',import.meta.url)});let frames=0;
  socket.addEventListener('message',()=>frames++);
  try{await new Promise(r=>socket.addEventListener('open',r));const error=new Promise(r=>socket.addEventListener('error',r));socket.send(command);await error;
   assert.equal(frames,command==='fixture-overflow'?64:0);assert.equal(socket.exited,false);
  }finally{assert.equal(await socket.terminate(),true);}
 }
});
