// One native socket per disposable worker. Inert when imported by the main
// thread; no credentials, databases, REST client or sender enter this worker.
import {isMainThread,parentPort,workerData} from 'node:worker_threads';
import {attachNativeDiagnostics} from '../snipe-rtds-passive-ab/diagnostics.mjs';
if(!isMainThread){
 if(workerData?.url!=='wss://ws-live-data.polymarket.com/')throw Error('RTDS_TARGET_REQUIRED');
 const post=value=>parentPort.postMessage(value);let socket,credits=64,closing=false;
 const dispose=attachNativeDiagnostics(value=>post({kind:'diagnostic',value}));
 socket=new WebSocket(workerData.url);
 socket.addEventListener('open',()=>post({kind:'open'}));
 socket.addEventListener('message',event=>{
  if(closing)return;
  if(typeof event.data!=='string'||Buffer.byteLength(event.data)>262144||--credits<0){closing=true;post({kind:'error'});socket.close();return;}
  post({kind:'message',data:event.data});
 });
 socket.addEventListener('error',()=>post({kind:'error'}));
 socket.addEventListener('close',event=>{
  closing=true;dispose();post({kind:'socket_close',code:Number.isInteger(event.code)?event.code:1006,reason:typeof event.reason==='string'?event.reason:''});parentPort.close();
 });
 parentPort.on('message',value=>{
  if(value?.kind==='consumed'){credits=Math.min(64,credits+1);return;}
  if(value?.kind==='close'){closing=true;socket.close();return;}
  if(value?.kind==='send'&&!closing&&typeof value.data==='string'&&Buffer.byteLength(value.data)<=16384){try{socket.send(value.data);}catch{post({kind:'error'});}}
 });
}
