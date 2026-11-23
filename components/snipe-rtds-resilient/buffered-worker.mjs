// A-only native transport. No credentials, wallet scope, queue DB or Telegram.
import {isMainThread,parentPort,workerData} from 'node:worker_threads';
import {createFrameBuffer,FRAME_FAILURES} from './frame-buffer.mjs';
import {attachNativeDiagnostics,safeSocketError} from '../snipe-rtds-passive-ab/diagnostics.mjs';
if(!isMainThread){
 if(workerData?.url!=='wss://ws-live-data.polymarket.com/'||workerData?.protocol!=='A_BUFFER_V1')throw Error('A_BUFFER_TARGET_REQUIRED');
 const post=row=>parentPort.postMessage(row);let socket,timer,closing=false,nativeEnd=null,pendingError=null,finished=false,finishing=false,ticks=0;
 const finish=()=>{if(!nativeEnd||finished||finishing)return;finishing=true;setImmediate(()=>{finished=true;clearInterval(timer);dispose();if(pendingError)post({kind:'transport_fault',reason:pendingError});post({kind:'socket_close',...nativeEnd,reason:pendingError});post({kind:'buffer_stats',stats:buffer.snapshot()});parentPort.close();});};
 const fault=reason=>{if(!FRAME_FAILURES.has(reason))reason='WORKER_PROTOCOL_ERROR';pendingError=reason;closing=true;post({kind:'transport_fault',reason});try{socket?.close();}catch{};};
 const buffer=createFrameBuffer({post,onFault:fault,onDrained:finish,limits:workerData.limits});
 const dispose=attachNativeDiagnostics(value=>post({kind:'diagnostic',value}));
 try{socket=new WebSocket(workerData.url);}catch{buffer.fail('SOCKET_CONSTRUCT_ERROR');nativeEnd={code:1006};finish();}
 if(socket){
  socket.addEventListener('open',()=>post({kind:'open'}));
  socket.addEventListener('message',event=>buffer.accept(event.data));
  socket.addEventListener('error',event=>{pendingError='NATIVE_SOCKET_ERROR';post({kind:'diagnostic',value:{kind:'NATIVE_SOCKET_ERROR',...safeSocketError(event?.error)}});nativeEnd??={code:1006};buffer.end();try{socket.close();}catch{};});
  socket.addEventListener('close',event=>{
   nativeEnd={code:Number.isInteger(event.code)?event.code:1006};if(event.code===1006)pendingError??='NATIVE_SOCKET_ERROR';
   // Already accepted complete frames drain before their generation closes.
   buffer.end();if(buffer.snapshot().failed||closing)finish();
  });
  timer=setInterval(()=>{buffer.tick();if(++ticks%10===0)post({kind:'buffer_stats',stats:buffer.snapshot()});},25);timer.unref?.();
  parentPort.on('message',frame=>{
   if(frame?.kind==='receipt'){buffer.acknowledge(frame);return;}
   if(frame?.kind==='close'){closing=true;buffer.cancel('SOURCE_GENERATION_FENCED');try{socket.close();}catch{};return;}
   if(frame?.kind==='send'&&!closing&&typeof frame.data==='string'&&Buffer.byteLength(frame.data)<=16384){try{socket.send(frame.data);}catch{buffer.fail('NATIVE_SEND_ERROR');}}
  });
 }
}
