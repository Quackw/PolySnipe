// Process-wide subscriptions, but capture only requests inside this transport's context.
// No URLs, addresses, headers, bodies, recipients, error messages or stacks are retained.
import {AsyncLocalStorage} from 'node:async_hooks';
import {channel} from 'node:diagnostics_channel';
import {performance} from 'node:perf_hooks';
const scope=new AsyncLocalStorage();
export function recordTelegramConnection(fields){
 const ctx=scope.getStore();if(!ctx||ctx.closed)return;
 const numeric=new Set(['dns_ms','tcp_ms','tls_ms','request_finish_ms','response_end_ms','family_attempt_ms','preconnect_attempts','preconnect_failures']);
 for(const [key,value] of Object.entries(fields)){
  if(numeric.has(key)&&Number.isFinite(value))ctx.data[key]=Math.max(0,Math.round(value*1000)/1000);
  if(key==='reused_socket'&&typeof value==='boolean')ctx.data[key]=value;
  if(key==='address_family'&&[4,6].includes(value))ctx.data[key]=value;
 }
}
const seenSockets=new WeakSet();
const codes=new Set(['ETIMEDOUT','ECONNRESET','ECONNREFUSED','ENOTFOUND','EAI_AGAIN','UND_ERR_CONNECT_TIMEOUT','UND_ERR_HEADERS_TIMEOUT','UND_ERR_BODY_TIMEOUT','UND_ERR_SOCKET','ABORT_ERR']);
export function safeErrorCodes(error){
 const result=new Set(),seen=new Set(),queue=[error];
 for(let n=0;queue.length&&n<16;n++){
  const e=queue.shift();if(!e||typeof e!=='object'||seen.has(e))continue;seen.add(e);
  if(codes.has(e.code))result.add(e.code);
  if(e.cause)queue.push(e.cause);
  if(Array.isArray(e.errors))queue.push(...e.errors.slice(0,8));
 }
 return [...result].slice(0,8);
}
const events={
 'undici:client:beforeConnect':'connect_start_ms',
 'undici:client:connected':'connect_ready_ms',
 'undici:client:connectError':'connect_error_ms',
 'undici:request:create':'request_created_ms',
 'undici:client:sendHeaders':'headers_write_observed_ms',
 'undici:request:bodySent':'body_sent_observed_ms',
 'undici:request:headers':'response_headers_ms',
};
for(const [name,field] of Object.entries(events))channel(name).subscribe(message=>{
 const ctx=scope.getStore();if(!ctx||ctx.closed)return;
 try{
  ctx.data[field]??=Math.round((performance.now()-ctx.start)*1000)/1000;
  if(name==='undici:client:sendHeaders'&&message.socket){
   ctx.data.socket_seen_by_telegram_transport=seenSockets.has(message.socket);
   seenSockets.add(message.socket);
  }
  if(message.error)ctx.data.error_codes=safeErrorCodes(message.error);
 }catch{} // Diagnostics must never break delivery.
});
export async function withTelegramDiagnostics(operation,emit){
 if(typeof emit!=='function')return operation(()=>{},()=>{});
 const ctx={start:performance.now(),closed:false,data:{schema:1,phase:'FETCH_PENDING',socket_seen_by_telegram_transport:null}};
 return scope.run(ctx,async()=>{
  try{return await operation(phase=>{ctx.data.phase=phase;},error=>{ctx.data.error_codes=safeErrorCodes(error);});}
  finally{
   ctx.closed=true;ctx.data.elapsed_ms=Math.round((performance.now()-ctx.start)*1000)/1000;
   // Missing hooks are NOT evidence that no request bytes were sent.
   ctx.data.pre_send_failure_proven=false;
   try{emit(Object.freeze({...ctx.data}));}catch{}
  }
 });
}
