// Inert on import. Subscribing observes existing Node events; it sends nothing.
import {channel} from 'node:diagnostics_channel';

const ERROR_NAMES=new Set(['Error','TypeError','RangeError','DOMException','AggregateError']);
const ERROR_CODES=new Set(['ECONNRESET','ECONNREFUSED','ECONNABORTED','ETIMEDOUT','EPIPE',
 'ENOTFOUND','EAI_AGAIN','ENETUNREACH','EHOSTUNREACH','UND_ERR_SOCKET','UND_ERR_CONNECT_TIMEOUT',
 'UND_ERR_HEADERS_TIMEOUT','UND_ERR_BODY_TIMEOUT','CERT_HAS_EXPIRED','UNABLE_TO_VERIFY_LEAF_SIGNATURE']);
export function safeSocketError(error){
 return {error_class:ERROR_NAMES.has(error?.name)?error.name:'OTHER',
  error_code:ERROR_CODES.has(error?.code)?error.code:ERROR_CODES.has(error?.cause?.code)?error.cause.code:'UNAVAILABLE'};
}
const subscribeNative=(name,listener)=>{
 const c=channel(name);c.subscribe(listener);return ()=>c.unsubscribe(listener);
};
export function attachNativeDiagnostics(onSignal,{subscribe=subscribeNative,onFailure=()=>{}}={}){
 const cleanups=[];let active=true;
 const dispose=()=>{active=false;for(const cleanup of cleanups.splice(0).reverse())cleanup();};
 try{
  for(const [name,kind] of [['ping','PING'],['pong','PONG'],['socket_error','SOCKET_ERROR']]){
   const listener=value=>{
    if(!active)return;
    try{
     // Undici 6 publishes control payload only, without socket identity. Never
     // read/store it, assign it to a generation, or infer heartbeat RTT from it.
     onSignal({kind,...(kind==='SOCKET_ERROR'?safeSocketError(value):{})});
    }catch{try{onFailure();}catch{/* Never throw into Node's diagnostics publisher. */}}
   };
   const cleanup=subscribe('undici:websocket:'+name,listener);
   if(typeof cleanup!=='function')throw Error('DIAGNOSTIC_SUBSCRIPTION_INVALID');
   cleanups.push(cleanup);
  }
 }catch{dispose();throw Error('PROTOCOL_DIAGNOSTICS_INIT_FAILED');}
 return dispose;
}
