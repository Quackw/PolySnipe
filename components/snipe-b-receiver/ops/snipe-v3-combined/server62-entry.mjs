// Explicit operator-only server-62 entry. Importing is inert; no installation or
// connection is performed by generating this file or its package.
import fs from 'node:fs';import path from 'node:path';import {createHash} from 'node:crypto';import {performance} from 'node:perf_hooks';import {pathToFileURL} from 'node:url';
import {normalizePairs,scopeFingerprint} from '../snipe-dual-rtds-ingress/scope.mjs';import {prepareBSSH} from './b-ssh-client.mjs';import {createRemoteB} from './b-receiver.mjs';
import {createBJournal} from './b-diagnostics.mjs';
import {openBCaptureStore} from './b-capture-store.mjs';
import {ManagedRtdsSocket} from '../snipe-rtds-resilient/managed-socket.mjs';
const ROOT=process.env.POLYSNIPE_B_STATE_DIR??'';
function bytes(name,max){
 const file=path.join(ROOT,name),fd=fs.openSync(file,fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW);
 try{const st=fs.fstatSync(fd);if(!st.isFile()||st.uid!==process.getuid()||(st.mode&0o077)!==0||st.size<1||st.size>max)throw Error('B_PRIVATE_FILE_REQUIRED');return fs.readFileSync(fd);}finally{fs.closeSync(fd);}
}
export async function runServer62B(){
 if(process.env.POLYSNIPE_B_ENABLE!=='1'||process.argv.length!==2)throw Error('B_OPERATOR_ENABLE_REQUIRED');
 if(!path.isAbsolute(ROOT))throw Error('B_STATE_DIRECTORY_REQUIRED');
 const directory=fs.lstatSync(ROOT);if(!directory.isDirectory()||directory.isSymbolicLink()||directory.uid!==process.getuid()||(directory.mode&0o077)!==0)throw Error('B_PRIVATE_DIRECTORY_REQUIRED');
 const cfg=JSON.parse(bytes('config.json',16384)),scopeBytes=bytes('frozen-scope.json',1048576),scopeDoc=JSON.parse(scopeBytes),scope=normalizePairs(scopeDoc.scope,{exact14:false});
 const scopeSha=createHash('sha256').update(scopeBytes).digest('hex'),fingerprint=scopeFingerprint(scope,{exact14:false});
 if(!scope.length||cfg.scope_sha256!==scopeSha||cfg.scope_fingerprint!==fingerprint||scopeDoc.fingerprint!==fingerprint)throw Error('B_SCOPE_MISMATCH');
 const key=bytes('bridge.key',32);if(key.length!==32)throw Error('B_KEY_INVALID');
 const journal=createBJournal({directory:path.join(ROOT,'diagnostics'),runId:cfg.run_id});await journal.start();
 if(journal.snapshot().disabled)console.error('B_DIAGNOSTICS_DEGRADED');
 const peerHealth=()=>{const status=source?.status(),diag=journal.snapshot();return {...status?.peer??{phase:'PREPARED',generation:0,activity_frames:0,last_valid_activity_age_ms:null,stop_reason:null},journal_ok:diag.started&&!diag.disabled&&diag.errors===0&&diag.dropped===0&&status?.capture.failures===0&&status?.channel==='READY'};};
 let source,ssh,capture;
 try{
  capture=openBCaptureStore({directory:ROOT,runId:cfg.run_id,scopeFingerprint:fingerprint});
  ssh=prepareBSSH({host:cfg.fast_host,user:cfg.ssh_user,port:cfg.ssh_port,identityFile:path.join(ROOT,'identity'),knownHostsFile:path.join(ROOT,'known_hosts'),runId:cfg.run_id,scopeFingerprint:fingerprint,key,wallets:[...new Set(scope.map(p=>p.wallet))],getHealth:peerHealth});
 }catch(error){key.fill(0);capture?.close();await journal.close();throw error;}
 let timer,publish=()=>{};source=createRemoteB({scope,ssh,capture,continuous:true,mono:()=>performance.now(),socketFactory:url=>new ManagedRtdsSocket(url,{onDiagnostic:row=>journal.record({...row,kind:'B_NATIVE_SIGNAL',at_ms:Date.now(),mono_ms:performance.now(),lifecycle_kind:row.kind,correlation:'PROCESS_ONLY_GENERATION_UNKNOWN'})}),onDiagnostic:row=>journal.record(row),onState:row=>{
  if(row.state==='STOPPED'){clearInterval(timer);try{publish();}catch{}void new Promise(resolve=>setImmediate(resolve)).then(()=>journal.close()).then(()=>{try{publish();}catch{}}).finally(()=>capture.close());process.exitCode=1;console.log(JSON.stringify({kind:'B_STOPPED',reason:row.reason,gaps:row.gaps,matched:row.matched,attention:row.attention,stale:row.stale}));}
 }});
 try{await source.start();key.fill(0);let ticks=0;
  publish=()=>{const value={run_id:cfg.run_id,at_ms:Date.now(),initial_scope_pairs:scope.length,...source.status(),diagnostics:{...journal.snapshot(),native_hooks_location:'PER_SOCKET_WORKER'},telegram_enabled:false,rest_enabled:false},tmp=path.join(ROOT,'status.'+process.pid+'.tmp'),dst=path.join(ROOT,'status.json');const fd=fs.openSync(tmp,fs.constants.O_WRONLY|fs.constants.O_CREAT|fs.constants.O_TRUNC|fs.constants.O_NOFOLLOW,0o600);try{fs.writeFileSync(fd,JSON.stringify(value)+'\n');}finally{fs.closeSync(fd);}fs.renameSync(tmp,dst);};
  publish();timer=setInterval(()=>{source.tick();if(++ticks%20===0)try{publish();}catch{source.stop('B_STATUS_WRITE_FAILED');}},250);console.log(JSON.stringify({kind:'B_RUNNING',scope_pairs:scope.length,telegram_enabled:false,rest_enabled:false}));
  for(const signal of ['SIGTERM','SIGINT'])process.once(signal,()=>{journal.record({kind:'B_PROCESS_SIGNAL',at_ms:Date.now(),type:signal});source.stop('OPERATOR_STOP');});
 }catch(error){key.fill(0);source.stop('B_START_FAILED');throw error;}
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href)runServer62B().catch(()=>{console.error('B_START_FAILED');process.exitCode=1;});
