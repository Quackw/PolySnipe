// Dedicated low-priority child: never compress/delete in the RTDS message handler.
import fs from 'node:fs';
import path from 'node:path';
import {pipeline} from 'node:stream/promises';
import {createGzip,createGunzip} from 'node:zlib';
import {createHash} from 'node:crypto';
import {Writable} from 'node:stream';
import {fileURLToPath} from 'node:url';
const GiB=1024**3;
async function digest(stream){const hash=createHash('sha256');await pipeline(stream,new Writable({write(b,e,cb){hash.update(b);cb();}}));return hash.digest('hex');}
function hourOf(name){const m=name.match(/-(\d{4}-\d\d-\d\dT\d\d)(?:-\d+)?\.jsonl(?:\.gz)?$/);return m?Date.parse(m[1]+':00:00Z'):null;}
function syncDirectory(dir){const fd=fs.openSync(dir,'r');try{fs.fsyncSync(fd);}finally{fs.closeSync(fd);}}
export async function maintainTrace(dir,{now=Date.now(),activeFile=null,preserveBeforeMs=now,retentionMs=48*3600000,maxBytes=12*GiB,minFreeBytes=8*GiB}={}){
 if(fs.realpathSync(dir)!==path.resolve(dir))throw Error('trace_path_guard');
 const lock=path.join(dir,'storage.lock');let fd;
 try{fd=fs.openSync(lock,'wx',0o600);fs.writeSync(fd,String(process.pid));}
 catch(e){
  if(e.code!=='EEXIST')throw e;
  const st=fs.lstatSync(lock);if(!st.isFile()||st.isSymbolicLink())throw Error('trace_lock_invalid');
  const pid=Number(fs.readFileSync(lock,'utf8'));
  if(Number.isSafeInteger(pid)&&pid>1){
   try{process.kill(pid,0);return {busy:true};}
   catch(error){if(error.code!=='ESRCH')return {busy:true};}
   fs.unlinkSync(lock);return maintainTrace(dir,{now,activeFile,preserveBeforeMs,retentionMs,maxBytes,minFreeBytes});
  }
  return {busy:true};
 }
 let compressed=0,pruned=0;const started=Date.now();
 try{
  let names=fs.readdirSync(dir).filter(n=>hourOf(n)!==null).sort();
  for(const name of names){
   const source=path.join(dir,name);if(fs.lstatSync(source).isSymbolicLink())throw Error('trace_symlink');
   if(name.endsWith('.gz')||name===activeFile||hourOf(name)+3600000>now-3600000)continue;
   const target=source+'.gz',tmp=target+'.partial';
   // At most one bounded rotated file's temporary output; preserve disk floor.
   const st=fs.statSync(source),disk=fs.statfsSync(dir);
   if(disk.bavail*disk.bsize<minFreeBytes+st.size+Math.ceil(st.size/100)+1024**2)continue;
   const originalHash=await digest(fs.createReadStream(source));
   if(!fs.existsSync(target)){
    // Old partials are retained for inspection rather than overwritten.
    await pipeline(fs.createReadStream(source),createGzip({level:1}),fs.createWriteStream(tmp,{flags:'wx',mode:0o600}));
    fs.chownSync(tmp,st.uid,st.gid);
    const h=await digest(fs.createReadStream(tmp).pipe(createGunzip()));
    if(h!==originalHash)throw Error('trace_archive_hash_mismatch');
    const out=fs.openSync(tmp,'r');try{fs.fsyncSync(out);}finally{fs.closeSync(out);}fs.renameSync(tmp,target);syncDirectory(dir);
   }else if(await digest(fs.createReadStream(target).pipe(createGunzip()))!==originalHash)throw Error('trace_existing_archive_mismatch');
   const after=fs.statSync(source);if(after.size!==st.size||after.mtimeMs!==st.mtimeMs)throw Error('trace_source_changed');
   fs.unlinkSync(source);syncDirectory(dir);compressed++;
  }
  names=fs.readdirSync(dir).filter(n=>hourOf(n)!==null);
  for(const name of names){
   const hour=hourOf(name);
   // Pre-rollout forensic evidence is pinned (compressed, never retention-deleted).
   if(name!==activeFile&&name.endsWith('.gz')&&hour>=preserveBeforeMs&&hour+3600000<now-retentionMs){fs.unlinkSync(path.join(dir,name));pruned++;}
  }
  names=fs.readdirSync(dir).filter(n=>hourOf(n)!==null);
  const bytes=names.reduce((n,f)=>n+fs.statSync(path.join(dir,f)).size,0),disk=fs.statfsSync(dir);
  const report={at_ms:Date.now(),duration_ms:Date.now()-started,compressed,pruned,bytes,files:names.length,retention_hours:retentionMs/3600000,
   max_bytes:maxBytes,min_free_bytes:minFreeBytes,free_bytes:disk.bavail*disk.bsize,budget_ok:bytes<=maxBytes,preserve_before_ms:preserveBeforeMs};
  const tmp=path.join(dir,'storage-health.json.tmp');fs.writeFileSync(tmp,JSON.stringify(report)+'\n',{mode:0o600});
  const owner=fs.statSync(dir);fs.chownSync(tmp,owner.uid,owner.gid);fs.renameSync(tmp,path.join(dir,'storage-health.json'));return report;
 }finally{fs.closeSync(fd);fs.unlinkSync(lock);}
}
if(process.argv[1]===fileURLToPath(import.meta.url)){
 const [dir,active,preserve]=process.argv.slice(2);
 maintainTrace(dir,{activeFile:active||null,preserveBeforeMs:Number(preserve)}).then(r=>console.log(JSON.stringify(r))).catch(()=>{console.error('TRACE_STORAGE_FAILED');process.exitCode=1;});
}
