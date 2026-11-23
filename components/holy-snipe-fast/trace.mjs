// Observability only. No network calls, delivery decisions or production DB writes.
import fs from 'node:fs';
import path from 'node:path';
import {createHash,randomUUID} from 'node:crypto';
import {canonicalTrade} from './identity.mjs';
import {spawn} from 'node:child_process';
import {fileURLToPath} from 'node:url';
const fields=['transactionHash','asset','side','size','price','timestamp','proxyWallet','topic','type'];
const sha=x=>createHash('sha256').update(JSON.stringify(x)).digest('hex');
export function projection(payload,topic,type){
  const out={topic:typeof topic==='string'?topic:null,type:typeof type==='string'?type:null};
  for(const k of fields.slice(0,7)){
    const v=payload?.[k];out[k]=typeof v==='string'||typeof v==='number'||typeof v==='boolean'||v===null?v:null;
  }
  return out;
}
export function parseReason(payload,wallet,baseline,now){
  const valid=canonicalTrade(payload,wallet,0,Number.MAX_SAFE_INTEGER);
  if(!valid)return 'INVALID_PAYLOAD';
  if(valid.timestamp*1000<baseline)return 'STALE_AFTER_RECONNECT_OR_ENABLE_BOUNDARY';
  if(valid.timestamp*1000>now+5000)return 'SOURCE_TIMESTAMP_IN_FUTURE';
  return 'UNKNOWN_REASON';
}
export function collisionEvidence(previous,current){
  const differences=Object.fromEntries(fields.filter(k=>previous[k]!==current[k]).map(k=>[k,{previous:previous[k],current:current[k]}]));
  const economic=['transactionHash','asset','side','size','price','proxyWallet'];
  const normalized=k=>k==='size'||k==='price'?Number: k==='transactionHash'||k==='proxyWallet'?x=>String(x).toLowerCase():x=>x;
  const sameEconomic=economic.every(k=>normalized(k)(previous[k])===normalized(k)(current[k]));
  const sameTime=Number(previous.timestamp)===Number(current.timestamp);
  const cross=previous.topic==='activity'&&current.topic==='activity'&&new Set([previous.type,current.type]).size===2&&[previous.type,current.type].every(x=>['trades','orders_matched'].includes(x));
  // Payload equality is evidence of repeat representation, not on-chain log identity.
  const category=sameEconomic&&sameTime&&previous.topic===current.topic&&previous.type===current.type?'A':sameEconomic&&sameTime&&cross?'B':!sameEconomic?'C':'D';
  return {category,confidence:category==='C'?'SUSPECTED_DISTINCT_EXECUTIONS_NOT_PROVEN':category==='D'?'AMBIGUOUS':'SAME_PAYLOAD_REPRESENTATION_ONLY',differences,shadow_identity:sha(economic.map(k=>normalized(k)(current[k])).concat(Number(current.timestamp))),production_dedup_changed:false};
}
export function createTrace(root,Database,options={}){
  try{return openTrace(root,Database,options);}catch{
    console.error(JSON.stringify({alert:'TRACE_DISABLED',reason:'TRACE_INIT_FAILED',at_ms:Date.now(),telegram_failure:false}));
    let errors=1;return {begin(){errors++;return null;},stage(){errors++;},queue(){errors++;},delivery(){errors++;},close(){},snapshot(){return {errors,unknown_reasons:0,disabled:true,trace_enabled:false,trace_disabled:true,disable_reason:'TRACE_INIT_FAILED'};}};
  }
}
function openTrace(root,Database,{now=Date.now,minFreeBytes=12*1024**3,policy='OBSERVABILITY_ONLY_NO_RECOVERY_NO_DEDUP_CHANGE',maxFileBytes=256*1024**2,maintenance=minFreeBytes===12*1024**3,diskFree=null}={}){
  const dir=path.join(root,'trace-v1');fs.mkdirSync(dir,{recursive:true,mode:0o700});
  const run='trace-'+now()+'-'+randomUUID();let seq=0,errors=0,unknownReasons=0,records=0,bytes=0,fd=null,hour=null,db=null,lastDisk=0,disabled=false;
  const stages={};
  const freeBytes=diskFree??(()=>{const s=fs.statfsSync(dir);return s.bavail*s.bsize;});
  let segment=0,fileBytes=0,activeFile=null,storage=null,storageChild=null,lastStorage=0,disabledAt=null,disableReason=null,droppedRecords=0;
  const storageConfig=path.join(dir,'storage-config.json');
  let preserveBeforeMs=now();
  if(maintenance){
    if(fs.existsSync(storageConfig))preserveBeforeMs=JSON.parse(fs.readFileSync(storageConfig,'utf8')).preserve_before_ms;
    else fs.writeFileSync(storageConfig,JSON.stringify({version:1,preserve_before_ms:preserveBeforeMs}),{flag:'wx',mode:0o600});
  }
  function disable(reason){if(disabled)return;disabled=true;disabledAt=now();disableReason=reason;
    console.error(JSON.stringify({alert:'TRACE_DISABLED',reason,at_ms:disabledAt,telegram_failure:false}));
  }
  function upkeep(){
    const t=now();if(!maintenance||storageChild||t-lastStorage<60000)return;lastStorage=t;
    try{
      storage=JSON.parse(fs.readFileSync(path.join(dir,'storage-health.json'),'utf8'));
      if(!storage.budget_ok)disable('TRACE_STORAGE_BUDGET');
    }catch(e){if(e.code!=='ENOENT')fail();}
    try{
      storageChild=spawn('nice',['-n','19','ionice','-c','2','-n','7',process.execPath,fileURLToPath(new URL('./trace-storage.mjs',import.meta.url)),dir,activeFile??'',String(preserveBeforeMs)],{stdio:'ignore'});
      storageChild.on('error',()=>{fail();storageChild=null;});storageChild.on('exit',code=>{if(code)fail();storageChild=null;});storageChild.unref();
    }catch{fail();storageChild=null;}
  }
  const fail=()=>{errors++;};
  try{
    const file=path.join(dir,'links.sqlite');db=new Database(file);fs.chmodSync(file,0o600);
    db.pragma('journal_mode = WAL');db.pragma('busy_timeout = 100');
    db.exec('CREATE TABLE IF NOT EXISTS queue_origins(event_key TEXT,recipient TEXT,trace_id TEXT,fields_json TEXT,PRIMARY KEY(event_key,recipient))');
  }catch{fail();}
  function write(row){
    try{
      if(disabled){droppedRecords++;return;}
      const t=now();
      if(t-lastDisk>60000){lastDisk=t;if(freeBytes()<minFreeBytes){disable('DISK_FREE_RESERVE');fail();return;}}
      const next=new Date(t).toISOString().slice(0,13).replaceAll(':','-');
      if(next!==hour||fileBytes>=maxFileBytes){if(fd!==null)fs.closeSync(fd);if(next!==hour)segment=0;else segment++;hour=next;fileBytes=0;
        activeFile=run+'-'+hour+(segment?'-'+String(segment).padStart(4,'0'):'')+'.jsonl';fd=fs.openSync(path.join(dir,activeFile),'a',0o600);}
      const line=JSON.stringify({schema:1,run_id:run,at_ms:t,...row})+'\n';const b=Buffer.from(line);
      let offset=0;while(offset<b.length){const n=fs.writeSync(fd,b,offset,b.length-offset);if(n<=0)throw Error('short_write');offset+=n;}
      records++;bytes+=b.length;fileBytes+=b.length;
      if(row.stage){stages[row.stage]=(stages[row.stage]||0)+1;if(row.reason==='UNKNOWN_REASON')unknownReasons++;if(row.stage==='TRACE_ERROR')errors++;}
    }catch{fail();}
  }
  function stage(id,name,details={}){write({trace_id:id,stage:name,...details});}
  const api={
    begin(payload,topic,type,raw){
      try{const p=projection(payload,topic,type),id='trc:v1:'+sha([run,++seq,p]);
        stage(id,'RAW_RECEIVED',{received_at_ms:now(),fields:p,raw_sha256:sha(raw),shadow_identity:sha(p)});return id;
      }catch{fail();return null;}
    },stage,
    queue(id,key,recipient,p,created){
      try{
        const r=String(recipient),prior=db?.prepare('SELECT trace_id,fields_json FROM queue_origins WHERE event_key=? AND recipient=?').get(key,r);
        if(created){
          if(!db)throw Error('trace_links_unavailable');
          db.prepare('INSERT OR REPLACE INTO queue_origins VALUES(?,?,?,?)').run(key,r,id,JSON.stringify(p));
          stage(id,'DEDUP_NEW',{canonical_key:key,recipient:r});stage(id,'QUEUE_CREATED',{canonical_key:key,recipient:r});
        }else{
          stage(id,'DEDUP_SUPPRESSED',{reason:'DEDUP_SUPPRESSED',canonical_key:key,recipient:r,occupying_trace_id:prior?.trace_id??null,origin_evidence:prior?'TRACED':'PRE_TRACE_QUEUE_ORIGIN',collision:prior?collisionEvidence(JSON.parse(prior.fields_json),p):{category:'D',confidence:'PRE_TRACE_ORIGIN_FIELDS_UNAVAILABLE',differences:null}});
        }
      }catch{fail();stage(id,'TRACE_ERROR',{reason:'TRACE_QUEUE_LINK_FAILED',canonical_key:key});}
    },
    delivery(row,name,details={}){
      try{
        const origin=db?.prepare('SELECT trace_id FROM queue_origins WHERE event_key=? AND recipient=?').get(row.event_key,String(row.chat_id));
        stage(origin?.trace_id??null,name,{canonical_key:row.event_key,recipient:String(row.chat_id),origin_evidence:origin?'TRACED':'PRE_TRACE_QUEUE_ORIGIN',attempt:row.attempts+1,...details});
      }catch{fail();}
    },
    snapshot(){upkeep();return {run_id:run,directory:dir,started_ms:Number(run.split('-')[1]),records,bytes,errors,unknown_reasons:unknownReasons,disabled,
      trace_enabled:!disabled,trace_disabled:disabled,dropped_records:droppedRecords,disabled_at_ms:disabledAt,disable_reason:disableReason,storage,uptime_ms:(disabledAt??now())-Number(run.split('-')[1]),stages,policy};},
    close(){write({stage:'TRACE_SHUTDOWN',trace_id:null});try{if(fd!==null){fs.fsyncSync(fd);fs.closeSync(fd);fd=null;}db?.close();}catch{fail();}}
  };
  write({stage:'TRACE_START',trace_id:null,limitations:['Pre-start raw events unavailable unless independently retained','Sync append is not a power-loss fsync guarantee','A/B payload classification is not on-chain execution proof']});
  return api;
}
