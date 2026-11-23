import test from 'node:test';import assert from 'node:assert/strict';
import {createFrameBuffer,FRAME_BUFFER_DEFAULTS} from './frame-buffer.mjs';
function fixture(limits={}){let at=0;const rows=[],faults=[],b=createFrameBuffer({post:r=>rows.push(r),onFault:r=>faults.push(r),mono:()=>at,wall:()=>1800000000000+at,limits});return {b,rows,faults,step(ms){at+=ms;b.tick();}};}
for(const count of [64,65,128,512])test(`bounded queue accepts ${count} synchronous frames with one payload in flight`,()=>{
 const f=fixture();for(let i=0;i<count;i++)assert.equal(f.b.accept('frame-'+i),true);
 assert.equal(f.rows.filter(r=>r.kind==='frame').length,1);assert.equal(f.faults.length,0);assert.equal(f.b.snapshot().pending,count);
 for(let i=1;i<=count;i++){const row=f.rows.find(r=>r.kind==='frame'&&r.seq===i);assert.ok(row);assert.equal(row.data,'frame-'+(i-1));assert.equal(f.b.acknowledge({seq:i,status:'PIPELINE_COMPLETED',reason:'COMMON_PIPELINE_RETURNED'}),true);}
 assert.equal(f.b.snapshot().pending,0);assert.equal(f.b.snapshot().receipted,count);assert.equal(f.b.snapshot().max_buffer_frames,count);assert.equal(f.b.snapshot().failed,false);
});
test('a receipt, not a sent payload, frees buffer memory; native close drains queued data',()=>{
 const f=fixture();f.b.accept('first');f.b.accept('second');const n=f.b.snapshot().buffer_bytes;f.b.end();assert.equal(f.b.snapshot().buffer_bytes,n);assert.equal(f.rows.some(r=>r.kind==='buffer_drained'),false);f.b.acknowledge({seq:1,status:'PIPELINE_COMPLETED',reason:'COMMON_PIPELINE_RETURNED'});f.b.acknowledge({seq:2,status:'PIPELINE_COMPLETED',reason:'COMMON_PIPELINE_RETURNED'});assert.equal(f.b.snapshot().buffer_bytes,0);assert.equal(f.rows.filter(r=>r.kind==='buffer_drained').length,1);
});
test('byte cap, count cap, frame cap and malformed native input have distinct reasons',()=>{
 for(const [limits,values,reason] of [[{maxBytes:8,maxFrameBytes:8},['aaaaa','bbbbb'],'FRAME_QUEUE_BYTES'],[{maxFrames:2},['a','b','c'],'FRAME_QUEUE_COUNT'],[{maxFrameBytes:2},['abc'],'FRAME_TOO_LARGE'],[{},[new Uint8Array(2)],'FRAME_NON_TEXT']]){
  const f=fixture(limits);for(const x of values)f.b.accept(x);assert.deepEqual(f.faults,[reason]);assert.equal(f.b.snapshot().buffer_bytes,0);assert.equal(f.b.snapshot().failed,true);assert.ok(f.rows.some(r=>r.kind==='frame_observed'&&!r.accepted&&r.reason===reason));
 }
});
test('sustained overload records an explicit fence tail and stays within its memory bound',()=>{
 const f=fixture({maxFrames:8});for(let i=0;i<10000;i++)f.b.accept('x');const s=f.b.snapshot();assert.deepEqual(f.faults,['FRAME_QUEUE_COUNT']);assert.equal(s.accepted,8);assert.equal(s.rejected,1);assert.equal(s.tail_after_fence,9991);assert.equal(s.native_frames,s.accepted+s.rejected+s.tail_after_fence);assert.ok(s.max_buffer_bytes<=FRAME_BUFFER_DEFAULTS.maxBytes);assert.equal(s.max_buffer_frames,8);
});
test('queue-age timeout and in-flight receipt timeout remain distinct',()=>{
 const q=fixture({maxWaitMs:50,ackTimeoutMs:100});q.b.accept('a');q.b.accept('b');q.step(51);assert.deepEqual(q.faults,['FRAME_QUEUE_TIMEOUT']);
 const a=fixture({maxWaitMs:100,ackTimeoutMs:50});a.b.accept('a');a.step(51);assert.deepEqual(a.faults,['FRAME_RECEIPT_TIMEOUT']);
});
test('invalid receipts fail closed without releasing a different frame',()=>{const f=fixture();f.b.accept('a');assert.equal(f.b.acknowledge({seq:2,status:'PIPELINE_COMPLETED',reason:'COMMON_PIPELINE_RETURNED'}),false);assert.deepEqual(f.faults,['FRAME_RECEIPT_INVALID']);});
test('limits cannot silently expand beyond the reviewed defaults',()=>{for(const patch of [{maxBytes:FRAME_BUFFER_DEFAULTS.maxBytes+1},{maxFrames:4097},{maxWaitMs:16001},{extra:1},{maxWaitMs:0}])assert.throws(()=>fixture(patch),/LIMIT/);});
for(const delay of [2000,2001,4000])test(`receipt delayed ${delay}ms retains all 512 frames and drains exactly once`,()=>{
 const f=fixture();for(let i=0;i<512;i++)f.b.accept('frame-'+i);const bytes=f.b.snapshot().buffer_bytes;
 f.step(delay);assert.deepEqual(f.faults,[]);assert.equal(f.b.snapshot().pending,512);assert.equal(f.b.snapshot().buffer_bytes,bytes);assert.equal(f.b.snapshot().receipted,0);assert.equal(f.rows.filter(r=>r.kind==='frame').length,1);
 for(let seq=1;seq<=512;seq++)assert.equal(f.b.acknowledge({seq,status:'PIPELINE_COMPLETED',reason:'COMMON_PIPELINE_RETURNED'}),true);
 assert.equal(f.b.snapshot().receipted,512);assert.equal(f.b.snapshot().pending,0);assert.equal(f.b.snapshot().buffer_bytes,0);assert.equal(f.b.snapshot().receipt_recoveries,delay>2000?1:0);assert.equal(f.b.snapshot().max_receipt_ms,delay);
});
test('missing receipt has one warning, finite hard failure and explicit pending range',()=>{
 const f=fixture();f.b.accept('a');f.b.accept('b');f.step(2001);f.step(1999);assert.equal(f.rows.filter(r=>r.kind==='receipt_stalled').length,1);assert.equal(f.b.snapshot().pending,2);
 f.step(4001);assert.deepEqual(f.faults,['FRAME_RECEIPT_TIMEOUT']);const fault=f.rows.find(r=>r.kind==='buffer_fault');assert.equal(fault.first_pending,1);assert.equal(fault.last_announced,2);assert.equal(fault.stats.pending,2);assert.equal(f.b.acknowledge({seq:1,status:'PIPELINE_COMPLETED',reason:'COMMON_PIPELINE_RETURNED'}),false);assert.equal(f.b.snapshot().receipted,0);
});
test('receipt cannot evade hard deadline between watchdog ticks',()=>{
 let at=0;const faults=[],b=createFrameBuffer({post:()=>{},mono:()=>at,onFault:r=>faults.push(r)});b.accept('a');at=8001;assert.equal(b.acknowledge({seq:1,status:'PIPELINE_COMPLETED',reason:'COMMON_PIPELINE_RETURNED'}),false);assert.deepEqual(faults,['FRAME_RECEIPT_TIMEOUT']);
});
test('continued receipts cannot keep an old queued frame beyond 16 seconds',()=>{
 const f=fixture();for(let i=0;i<4;i++)f.b.accept('frame-'+i);
 for(const seq of [1,2]){f.step(7000);assert.equal(f.b.acknowledge({seq,status:'PIPELINE_COMPLETED',reason:'COMMON_PIPELINE_RETURNED'}),true);}
 f.step(2001);assert.deepEqual(f.faults,['FRAME_QUEUE_TIMEOUT']);const fault=f.rows.find(r=>r.kind==='buffer_fault');assert.equal(fault.stats.receipted,2);assert.equal(fault.stats.pending,2);
});
