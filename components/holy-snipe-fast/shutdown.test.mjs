import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {spawnSync} from 'node:child_process';
import {fileURLToPath} from 'node:url';
import {createFastShutdown} from './shutdown.mjs';

test('FAST refuses an empty owner list before preparing SQLite statements or contacting the bot',t=>{
  const root=fs.mkdtempSync(path.join(os.tmpdir(),'fast-owners-'));
  t.after(()=>fs.rmSync(root,{recursive:true,force:true}));
  const blockNetwork="import https from 'node:https'; https.request=()=>{throw Error('TEST_NETWORK_BLOCKED');};";
  const fast=fileURLToPath(new URL('./fast.mjs',import.meta.url));
  const result=spawnSync(process.execPath,['--import','data:text/javascript,'+encodeURIComponent(blockNetwork),fast],{
    env:{
      POLYSNIPE_FAST_ENABLE:'1',POLYSNIPE_FAST_STATE_DIR:root,TELEGRAM_BOT_TOKEN:'fixture',
      EXPECTED_TELEGRAM_USERNAME:'fixture_bot',OWNER_TELEGRAM_USER_IDS:' , '
    },
    encoding:'utf8',timeout:3000
  });
  assert.ifError(result.error);
  assert.equal(result.status,1);
  assert.match(result.stderr,/Error: owners_not_configured/);
  assert.deepEqual(fs.readdirSync(root),[]);
});

test('FAST shutdown clears the REDEEM interval and shares one cleanup across overlapping calls',async t=>{
  t.mock.timers.enable({apis:['setInterval']});
  let polls=0,stops=0,finish;
  const redeemPoller=setInterval(()=>{polls++;},10_000);
  const shutdown=createFastShutdown([redeemPoller],()=>{
    stops++;
    return new Promise(resolve=>{finish=resolve;});
  });
  t.mock.timers.tick(10_000);
  const first=shutdown();
  assert.equal(shutdown(),first);
  await Promise.resolve();
  t.mock.timers.tick(30_000);
  assert.equal(polls,1);
  assert.equal(stops,1);
  finish();
  await first;
  assert.equal(shutdown(),first);
});
