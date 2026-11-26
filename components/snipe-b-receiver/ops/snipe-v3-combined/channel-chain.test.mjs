import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { startFastBridge } from '../../../snipe-v3-combined/fast-bridge-server.mjs';
import { prepareBSSH } from './b-ssh-client.mjs';
const fp = 'a'.repeat(64),
  runId = 'channel-chain-test',
  key = Buffer.alloc(32, 19),
  wallet = '0x' + '1'.repeat(40);
const helper = String.raw`import sys,os,importlib.util
spec=importlib.util.spec_from_file_location('h',sys.argv[1]);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
m.DIRECTORY=sys.argv[2];m.SOCKET=os.path.join(m.DIRECTORY,'rtds-b-ingress.sock')
import types
m.pwd.getpwnam=lambda name:types.SimpleNamespace(pw_uid=os.geteuid())
sys.argv=['bridge'];sys.exit(m.run())`;
async function waitReady(relay) {
  for (let i = 0; i < 500 && relay.status().state !== 'READY'; i++)
    await new Promise((r) => setTimeout(r, 2));
  assert.equal(relay.status().state, 'READY');
}
test('SSH stdio channel survives a quiet interval longer than 15s and still closes on EOF', async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'b-chain-'));
  fs.chmodSync(dir, 0o700);
  for (const name of ['identity', 'known_hosts'])
    fs.writeFileSync(path.join(dir, name), 'fixture', { mode: 0o600 });
  fs.writeFileSync(path.join(dir, 'bridge.key'), key, { mode: 0o600 });
  let active = false,
    count = 0,
    server,
    relay;
  const owner = {
    openSource() {
      active = true;
      return { status: 'CONNECTED', scope_fingerprint: fp };
    },
    submit() {
      assert.equal(active, true);
      count++;
      return { status: 'ACCEPTED', results: [] };
    },
    disconnectSource() {
      active = false;
    }
  };
  try {
    server = startFastBridge({
      directory: dir,
      keyPath: path.join(dir, 'bridge.key'),
      runId,
      scopeFingerprint: fp,
      owner
    });
    await server.ready;
    const prepared = prepareBSSH({
      host: 'fast.internal',
      user: 'root',
      identityFile: path.join(dir, 'identity'),
      knownHostsFile: path.join(dir, 'known_hosts'),
      runId,
      scopeFingerprint: fp,
      key,
      wallets: [wallet],
      spawn: (_exe, args, options) => {
        assert.ok(args.includes('StrictHostKeyChecking=yes'));
        return spawn(
          '/usr/bin/python3',
          ['-I', '-c', helper, path.resolve('components/snipe-v3-combined/bridge-stdio.py'), dir],
          options
        );
      }
    });
    relay = prepared.start();
    await waitReady(relay);
    await new Promise((r) => setTimeout(r, 16000));
    assert.equal(relay.status().state, 'READY');
    const result = await relay.enqueue({
      topic: 'activity',
      type: 'trades',
      payload: { proxyWallet: wallet, title: 'Тест 🚀' }
    });
    assert.equal(result.status, 'QUEUED_OR_DEDUPLICATED');
    assert.equal(count, 1);
    server.close();
    for (let i = 0; i < 100 && relay.status().state !== 'CLOSED'; i++)
      await new Promise((r) => setTimeout(r, 10));
    assert.equal(relay.status().state, 'CLOSED');
  } finally {
    relay?.close();
    server?.close();
    fs.rmSync(dir, { recursive: true, force: true });
  }
});
