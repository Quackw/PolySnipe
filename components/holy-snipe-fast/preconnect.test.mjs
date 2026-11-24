import test from 'node:test';
import assert from 'node:assert/strict';
import https from 'node:https';
import tls from 'node:tls';
import { PassThrough } from 'node:stream';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { execFileSync } from 'node:child_process';
import { createPreconnectAgent } from './preconnect-agent.mjs';
import { createTelegramHTTPSFetch } from './telegram-https.mjs';
import { telegramRequest, deliveryDecision } from './telegram-transport.mjs';

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'snipe-preconnect-'));
process.on('exit', () => fs.rmSync(dir, { recursive: true, force: true }));
execFileSync(
  'openssl',
  [
    'req',
    '-x509',
    '-newkey',
    'rsa:2048',
    '-nodes',
    '-keyout',
    dir + '/key',
    '-out',
    dir + '/cert',
    '-days',
    '1',
    '-subj',
    '/CN=localhost',
    '-addext',
    'subjectAltName=DNS:localhost'
  ],
  { stdio: 'ignore' }
);
const key = fs.readFileSync(dir + '/key'),
  cert = fs.readFileSync(dir + '/cert');
async function setup(t, mode = 'success', verify = true) {
  let posts = 0,
    connections = 0;
  const events = [];
  const server = https.createServer({ key, cert }, (req, res) => {
    req.resume();
    req.on('end', () => {
      posts++;
      if (mode === 'response_lost') {
        res.socket.destroy();
        return;
      }
      res.end(JSON.stringify({ ok: true, result: { message_id: 5 } }));
    });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const connect = (options) => {
    connections++;
    if (mode === 'first_fails' && connections === 1) {
      const s = new PassThrough();
      queueMicrotask(() => s.destroy(Object.assign(new Error('timeout'), { code: 'ETIMEDOUT' })));
      return s;
    }
    if (mode === 'stuck' || (mode === 'late_success' && connections === 1)) {
      return new PassThrough();
    }
    return tls.connect({
      ...options,
      host: '127.0.0.1',
      port: server.address().port,
      servername: 'localhost',
      ...(verify ? { ca: cert } : {})
    });
  };
  const agent = createPreconnectAgent({ connect, attemptTimeoutMs: 80, onAttempt: (e) => events.push(e) });
  t.after(() => {
    agent.destroy();
    server.closeAllConnections();
    server.close();
  });
  const fetchImpl = createTelegramHTTPSFetch({
    agent,
    requestImpl: (url, opts, cb) =>
      https.request(
        'https://localhost:' + server.address().port,
        { ...opts, servername: 'localhost', ...(verify ? { ca: cert } : {}) },
        cb
      )
  });
  const send = (timeoutMs = 1000) =>
    telegramRequest('test', 'sendMessage', { chat_id: 1, text: 'fixture' }, { fetchImpl, timeoutMs });
  return {
    send,
    get posts() {
      return posts;
    },
    get connections() {
      return connections;
    },
    events
  };
}
test('one TLS failure then verified connection sends exactly one POST', async (t) => {
  const x = await setup(t, 'first_fails');
  assert.equal((await x.send()).kind, 'OK');
  assert.equal(x.connections, 2);
  assert.equal(x.posts, 1);
  assert.deepEqual(
    x.events.map((e) => e.result),
    ['failed', 'ready']
  );
});
test('response loss after POST remains UNKNOWN and never replays', async (t) => {
  const x = await setup(t, 'response_lost');
  const r = await x.send();
  assert.equal(r.kind, 'AMBIGUOUS');
  assert.equal(x.posts, 1);
  assert.equal(x.connections, 1);
  assert.equal(deliveryDecision(r, { attempts: 0 }, 1).state, 'DELIVERY_UNKNOWN');
  assert.equal(deliveryDecision(r, { attempts: 0 }, 1).next, 0);
});
test('untrusted TLS certificate fails closed without retries or POST', async (t) => {
  const x = await setup(t, 'success', false);
  assert.equal((await x.send()).kind, 'AMBIGUOUS');
  assert.equal(x.connections, 1);
  assert.equal(x.posts, 0);
});
test('bounded stalled handshakes yield UNKNOWN without POST', async (t) => {
  const x = await setup(t, 'stuck');
  const r = await x.send();
  assert.equal(r.kind, 'AMBIGUOUS');
  assert.equal(x.connections, 2);
  assert.equal(x.posts, 0);
});
test('caller deadline during handshake does not send later', async (t) => {
  const x = await setup(t, 'stuck');
  assert.equal((await x.send(20)).kind, 'AMBIGUOUS');
  await new Promise((r) => setTimeout(r, 220));
  assert.equal(x.posts, 0);
});
test('verified connection arriving after caller deadline cannot send the abandoned POST', async (t) => {
  const x = await setup(t, 'late_success');
  assert.equal((await x.send(20)).kind, 'AMBIGUOUS');
  await new Promise((r) => setTimeout(r, 220));
  assert.equal(x.connections, 2);
  assert.equal(x.posts, 0);
});
test('verified socket is reused for subsequent distinct actions', async (t) => {
  const x = await setup(t);
  assert.equal((await x.send()).kind, 'OK');
  assert.equal((await x.send()).kind, 'OK');
  assert.equal(x.posts, 2);
  assert.equal(x.connections, 1);
});
