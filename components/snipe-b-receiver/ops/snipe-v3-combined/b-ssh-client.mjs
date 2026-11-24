// Builds only a fixed-argument SSH invocation. No shell, credentials, connection
// or subprocess is created until start() is explicitly called by an operator.
import fs from 'node:fs';
import path from 'node:path';
import { spawn as spawnDefault } from 'node:child_process';
import { createBBridgeProducer } from '../snipe-v3-readiness/b-bridge.mjs';
import { createBRelay } from '../snipe-v3-readiness/bridge-stream.mjs';
function privateFile(file) {
  if (typeof file !== 'string' || !path.isAbsolute(file)) throw Error('SSH_PRIVATE_FILE_REQUIRED');
  const s = fs.lstatSync(file);
  if (!s.isFile() || s.isSymbolicLink() || s.uid !== process.getuid() || (s.mode & 0o077) !== 0)
    throw Error('SSH_PRIVATE_FILE_REQUIRED');
}
export function prepareBSSH({
  host,
  user,
  port = 22,
  identityFile,
  knownHostsFile,
  runId,
  scopeFingerprint,
  key,
  wallets,
  onScope = () => {},
  getHealth = null,
  spawn = spawnDefault
}) {
  if (
    typeof host !== 'string' ||
    !/^[a-z0-9][a-z0-9.-]{0,252}$/i.test(host) ||
    host.includes('..') ||
    typeof user !== 'string' ||
    !/^[a-z_][a-z0-9_-]{0,31}$/.test(user) ||
    !Number.isSafeInteger(port) ||
    port < 1 ||
    port > 65535 ||
    typeof spawn !== 'function'
  )
    throw Error('SSH_TARGET_INVALID');
  privateFile(identityFile);
  privateFile(knownHostsFile);
  const args = [
    '-F',
    '/dev/null',
    '-T',
    '-4',
    '-i',
    identityFile,
    '-p',
    String(port),
    '-l',
    user,
    '-o',
    'BatchMode=yes',
    '-o',
    'IdentitiesOnly=yes',
    '-o',
    'IdentityAgent=none',
    '-o',
    'StrictHostKeyChecking=yes',
    '-o',
    'UserKnownHostsFile=' + knownHostsFile,
    '-o',
    'GlobalKnownHostsFile=/dev/null',
    '-o',
    'PasswordAuthentication=no',
    '-o',
    'KbdInteractiveAuthentication=no',
    '-o',
    'ClearAllForwardings=yes',
    '-o',
    'PermitLocalCommand=no',
    '-o',
    'ControlMaster=no',
    '-o',
    'ControlPath=none',
    '-o',
    'ConnectionAttempts=1',
    '-o',
    'ConnectTimeout=8',
    '-o',
    'ServerAliveInterval=5',
    '-o',
    'ServerAliveCountMax=2',
    '--',
    host
  ];
  const secret = Buffer.from(key);
  if (secret.length !== 32) throw Error('SSH_BRIDGE_KEY_INVALID');
  let active = null;
  let closed = false;

  function start({ onScope: applyScope = onScope } = {}) {
    if (closed) throw Error('SSH_CLOSED');
    if (active) throw Error('SSH_SESSION_STILL_ACTIVE');
    const producer = createBBridgeProducer({ runId, scopeFingerprint, key: secret });
    let child,
      relay,
      exited = false,
      terminateTimer;

    function destroy() {
      child.stdin?.destroy();
      child.stdout?.destroy();
      if (exited || terminateTimer) return;
      child.kill('SIGTERM');
      terminateTimer = setTimeout(() => {
        if (!exited) child.kill('SIGKILL');
      }, 1000);
      terminateTimer.unref();
    }

    function done(reason) {
      exited = true;
      clearTimeout(terminateTimer);
      if (active?.child === child) active = null;
      relay?.close(reason);
    }

    try {
      child = spawn('/usr/bin/ssh', args, {
        shell: false,
        stdio: ['pipe', 'pipe', 'ignore'],
        env: { PATH: '/usr/bin:/bin', HOME: '/nonexistent' }
      });
      active = { child, close: () => relay?.close() };
      child.once('error', () => {
        if (child.pid) relay?.close('SSH_PROCESS_ERROR');
        else done('SSH_START_FAILED');
      });
      child.once('exit', (code, signal) =>
        done(signal ? 'SSH_PROCESS_SIGNAL' : code === 0 ? 'SSH_PROCESS_EOF' : 'SSH_PROCESS_EXIT_ERROR')
      );
      if (!child.stdin || !child.stdout) throw Error('SSH_STREAM_INVALID');
      const stream = {
        write: (data) => child.stdin.write(data),
        on(...args) {
          child.stdout.on(...args);
          return this;
        },
        removeListener(...args) {
          child.stdout.removeListener(...args);
          return this;
        },
        destroy
      };
      relay = createBRelay({ stream, producer, wallets, onScope: applyScope, getHealth });
      child.stdin.on('error', () => relay.close('SSH_STDIN_ERROR'));
      return relay;
    } catch (error) {
      producer.close();
      if (child)
        try {
          destroy();
        } catch {}
      throw error;
    }
  }

  return Object.freeze({
    start,
    close() {
      closed = true;
      secret.fill(0);
      active?.close();
    },
    description: { forced_command_required: true, pinned_host_required: true, network_started: false }
  });
}
