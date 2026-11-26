// Optional Unix endpoint. Import is inert; start only with explicit enablement.
// SSH forced-command access must terminate at this one socket, never a shell.
import fs from 'node:fs';
import path from 'node:path';
import net from 'node:net';
import { createBBridgeReceiver } from '../snipe-v3-readiness/b-bridge.mjs';
import { serveBChannel } from '../snipe-v3-readiness/bridge-stream.mjs';
export function startFastBridge({
  directory,
  keyPath,
  runId,
  scopeFingerprint,
  owner,
  onState = () => {},
  getWallets = null,
  healthEnabled = false
}) {
  if (
    typeof directory !== 'string' ||
    !path.isAbsolute(directory) ||
    typeof keyPath !== 'string' ||
    !path.isAbsolute(keyPath) ||
    path.dirname(keyPath) !== directory ||
    path.basename(keyPath) !== 'bridge.key'
  )
    throw Error('BRIDGE_PATH_INVALID');
  const dir = fs.lstatSync(directory);
  if (!dir.isDirectory() || dir.isSymbolicLink() || dir.uid !== process.getuid() || (dir.mode & 0o077) !== 0)
    throw Error('BRIDGE_PRIVATE_DIRECTORY_REQUIRED');
  const fd = fs.openSync(keyPath, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  let key;
  try {
    const st = fs.fstatSync(fd);
    if (!st.isFile() || st.uid !== process.getuid() || (st.mode & 0o077) !== 0 || st.size !== 32)
      throw Error('BRIDGE_PRIVATE_KEY_REQUIRED');
    key = fs.readFileSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  const socketPath = path.join(directory, 'rtds-b-ingress.sock');
  if (fs.existsSync(socketPath)) {
    key.fill(0);
    throw Error('BRIDGE_SOCKET_ALREADY_EXISTS');
  }
  let channel = null,
    closed = false,
    ownInode = null;
  const receiver = createBBridgeReceiver({ runId, scopeFingerprint, key, owner });
  key.fill(0);
  const server = net.createServer((socket) => {
    if (closed || channel) {
      socket.destroy();
      return;
    }
    channel = serveBChannel({
      stream: socket,
      receiver,
      getWallets,
      healthEnabled,
      onState: (value) => onState(value === 'READY' ? 'B_AUTHENTICATED' : 'B_CHANNEL_CLOSED')
    });
    socket.once('close', () => {
      channel = null;
      onState('B_DISCONNECTED');
    });
    onState('B_CHANNEL_ACCEPTED');
  });
  server.on('error', () => onState('B_CHANNEL_ERROR'));
  const ready = new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(socketPath, () => {
      try {
        fs.chmodSync(socketPath, 0o600);
        const st = fs.lstatSync(socketPath);
        if (!st.isSocket() || st.uid !== process.getuid()) throw Error('BRIDGE_SOCKET_INVALID');
        ownInode = st.ino;
        server.removeListener('error', reject);
        onState('B_CHANNEL_READY');
        resolve();
      } catch (e) {
        server.close();
        reject(e);
      }
    });
  });
  function close() {
    if (closed) return;
    closed = true;
    channel?.close();
    receiver.close();
    server.close();
    try {
      const st = fs.lstatSync(socketPath);
      if (st.isSocket() && st.ino === ownInode && st.uid === process.getuid()) fs.unlinkSync(socketPath);
    } catch (e) {
      if (e.code !== 'ENOENT') throw e;
    }
    onState('B_CHANNEL_STOPPED');
  }
  return Object.freeze({
    ready,
    close,
    syncScope: (wallets) => channel?.syncScope(wallets) ?? false,
    health: () => channel?.status().peer_health ?? { state: 'UNKNOWN', reason: 'B_NOT_CONNECTED' }
  });
}
