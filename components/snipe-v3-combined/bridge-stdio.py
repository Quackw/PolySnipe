#!/usr/bin/python3
"""Fixed SSH forced command. Relays bytes only to one protected FAST socket.

Install only after separate approval; do not accept paths, flags or a shell.
"""
import os, pwd, selectors, socket, stat, sys
SOCKET='/var/lib/holy-snipe-fast/rtds-b-private/rtds-b-ingress.sock'
DIRECTORY='/var/lib/holy-snipe-fast/rtds-b-private'
MAX_BUFFER=600000

def checked_socket_path():
 d=os.lstat(DIRECTORY);s=os.lstat(SOCKET);uid=pwd.getpwnam('holysnipefast').pw_uid
 if os.geteuid()!=uid or not stat.S_ISDIR(d.st_mode) or d.st_uid!=uid or stat.S_IMODE(d.st_mode)&0o077:raise ValueError('PRIVATE_DIRECTORY_REQUIRED')
 if not stat.S_ISSOCK(s.st_mode) or s.st_uid!=uid or stat.S_IMODE(s.st_mode)!=0o600:raise ValueError('PRIVATE_SOCKET_REQUIRED')
 return SOCKET

def run():
 if len(sys.argv)!=1 or os.environ.get('SSH_ORIGINAL_COMMAND',''):return 1
 sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM);sock.settimeout(5);sock.connect(checked_socket_path());sock.setblocking(False)
 for fd in (0,1):os.set_blocking(fd,False)
 selector=selectors.DefaultSelector();up=bytearray();down=bytearray();stdin_open=True;socket_open=True
 try:
  while socket_open or down:
   selector.close();selector=selectors.DefaultSelector()
   if stdin_open and len(up)<MAX_BUFFER:selector.register(0,selectors.EVENT_READ,'stdin')
   if down:selector.register(1,selectors.EVENT_WRITE,'stdout')
   mask=(selectors.EVENT_READ if socket_open and len(down)<MAX_BUFFER else 0)|(selectors.EVENT_WRITE if up else 0)
   if mask:selector.register(sock,mask,'socket')
   events=selector.select(15)
   # A filtered wallet stream is legitimately quiet. SSH keepalives handle
   # network loss; EOF and socket errors below still close the relay.
   if not events:continue
   for key,mask in events:
    if key.data=='stdin':
     chunk=os.read(0,min(65536,MAX_BUFFER-len(up)))
     if chunk:up.extend(chunk)
     else:stdin_open=False
    elif key.data=='stdout':
     try:n=os.write(1,down);del down[:n]
     except BrokenPipeError:return 1
    else:
     if mask&selectors.EVENT_READ:
      chunk=sock.recv(min(65536,MAX_BUFFER-len(down)))
      if chunk:down.extend(chunk)
      else:socket_open=False
     if mask&selectors.EVENT_WRITE and up:
      n=sock.send(up);del up[:n]
   if not stdin_open and not up and socket_open:
    try:sock.shutdown(socket.SHUT_WR)
    except OSError:pass
  return 0
 finally:selector.close();sock.close()
if __name__=='__main__':
 try:raise SystemExit(run())
 except (OSError,ValueError,KeyError):raise SystemExit(1)
