# Temporary bring-up diagnostic for the Windows port: reproduces the first
# pytest crash in stages, so the failing native call is named by the last
# stage that printed.
from __future__ import annotations

import faulthandler
import socket

faulthandler.enable()

from zuvloop import _zuvloop  # noqa: E402

print("stage 1: imported, libuv", _zuvloop.libuv_version(), flush=True)

loop = _zuvloop.Loop()
print("stage 2: Loop() constructed", flush=True)

a, b = socket.socketpair()
a.setblocking(False)
b.setblocking(False)
print("stage 3: socketpair", a.fileno(), b.fileno(), flush=True)

loop.add_reader(a.fileno(), print)
print("stage 4: add_reader", flush=True)

loop.add_writer(a.fileno(), print)
print("stage 5: add_writer", flush=True)

b.send(b"x")
loop.call_soon(loop.stop)
loop._run()
print("stage 6: ran the loop", flush=True)

loop.remove_reader(a.fileno())
loop.remove_writer(a.fileno())
print("stage 7: removed watchers", flush=True)

a.close()
b.close()
loop._close()
print("stage 8: closed", flush=True)

import zuvloop  # noqa: E402

full = zuvloop.new_event_loop()
print("stage 9: EventLoop constructed", flush=True)
try:
    infos = full.run_until_complete(full.getaddrinfo("127.0.0.1", 80, type=socket.SOCK_STREAM))
    print("stage 10: getaddrinfo", infos, flush=True)
finally:
    full.close()
print("stage 11: EventLoop closed", flush=True)
