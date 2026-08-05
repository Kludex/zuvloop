# Architecture

zuvloop is a CPython extension module written in Zig, with a small Python layer on
top. What lives where follows one rule: **native if it runs per callback or per
packet, Python if it runs per connection or per loop.**

| Layer | Where | Why |
| --- | --- | --- |
| Ready queue, timer heap, cross-thread inbox | `zig/loop.zig` | Once per callback |
| Callback handles | `zig/handle.zig` | Arguments stored inline, invoked through vectorcall |
| Stream and pipe reads and writes | `zig/transport.zig` | Once per packet |
| Datagram sends and receives | `zig/datagram.zig` | Once per datagram |
| Descriptor watchers | `zig/poller.zig` | One `uv_poll_t` per descriptor |
| Name resolution | `zig/dns.zig` | On libuv's threadpool, not the executor |
| Process spawning | `zig/process.zig` | libuv reaps the child, so no watcher is needed |
| Connection and server setup | `src/zuvloop/_connect.py` | Once per connection |
| Lifecycle, executors, error reporting | `src/zuvloop/_base.py` | Once per loop |
| OpenTelemetry emission | `src/zuvloop/_instrumentation.py` | The only file that imports OTel |

## The GIL

The GIL is released for the whole `uv_run` call. Callbacks reacquire it through a
thread state saved once by the loop, rather than `PyGILState_Ensure`, and a
nested callback within the same batch pays nothing.

## Scheduling

`call_soon(cb, a, b)` allocates no tuple. Up to three arguments live inside the
handle and are passed straight to `PyObject_Vectorcall`.

Timers share one `uv_timer_t` behind an internal heap rather than taking a libuv
handle each. Cancellation is O(1) and the heap is compacted lazily, which matches
asyncio's scheduler semantics rather than merely approximating them.

A batch runs exactly the callbacks queued when it started. asyncio guarantees
everything already scheduled runs even if one of them calls `stop()`, so the
batch is never cut short.

## Reads

Two paths, chosen by the traffic. Below 64 KiB the data is copied out of a shared
buffer into an exactly sized `bytes`; above it, libuv fills the final object
directly and nothing is copied. The threshold a transport is judged against
follows what the peer has been sending, doubling whenever a read fills the
buffer.

## Writes

Everything written during one turn goes out as a single vectored `uv_try_write`.
The flush runs from a **prepare handle**, which libuv runs before it computes the
poll timeout — from a check handle, a loop with nothing else to do would block
for I/O while still holding data the peer was waiting for.

Nothing is copied: when the socket cannot take it all, the queued request holds a
buffer view of the caller's memory.

## Name resolution

An address literal is answered by `inet_pton` without entering libc.
`getaddrinfo` costs around half a microsecond even under `AI_NUMERICHOST`, where
it has nothing to look up but still builds a chain, takes the resolver's locks
and has to be freed.

Everything the shortcut cannot answer identically falls back to libc: a scoped
address, whose zone only libc can resolve; a legacy form like `127.1` that
`inet_pton` rejects and `getaddrinfo` accepts; an unspecified socket type, which
libc answers with one entry per type. Real hostnames resolve on libuv's
threadpool, not the executor.

## Sockets

Sockets are created, bound and accepted in Python. Those happen once per
connection, so the readable implementation is worth more there than the last
microsecond — and it is where the ownership rules live.

/// note | Descriptor ownership

Once libuv adopts a descriptor it owns it. The Python socket that was holding it
is detached rather than closed, or Python would close a descriptor libuv has
already closed and possibly reused. Everything after adoption is written to be
infallible, so ownership cannot be split by a failure part-way through.
///
