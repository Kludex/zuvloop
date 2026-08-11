# Networking

zuvloop implements the whole of asyncio's networking surface. There is no zuvloop API
here — these are asyncio's methods, and this page records what zuvloop does
underneath and where it is worth knowing.

## Connections

```python
transport, protocol = await loop.create_connection(Protocol, "example.com", 443, ssl=True)
```

The socket is created, resolved and connected in Python; libuv takes the
descriptor from there and owns every read and write. That split is deliberate: a
connection is established once, and the readable implementation is worth more
there than the last microsecond.

`create_unix_connection` works the same way over `AF_UNIX`.

## Servers

```python
server = await loop.create_server(Protocol, "127.0.0.1", 8080)
async with server:
    await server.serve_forever()
```

[`zuvloop.Server`](../reference/api.md#zuvloopserver) implements `asyncio.AbstractServer`, including
`close_clients()` and `abort_clients()`.

`create_unix_server` accepts `cleanup_socket`, and honours it the way asyncio
does: the socket file is unlinked at close only if it is still the same file that
was bound, compared by device and inode.

## Reading

Reads land in a `bytes` object sized to what the peer has been sending. Below
64 KiB the data is copied out of a shared buffer into an exactly sized object —
an HTTP request is a couple of hundred bytes, and allocating a large object to
shrink it again costs more than the copy. Above that, libuv fills the final
object directly and nothing is copied.

`BufferedProtocol` is supported and goes one better: libuv reads straight into
the buffer your protocol hands out.

```python
class Protocol(asyncio.BufferedProtocol):
    def get_buffer(self, sizehint: int) -> memoryview:
        return self._buffer

    def buffer_updated(self, nbytes: int) -> None: ...
```

## Writing

Writes issued during one turn of the loop are sent together, as a single
vectored write. This matters more than it sounds: ASGI and aiohttp both send a
response as a header write followed by a body write, so a loop that writes each
piece as it arrives spends two syscalls on every response.

```python
transport.write(headers)  # both go out
transport.write(body)     # in one writev
```

Nothing is copied. A write the socket accepts outright never allocates; when it
cannot take everything, the queued request holds a buffer view of your memory
rather than a copy of it.

/// note | What `get_write_buffer_size()` reports

Bytes still waiting in the batch count towards the queue, so
`get_write_buffer_size()` is non-zero between a `write()` and the end of the
turn. They are not counted as *backed up*, though: flow control triggers on what
the socket refused, not on what has not been offered to it yet.
///

## TLS

```python
transport, protocol = await loop.create_connection(Protocol, host, 443, ssl=context)
```

TLS runs through asyncio's own `SSLProtocol`, over a native transport. `start_tls`
works too, including the case where it swaps a plain protocol for a
`BufferedProtocol` — the transport re-reads which one it has rather than assuming.

## Descriptor watching

`add_reader`, `add_writer` and their `remove_` counterparts are implemented with
one `uv_poll_t` per descriptor, and the `sock_*` family
(`sock_recv`, `sock_sendall`, `sock_connect`, `sock_accept`, …) is built on them.

## Sending files

`sock_sendfile` and `sendfile` transfer file contents with the `sendfile(2)`
system call — the kernel moves the bytes, nothing is copied through Python. For
`sendfile(transport, ...)` the loop first lets the transport's buffered writes
drain and pauses reading, so the file cannot reorder around data written before
it. Targets the syscall cannot serve — a `BytesIO`, a TLS transport, a pipe —
fall back to a read-and-write loop. With `fallback=False` they raise instead:
`RuntimeError` where the transport itself rules the syscall out, as a TLS
transport does, and `SendfileNotAvailableError` everywhere else.
