# Subprocesses and pipes

```python
process = await asyncio.create_subprocess_exec(
    "cat", stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE
)
stdout, _ = await process.communicate(b"round trip")
```

`create_subprocess_exec` and `create_subprocess_shell` work as they do on any
asyncio loop, with every `Popen` keyword — `env`, `cwd`, `pass_fds`,
`start_new_session` and the rest.

## How it is put together

The process is driven by asyncio's own subprocess transport. What that transport
needs from a loop is `connect_read_pipe` and `connect_write_pipe`, and those are
native here, so the **stdio data path is zuvloop's while the spawn is CPython's**.

That is the same trade as sockets: a process is started once, so the tested
implementation is worth more there than owning the fork. The bytes, which are not
once, go through the native transport.

Child reaping uses a pidfd where the kernel has one, and a thread per child
otherwise.

## Pipes

```python
read_transport, protocol = await loop.connect_read_pipe(Protocol, pipe)
write_transport, protocol = await loop.connect_write_pipe(Protocol, pipe)
```

Pipes reuse the stream transport — libuv drives FIFOs through the same handle it
drives Unix sockets through. A write pipe never starts a read on its descriptor,
because a write-only descriptor cannot be read and trying would fail the
transport rather than simply deliver nothing.

The descriptor is checked the way asyncio checks it: FIFOs, sockets and character
devices are accepted, and anything else — a regular file, say — raises
`ValueError`.

/// note | Who closes what

The transport owns the pipe you hand it, and closes it. Internally the descriptor
is duplicated before libuv adopts it, which keeps the two owners apart: libuv
closes its own descriptor, the transport closes your pipe object, and neither can
close the other's.
///
