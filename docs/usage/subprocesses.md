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

The child is spawned with `uv_spawn` and its stdio runs over native pipe
transports. asyncio's own subprocess transport still drives it: that transport
reaches for six things on a `subprocess.Popen` and the three pipe objects, so
presenting that surface over a libuv process handle replaces the spawn without
touching the protocol callbacks or the exit bookkeeping.

libuv reaps the child itself and reports the status through its exit callback,
so there is no child watcher - no thread per child, and no pidfd to poll.

Signals go through `uv_process_kill` rather than the pid. asyncio signals the raw
pid and swallows the lookup error, which can reach a process that merely
inherited a reaped pid; a handle can only signal the child it spawned.

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
