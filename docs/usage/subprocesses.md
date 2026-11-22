# Subprocesses and pipes

```python
process = await asyncio.create_subprocess_exec(
    "cat", stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE
)
stdout, _ = await process.communicate(b"round trip")
```

`create_subprocess_exec` and `create_subprocess_shell` follow asyncio's public
contract, including `env`, `cwd`, `pass_fds`, `start_new_session`, `user`,
`group`, `extra_groups`, `umask`, `process_group`, `close_fds`,
`restore_signals`, `preexec_fn`, `pipesize`, `startupinfo` and `creationflags`
where the host platform supports them.

## How it is put together

On macOS and Windows, the child is spawned with `uv_spawn` by default and its
stdio runs over native pipe transports. Linux uses `subprocess.Popen` so its
race-free `close_fds` implementation preserves Python's descriptor-inheritance
contract; non-empty `pass_fds`, `user`, `group`, `extra_groups`, `umask`,
`process_group`, `close_fds=False`, `restore_signals=False`, `preexec_fn` and
`pipesize` also select that path on Unix, because libuv has no equivalent for
most of them. asyncio's own subprocess transport drives both
implementations, so protocol callbacks and exit bookkeeping stay identical.

On Windows, `startupinfo`, non-zero `creationflags` and `close_fds=False` select
`subprocess.Popen` too, because `uv_spawn` takes none of them.

On Windows, so does a program that is not an absolute path, such as `"git"` or
`"tools\\run.exe"`. `subprocess.Popen` leaves the search to `CreateProcess`,
which looks in the parent's directories and on the parent's `PATH`. libuv looks
in the child's `cwd` and on the `PATH` from `env`, so it could run a different
program, or find one where asyncio raises `FileNotFoundError`. An absolute path,
`sys.executable` included, still uses `uv_spawn`.

libuv reaps children it spawns and reports status through its exit callback.
The stdlib path uses one bounded, process-wide reaper thread for all children
rather than a thread per child.

On the libuv path, signals go through `uv_process_kill` rather than the pid. The
stdlib path delegates signalling to its `Popen` object.

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

/// warning | On Windows, a `close_fds=False` child can inherit the pipe

The duplicate is made with `os.dup()`. On Windows it is inheritable for an
instant, until Python clears the flag. When the pipe is standard input, output
or error, Python leaves it inheritable for good. A child spawned with
`close_fds=False` while the duplicate is inheritable gets the handle and keeps
the pipe open. CPython's own subprocess code has races of the same kind.

Keep the default `close_fds=True`. If you cannot, do not connect a pipe on one
thread while another thread spawns such a child, and expect every such child to
inherit a standard stream you have connected.
///
