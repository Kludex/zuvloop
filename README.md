# zuv

A [libuv](https://libuv.org) event loop for `asyncio`, written in [Zig](https://ziglang.org).

`zuv` replaces the asyncio event loop with one whose hot paths - callback scheduling, timers,
descriptor watching, name resolution and the stream data path - are implemented natively and
driven by libuv. It targets uvloop's performance while shipping type hints, a strict-mypy-clean
Python surface, and first-class [Logfire](https://logfire.pydantic.dev) instrumentation.

```python
import asyncio

import zuv


async def main() -> None:
    reader, writer = await asyncio.open_connection("example.com", 80)
    writer.write(b"GET / HTTP/1.0\r\nHost: example.com\r\n\r\n")
    await writer.drain()
    print(await reader.read(64))
    writer.close()
    await writer.wait_closed()


zuv.run(main())
```

Or hand the loop factory to asyncio directly:

```python
asyncio.run(main(), loop_factory=zuv.new_event_loop)
```

## Requirements

- Python 3.14 or newer
- Linux or macOS
- [Zig](https://ziglang.org/download/) 0.16 to build from source

## Design

| Layer | Where it lives | Why |
| --- | --- | --- |
| Ready queue, timer heap, cross-thread inbox | `zig/loop.zig` | Called once per callback |
| Callback handles | `zig/handle.zig` | Arguments stored inline, invoked through vectorcall |
| Stream reads and writes | `zig/transport.zig` | Called once per packet |
| Descriptor watchers | `zig/poller.zig` | One `uv_poll_t` per descriptor |
| Name resolution | `zig/dns.zig` | Runs on libuv's threadpool, not the executor |
| Connection and server setup | `src/zuv/_connect.py` | Called once per connection |
| Lifecycle, executors, error reporting | `src/zuv/_base.py` | Called once per loop |

A few decisions worth knowing about:

- **The GIL is released for the whole `uv_run` call** and reacquired per callback batch. Callbacks
  that need no Python work - buffer allocation, write completions with nothing waiting - never
  take it.
- **`call_soon(cb, a, b)` allocates no tuple.** Up to three arguments are stored inside the handle
  and passed straight to `PyObject_Vectorcall`.
- **Timers use one `uv_timer_t` and an internal heap**, rather than a libuv handle per timer.
  Cancellation is O(1) and the heap is compacted lazily, matching asyncio's scheduler semantics.
- **Writes try `uv_try_write` first.** A socket with room in its send buffer completes the write
  without allocating a request or copying.
- **Sockets are created, bound and accepted in Python.** Those happen once per connection, so the
  readable implementation is worth more there than the last microsecond.

## Instrumentation

Slow callbacks and unhandled exceptions are always reported through `logfire`. Because `zuv` depends
on `logfire-api`, this costs nothing until an application calls `logfire.configure()`.

```python
import logfire
import zuv

logfire.configure()


async def main() -> None:
    zuv.instrument()  # periodic loop gauges
    ...
```

Slow-callback reports carry the awaiting call graph, captured with `asyncio.format_call_graph()`
(new in 3.14), so you see *why* the callback was running rather than just its repr.

Because `zuv` schedules real `asyncio.Task` objects, the 3.14 out-of-process tooling works
unchanged:

```console
$ python -m asyncio ps <pid>
$ python -m asyncio pstree <pid>
```

## Not implemented yet

`create_datagram_endpoint`, `subprocess_exec`, `subprocess_shell`, `connect_read_pipe`,
`connect_write_pipe`, `sendfile` and `sock_sendfile` raise `NotImplementedError`.
`get_extra_info("socket")` is not provided - libuv owns the descriptor, so handing out a Python
socket object for it would risk a double close. `sockname`, `peername`, `family`, `type` and
`proto` are available.

Handles returned by `call_soon` and `call_later` implement the `asyncio.Handle` and
`asyncio.TimerHandle` interfaces but are not instances of those classes.

## Development

```console
$ uv venv --python 3.14
$ uv pip install -e . --group dev
$ uv run pytest
$ uv run mypy
$ uv run ruff check
$ uv run --group bench python benchmarks/run.py
```

The extension is rebuilt by `hatch_build.py` on every install. To rebuild in place:

```console
$ python scripts/build.py
```

## Vendored libuv

`vendor/libuv` is an unmodified upstream release tarball; see `vendor/README.md`. Update it with
`./vendor/update-libuv.sh <version>`.

## License

MIT
