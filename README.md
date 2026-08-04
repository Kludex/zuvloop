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

## Performance

`python benchmarks/run.py`, on an M3 Max running macOS 26 and CPython 3.14, best of three:

| Benchmark | asyncio | uvloop | zuv | zuv / uvloop |
| --- | ---: | ---: | ---: | ---: |
| `call_soon` | 2.2M/s | 5.4M/s | **7.9M/s** | **1.48x** |
| `call_soon` with arguments | 2.3M/s | 3.7M/s | **6.0M/s** | **1.61x** |
| timer schedule + cancel | 1.5M/s | 2.0M/s | **11.9M/s** | **5.94x** |
| loop iterations (`sleep(0)`) | 72.7k/s | 79.1k/s | 78.8k/s | 1.00x |
| echo round trips, 1 KiB | 45.3k/s | 52.4k/s | **53.6k/s** | **1.02x** |
| bulk stream | 8.5 GiB/s | 8.7 GiB/s | **9.8 GiB/s** | **1.12x** |
| `getaddrinfo`, numeric host | 27.8k/s | 1.50M/s | 903k/s | 0.60x |

Scheduling and timers are where the design differs most: arguments live inside the handle rather
than in a tuple, and timers share one `uv_timer_t` behind a heap instead of taking a libuv handle
each. `getaddrinfo` is the one place uvloop is still ahead - it parses address literals itself,
while `zuv` hands them to libc with `AI_NUMERICHOST`, which is slower but cannot disagree with
`socket.getaddrinfo`. It is still 32x asyncio.

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

- **The GIL is released for the whole `uv_run` call.** Callbacks reacquire it through a thread
  state saved once by the loop, rather than `PyGILState_Ensure`, and a nested callback within the
  same batch pays nothing.
- **`call_soon(cb, a, b)` allocates no tuple.** Up to three arguments are stored inside the handle
  and passed straight to `PyObject_Vectorcall`.
- **Timers use one `uv_timer_t` and an internal heap**, rather than a libuv handle per timer.
  Cancellation is O(1) and the heap is compacted lazily, matching asyncio's scheduler semantics.
- **Writes try `uv_try_write` first**, and never copy. A socket with room in its send buffer
  completes the write without allocating anything; when a write has to be queued, the request holds
  a buffer view of the caller's memory rather than a copy of it.
- **Reads land directly in the `bytes` object** handed to `data_received`, so the kernel writes once
  and nothing is copied afterwards. `BufferedProtocol` goes one better and reads into the protocol's
  own buffer.
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

Because `zuv` schedules real `asyncio.Task` objects rather than its own, the 3.14 introspection
APIs work unchanged - `asyncio.all_tasks()`, `asyncio.current_task()`, `asyncio.capture_call_graph()`
and `asyncio.print_call_graph()` are all covered by the test suite. The same applies to the
out-of-process tooling, which reads those task objects straight out of process memory:

```console
$ python -m asyncio ps <pid>
$ python -m asyncio pstree <pid>
```

Those two need the platform's usual debugging privileges (on macOS, `sudo`) - a restriction that
applies to any event loop, `zuv` or not.

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
