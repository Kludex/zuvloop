# zuv

A [libuv](https://libuv.org) event loop for `asyncio`, written in [Zig](https://ziglang.org).

`zuv` replaces the asyncio event loop with one whose hot paths - callback scheduling, timers,
descriptor watching, name resolution and the stream data path - are implemented natively and
driven by libuv. It targets uvloop's performance while shipping type hints, a strict-mypy-clean
Python surface, and OpenTelemetry instrumentation.

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

`python benchmarks/run.py` and `python benchmarks/uvicorn_bench.py`, on an M3 Max running
macOS 26 and CPython 3.14. Rounds are interleaved across loops and the best of each is
reported, with the run-to-run spread beside it.

| Benchmark | asyncio | uvloop | zuv | zuv / uvloop |
| --- | ---: | ---: | ---: | ---: |
| `call_soon` | 2.65M/s | 5.35M/s | **7.81M/s** | **1.46x** |
| `call_soon` with arguments | 2.22M/s | 3.72M/s | **6.20M/s** | **1.66x** |
| timer schedule + cancel | 1.54M/s | 2.23M/s | **11.8M/s** | **5.29x** |
| bulk stream | 7.9 GiB/s | 9.0 GiB/s | **9.6 GiB/s** | **1.07x** |
| loop iterations (`sleep(0)`) | 72.2k/s | 79.6k/s | 79.8k/s | 1.00x |
| echo round trips, 1 KiB | 47.2k/s | 61.3k/s | 59.9k/s | 0.98x |
| uvicorn, plaintext | 53.7k req/s | 69.6k req/s | 66.8k req/s | 0.96x |
| uvicorn, 10 KiB body | 51.4k req/s | 68.0k req/s | 62.9k req/s | 0.93x |
| `getaddrinfo`, numeric host | 27.8k/s | 1.50M/s | 897k/s | 0.59x |

Scheduling and timers are where the design differs most: arguments live inside the handle
rather than in a tuple, and timers share one `uv_timer_t` behind a heap instead of taking a
libuv handle each.

**uvloop is still ahead on request/response throughput** - 4 to 7% on real HTTP serving. zuv
is roughly 25% faster than stock asyncio there, but the goal is to match uvloop and it does
not yet. `getaddrinfo` is the other gap: uvloop parses address literals itself, while zuv
hands them to libc with `AI_NUMERICHOST`, which is slower but cannot disagree with
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
| OpenTelemetry emission | `src/zuv/_instrumentation.py` | The only file that imports OTel |

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

`zuv` emits plain [OpenTelemetry](https://opentelemetry.io). Its only runtime dependency is
`opentelemetry-api` - not the SDK, and nothing vendor-specific. Until an application installs a
provider, OpenTelemetry hands back proxy instruments whose methods do nothing, so an uninstrumented
program pays nothing.

The measurement happens in Zig; Python only records it.

| Signal | Kind | Measured by |
| --- | --- | --- |
| `zuv.slow_callback` | span, with real start and end timestamps | `uv_hrtime()` around the callback |
| `zuv.unhandled_exception` | span, with the exception recorded | the loop's error path |
| `zuv.slow_callbacks`, `zuv.unhandled_exceptions` | counters | as above |
| `zuv.callback_duration` | histogram | `uv_hrtime()` |
| `zuv.loop_count`, `events`, `events_waiting`, `idle_time_ns`, `callbacks_run`, `ready`, `timers`, `watchers` | gauges | native counters plus `uv_metrics_info()`, sampled on a dedicated `uv_timer_t` |

Anything that speaks OpenTelemetry collects it. `logfire.configure()` is one such thing:

```python
import logfire
import zuv

logfire.configure()  # installs the OTel providers; zuv needs no logfire import


async def main() -> None:
    zuv.instrument()  # start the periodic loop gauges
    ...
```

Slow-callback spans carry the awaiting call graph, captured with `asyncio.format_call_graph()`
(new in 3.14), so you see *why* the callback was running rather than just its repr. The span's
duration is reconstructed from the loop's monotonic measurement, so it covers the callback itself
rather than the moment it was reported.

Gauges are deliberately synchronous rather than observable: the values are live loop state, and an
observable instrument's callback would run on the exporter's collection thread while the loop
thread is mutating them. Pushing from the loop's own timer is what makes reading them safe.

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
