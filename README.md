# zuvloop

A [libuv](https://libuv.org) event loop for `asyncio`, written in [Zig](https://ziglang.org).

**Documentation**: <https://zuvloop.marcelotryle.com>

`zuvloop` replaces the asyncio event loop with one whose hot paths - callback scheduling, timers,
descriptor watching, name resolution and the stream data path - are implemented natively and
driven by libuv. It targets uvloop's performance while shipping type hints, a strict-mypy-clean
Python surface, and OpenTelemetry instrumentation.

```python
import asyncio

import zuvloop


async def main() -> None:
    reader, writer = await asyncio.open_connection("example.com", 80)
    writer.write(b"GET / HTTP/1.0\r\nHost: example.com\r\n\r\n")
    await writer.drain()
    print(await reader.read(64))
    writer.close()
    await writer.wait_closed()


zuvloop.run(main())
```

Or hand the loop factory to asyncio directly:

```python
asyncio.run(main(), loop_factory=zuvloop.new_event_loop)
```

## Performance

`benchmarks/`, on an M3 Max running
macOS 26 and CPython 3.14. Rounds are interleaved across loops and the best of each is
reported, with the run-to-run spread beside it.

| Benchmark | asyncio | uvloop | zuvloop | zuvloop / uvloop |
| --- | ---: | ---: | ---: | ---: |
| `call_soon` | 2.69M/s | 4.69M/s | **5.91M/s** | **1.26x** |
| `call_soon` with arguments | 2.43M/s | 3.87M/s | **6.47M/s** | **1.67x** |
| timer schedule + cancel | 1.58M/s | 2.62M/s | **9.55M/s** | **3.64x** |
| bulk stream | 8.4 GiB/s | 8.5 GiB/s | **10.2 GiB/s** | **1.20x** |
| loop iterations (`sleep(0)`) | 73.2k/s | 78.2k/s | 78.2k/s | 1.00x |
| echo round trips, 1 KiB | 39.0k/s | 56.8k/s | **58.5k/s** | **1.03x** |
| uvicorn, plaintext | 55.3k req/s | 71.9k req/s | **75.9k req/s** | **1.06x** |
| uvicorn, 10 KiB body | 52.6k req/s | 68.7k req/s | **73.6k req/s** | **1.07x** |
| aiohttp server | 49.0k req/s | 59.8k req/s | **60.6k req/s** | **1.01x** |
| aiohttp client | 13.2k req/s | 16.2k req/s | **16.9k req/s** | **1.04x** |
| `getaddrinfo`, numeric host | 28.5k/s | 1.57M/s | **1.90M/s** | **1.21x** |

Scheduling and timers are where the design differs most: arguments live inside the handle
rather than in a tuple, and timers share one `uv_timer_t` behind a heap instead of taking a
libuv handle each.

Reads take one of two paths depending on the traffic. Below 64 KiB the data is copied out of
a shared buffer into an exactly sized `bytes` - an HTTP request is a couple of hundred bytes,
and allocating a large object to shrink it again costs more than the copy. Above that libuv
fills the final object directly and nothing is copied. The threshold the transport is judged
against follows the traffic, doubling whenever a read fills the buffer.

Writes issued during one turn of the loop are sent together, as a single vectored write. That
matters more than it sounds: ASGI and aiohttp both send a response as a header write followed by
a body write, so a loop that writes each piece as it arrives spends two syscalls on every
response. `benchmarks/write_batching.py` measures exactly that, by serving one fixed response
both ways:

| response split into two writes | asyncio | uvloop | zuvloop |
| --- | ---: | ---: | ---: |
| throughput lost | -36% | -0.1% | **-0.7%** |

The HTTP rows depend as much on the server's parser as on the loop, so `uvicorn` is measured with
`httptools` and the benchmark prints which parser it found; the same numbers on uvicorn's
pure-Python fallback are a third as large for every loop, and say nothing about the loop at all.

Address literals are answered without entering libc. `getaddrinfo` costs around half a
microsecond even under `AI_NUMERICHOST`, where it has nothing to look up but still builds a chain,
takes the resolver's locks and has to be freed; `inet_pton` is an order of magnitude cheaper.
Everything the shortcut cannot answer identically is refused and falls back to libc - a scoped
address, whose zone only libc can resolve, a legacy form like `127.1` that `inet_pton` rejects,
an unspecified socket type, or any flag that could change the answer. Across 2430 combinations of
host, port, family, type and flags, this agrees with `socket.getaddrinfo` on exactly the cases it
did before the shortcut existed.

## Compatibility

zuvloop is checked against the test suites of the projects that exercise an event loop hardest,
by running them unmodified with the loop swapped underneath.

| Suite | Result |
| --- | --- |
| uvicorn | 1257 passed, no failures |
| aiohttp | 4473 passed, 36 failed - 33 of which also fail on stock asyncio |

Three aiohttp failures are zuvloop's alone. Two are the `blockbuster` plugin flagging `os.stat`
inside `create_unix_server` - a call stdlib asyncio makes in the same place, and which the plugin
exempts by file path rather than by behaviour. The third is a genuine difference, below.

For reference, uvloop cannot complete that suite: it fails fifteen tests in
`test_client_functional.py` and then hangs.

### Known differences

**Patching `loop.time()` does not move the scheduler.** asyncio runs its timers off `self.time()`,
so replacing that method fast-forwards the loop - a trick test suites use to expire timeouts
without waiting. zuvloop keeps the timer heap in Zig and reads the clock directly, so a patched
`time()` changes what `loop.time()` returns and nothing else. Making the scheduler consult Python
on every timer operation would cost more than the compatibility is worth; a loop that needs a
controllable clock should schedule against one explicitly.

## Requirements

- Python 3.14 or newer
- Linux or macOS
- [Zig](https://ziglang.org/download/) 0.16 to build from source

## Design

| Layer | Where it lives | Why |
| --- | --- | --- |
| Ready queue, timer heap, cross-thread inbox | `zig/loop.zig` | Called once per callback |
| Callback handles | `zig/handle.zig` | Arguments stored inline, invoked through vectorcall |
| Stream and pipe reads and writes | `zig/transport.zig` | Called once per packet |
| Datagram sends and receives | `zig/datagram.zig` | Called once per datagram |
| Descriptor watchers | `zig/poller.zig` | One `uv_poll_t` per descriptor |
| Name resolution | `zig/dns.zig` | Runs on libuv's threadpool, not the executor |
| Connection and server setup | `zuvloop/_connect.py` | Called once per connection |
| Lifecycle, executors, error reporting | `zuvloop/_base.py` | Called once per loop |
| OpenTelemetry emission | `zuvloop/_instrumentation.py` | The only file that imports OTel |

A few decisions worth knowing about:

- **The GIL is released for the whole `uv_run` call.** Callbacks reacquire it through a thread
  state saved once by the loop, rather than `PyGILState_Ensure`, and a nested callback within the
  same batch pays nothing.
- **`call_soon(cb, a, b)` allocates no tuple.** Up to three arguments are stored inside the handle
  and passed straight to `PyObject_Vectorcall`.
- **Timers use one `uv_timer_t` and an internal heap**, rather than a libuv handle per timer.
  Cancellation is O(1) and the heap is compacted lazily, matching asyncio's scheduler semantics.
- **Writes are batched per turn and never copied.** Everything written during one turn of the loop
  goes out as a single vectored `uv_try_write`, so a response sent in pieces still costs one
  syscall; when the socket cannot take it all, the queued request holds a buffer view of the
  caller's memory rather than a copy of it. The flush runs from a prepare handle, which libuv runs
  before it computes the poll timeout - so no write ever waits on the loop going to sleep.
- **Reads land directly in the `bytes` object** handed to `data_received`, so the kernel writes once
  and nothing is copied afterwards. `BufferedProtocol` goes one better and reads into the protocol's
  own buffer.
- **Sockets are created, bound and accepted in Python.** Those happen once per connection, so the
  readable implementation is worth more there than the last microsecond.

## Instrumentation

`zuvloop` emits plain [OpenTelemetry](https://opentelemetry.io). Its only runtime dependency is
`opentelemetry-api` - not the SDK, and nothing vendor-specific. Until an application installs a
provider, OpenTelemetry hands back proxy instruments whose methods do nothing, so an uninstrumented
program pays nothing.

The measurement happens in Zig; Python only records it.

| Signal | Kind | Measured by |
| --- | --- | --- |
| `zuvloop.slow_callback` | span, with real start and end timestamps | `uv_hrtime()` around the callback |
| `zuvloop.unhandled_exception` | span, with the exception recorded | the loop's error path |
| `zuvloop.slow_callbacks`, `zuvloop.unhandled_exceptions` | counters | as above |
| `zuvloop.callback_duration` | histogram | `uv_hrtime()` |
| `zuvloop.loop_count`, `events`, `events_waiting`, `idle_time_ns`, `callbacks_run`, `ready`, `timers`, `watchers` | gauges | native counters plus `uv_metrics_info()`, sampled on a dedicated `uv_timer_t` |

Anything that speaks OpenTelemetry collects it. `logfire.configure()` is one such thing:

```python
import logfire
import zuvloop

logfire.configure()  # installs the OTel providers; zuvloop needs no logfire import


async def main() -> None:
    zuvloop.instrument()  # start the periodic loop gauges
    ...
```

Slow-callback spans carry the awaiting call graph, captured with `asyncio.format_call_graph()`
(new in 3.14), so you see *why* the callback was running rather than just its repr. The span's
duration is reconstructed from the loop's monotonic measurement, so it covers the callback itself
rather than the moment it was reported.

Gauges are deliberately synchronous rather than observable: the values are live loop state, and an
observable instrument's callback would run on the exporter's collection thread while the loop
thread is mutating them. Pushing from the loop's own timer is what makes reading them safe.

Because `zuvloop` schedules real `asyncio.Task` objects rather than its own, the 3.14 introspection
APIs work unchanged - `asyncio.all_tasks()`, `asyncio.current_task()`, `asyncio.capture_call_graph()`
and `asyncio.print_call_graph()` are all covered by the test suite. The same applies to the
out-of-process tooling, which reads those task objects straight out of process memory:

```console
$ python -m asyncio ps <pid>
$ python -m asyncio pstree <pid>
```

Those two need the platform's usual debugging privileges (on macOS, `sudo`) - a restriction that
applies to any event loop, `zuvloop` or not.

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
