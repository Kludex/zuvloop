# zuvloop

<p align="center">
  <em>A fast, drop-in asyncio event loop, powered by libuv and written in Zig ⚡</em>
</p>

---

**Documentation**: <a href="https://zuvloop.marcelotryle.com" target="_blank">https://zuvloop.marcelotryle.com</a>

**Source Code**: <a href="https://github.com/Kludex/zuvloop" target="_blank">https://github.com/Kludex/zuvloop</a>

---

zuvloop is a replacement for the built-in `asyncio` event loop.

Your code stays the same. The loop underneath gets faster. 🚀

The key features are:

- **Fast**: Scheduling, timers, sockets, and DNS run in native code, driven by [libuv](https://libuv.org) - the same engine behind Node.js. Over **25x faster than asyncio** at thread-safe scheduling and matches or beats [uvloop](https://github.com/MagicStack/uvloop) on 13 of the 14 benchmarks below.
- **Drop-in**: One line to switch. Everything is standard `asyncio` — same `Task` objects, same protocols, same APIs.
- **Fully typed**: Ships type hints for everything and passes **strict mypy**. Your editor will love it. ✨
- **Observable**: Built-in [OpenTelemetry](https://opentelemetry.io) instrumentation — slow-callback spans, unhandled-exception spans, loop metrics. Zero cost until you turn it on.
- **Modern**: Built for Python 3.14, including the new asyncio introspection tools (`python -m asyncio ps`, call graphs, and friends).

## Performance

<p align="center">
  <img src="docs/assets/performance.png" alt="zuvloop vs uvloop vs asyncio benchmarks">
</p>

Throughput relative to stock asyncio (higher is better), measured with the suite in
`benchmarks/` on an M3 Max, macOS 26, CPython 3.14, and libuv 1.51.0. Each result is the median of seven
interleaved in-process runs or five interleaved HTTP runs. The labels show the absolute numbers.

| Benchmark | asyncio | uvloop | zuvloop |
| --- | ---: | ---: | ---: |
| `call_soon` registration | 2.70M/s | 4.99M/s | **9.80M/s** |
| `call_soon` registration with arguments | 2.40M/s | 3.45M/s | **7.95M/s** |
| ready callback batch | 6.36M/s | 12.1M/s | **17.1M/s** |
| `call_soon_threadsafe` | 0.44M/s | 5.13M/s | **11.9M/s** |
| timer schedule + cancel | 1.17M/s | 1.93M/s | **9.02M/s** |
| completed timer rounds | 71.1k/s | 78.2k/s | **2.93M/s** |
| prebuilt due timer batch | 1.38M/s | 2.73M/s | **5.89M/s** |
| ready chain with 250 idle connections | 70.7k/s | 76.3k/s | **362.6k/s** |
| bulk stream | 7.1 GiB/s | 7.9 GiB/s | **9.7 GiB/s** |
| echo round trips, 1 KiB | 36.8k/s | 53.6k/s | **57.6k/s** |
| uvicorn, plaintext | 50.3k req/s | 67.2k req/s | **72.6k req/s** |
| uvicorn, 10 KiB body | 48.0k req/s | 65.2k req/s | **70.2k req/s** |
| aiohttp server | 46.2k req/s | 58.0k req/s | **58.8k req/s** |
| aiohttp client | 12.6k req/s | **15.0k req/s** | 14.8k req/s |
| `getaddrinfo`, numeric host | 27.0k/s | 1.48M/s | **1.70M/s** |

The `call_soon` rows measure registration only and drain the queued callbacks after timing stops. The ready callback
batch builds its queue before timing starts, then measures dispatch only. `call_soon_threadsafe` measures a producer
thread scheduling while the loop concurrently dispatches its callbacks.

The timer rows measure different work. Timer schedule + cancel isolates heap bookkeeping and handle cleanup without
firing callbacks. Completed timer rounds chain one zero-delay timer per event loop turn. The prebuilt due timer batch
measures heap draining and callback dispatch, with allocation and deallocation outside the timed section.

Curious how? The [architecture docs](https://zuvloop.marcelotryle.com) explain the design:
argument storage inside handles (no tuple per callback), a native timer heap behind a single
`uv_timer_t`, per-turn vectored write batching, zero-copy reads, and a `getaddrinfo` fast path
for address literals.

## Requirements

- Python 3.14+
- Standard and free-threaded CPython builds
- Linux, macOS or Windows
- Prebuilt wheels for Linux x86-64/AArch64, macOS x86-64/arm64 and Windows AMD64/ARM64

## Installation

```console
$ pip install zuvloop
```

Source distributions install a pinned Zig 0.16 toolchain in their isolated build
environment. Direct native development commands require Zig 0.16 on `PATH`.

## Example

Write normal asyncio code, run it with zuvloop:

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

Prefer to keep `asyncio.run()`? Hand it the loop factory:

```python
asyncio.run(main(), loop_factory=zuvloop.new_event_loop)
```

That's it. That's the migration. 🎉

## Observability

zuvloop emits plain OpenTelemetry. Its small runtime surface is `opentelemetry-api` plus
`typing-extensions` for the shipped type declarations — no SDK and nothing vendor-specific.
Providers can be configured before the loop starts or from inside
it — `logfire.configure()` in `main()` works: zuvloop checks at each `run_forever()` entry and
re-checks on its sampling interval (`loop.metrics_interval`, 10 seconds by default) while the
loop runs. Until a provider is installed the instruments are no-ops and slow-callback timing
stays off.

Anything that speaks OpenTelemetry can collect it. For example, with
<a href="https://logfire.pydantic.dev" target="_blank">Logfire</a>:

```python
import logfire
import zuvloop

logfire.configure()  # installs the OTel providers


async def main() -> None: ...


zuvloop.run(main())
```

That's all — there is no zuvloop-specific setup. Spans and counters are emitted
as events happen, and the loop gauges are sampled automatically while the loop
runs (published only once a real provider is installed; without one the
snapshot is dropped).

You get:

- **`zuvloop.slow_callback`** spans — with real start/end timestamps measured by `uv_hrtime()`
  in native code, and the awaiting call graph attached (via `asyncio.format_call_graph()`),
  so you see *why* a callback was running, not just its repr.
- **`zuvloop.unhandled_exception`** spans — with the exception recorded.
- Counters, a callback-duration histogram, and live loop gauges (`loop_count`, `events`,
  `idle_time_ns`, `ready`, `timers`, `watchers`, ...).

And because zuvloop schedules real `asyncio.Task` objects, the Python 3.14 introspection tools
work unchanged:

```console
$ python -m asyncio ps <pid>
$ python -m asyncio pstree <pid>
```

## Compatibility

Every pull request is held behind three stable aggregate gates. The first covers the in-repository
suite on Linux and macOS, the portable suite on Windows, plus musl runtime tests,
cross-compilation, ReleaseSafe builds and documentation. The second runs CPython conformance and
pinned upstream suites from aiohttp, uvicorn, AnyIO, websockets, aioquic, Tornado and HTTPX2 with
zuvloop swapped underneath. The third runs the native sanitizer build and a 500-cycle
resource-ownership soak.

The compatibility run also tests CPython 3.14.0, the newest 3.14 patch, and standard and free-threaded
3.15 builds, then exercises gRPC AsyncIO, asyncpg, Psycopg and redis-py against real local services.
The immutable commits and exact commands in `.github/workflows/compatibility.yml` are the source
of truth.

`scripts/conformance.py` runs CPython's `EventLoopTestsMixin`, `SubprocessTestsMixin` and
`BaseSockTestsMixin` against zuvloop, downloading the source of whichever interpreter is running
so the suite always matches it. Each test runs in its own process, so a hang is reported rather
than stopping the run. Three of the four skips are white-box tests of CPython's own internals -
two patch `asyncio.base_events.socket`, one counts calls to `BaseEventLoop._run_once` - which no
loop outside the standard library can satisfy.

The aiohttp compatibility run disables its optional `blockbuster` plugin because that plugin
exempts stdlib asyncio calls by source filename and therefore reports equivalent `os.stat` and
`os.sendfile` calls from any third-party loop. Its remaining strict expected failure is the
`loop.time()` difference below. One concurrent WebSocket-close test is skipped because it assumes
selector-loop ready/I/O ordering and fails intermittently on uvloop too; every other aiohttp test
remains enforced.

(For reference: uvloop cannot complete the aiohttp suite — it fails fifteen tests and then hangs.)

There is one intentional difference: **patching `loop.time()` does not move the scheduler**.
zuvloop keeps its timer heap in native code and reads the clock directly, so monkeypatching
`time()` — a trick some test suites use to fast-forward timeouts — changes what `loop.time()`
returns and nothing else. A loop that needs a controllable clock should schedule against one
explicitly.

One more deliberate divergence: handles
returned by `call_soon` implement the `asyncio.Handle` interface but are not instances of it:
the base class is 56 bytes of storage such a handle never writes, measured at 2% of `call_soon`,
which is the object the loop allocates more often than any other. `call_later` and `call_at`
do return real `asyncio.TimerHandle` instances, so they order and compare by deadline, and
`call_soon_threadsafe` returns a real `asyncio.Handle`, because 3.14 requires cancelling one
from another thread to block until a callback that has already started finishes.

## Development

```console
$ uv venv --python 3.14
$ uv pip install -e . --group dev
$ uv run pytest
$ uv run mypy
$ uv run ruff check
$ uv run ruff format --check
$ ./scripts/check-zig  # requires ZLint 0.9.1 on PATH
$ uv run --group bench python benchmarks/run.py
```

The extension is rebuilt by the hatch-ziglang build hook on every install. To rebuild in place:

```console
$ python scripts/build.py
```

`vendor/libuv` is an unmodified upstream release tarball; see `vendor/README.md`. Update it with
`./vendor/update-libuv.sh <version> <sha256>`.

## License

This project is licensed under the terms of the MIT license.
