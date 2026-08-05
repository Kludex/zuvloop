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

- **Fast**: Scheduling, timers, sockets, and DNS run in native code, driven by [libuv](https://libuv.org) — the same engine behind Node.js. Up to **6x faster than asyncio** and faster than [uvloop](https://github.com/MagicStack/uvloop) on every benchmark below.
- **Drop-in**: One line to switch. Everything is standard `asyncio` — same `Task` objects, same protocols, same APIs.
- **Fully typed**: Ships type hints for everything and passes **strict mypy**. Your editor will love it. ✨
- **Observable**: Built-in [OpenTelemetry](https://opentelemetry.io) instrumentation — slow-callback spans, unhandled-exception spans, loop metrics. Zero cost until you turn it on.
- **Modern**: Built for Python 3.14, including the new asyncio introspection tools (`python -m asyncio ps`, call graphs, and friends).

## Performance

<p align="center">
  <img src="docs/assets/performance.png" alt="zuvloop vs uvloop vs asyncio benchmarks">
</p>

Throughput relative to stock asyncio (higher is better), measured with the suite in
`benchmarks/` on an M3 Max, macOS 26, CPython 3.14. The labels show the absolute numbers.

| Benchmark | asyncio | uvloop | zuvloop |
| --- | ---: | ---: | ---: |
| `call_soon` | 2.69M/s | 4.69M/s | **5.91M/s** |
| `call_soon` with arguments | 2.43M/s | 3.87M/s | **6.47M/s** |
| timer schedule + cancel | 1.58M/s | 2.62M/s | **9.55M/s** |
| bulk stream | 8.4 GiB/s | 8.5 GiB/s | **10.2 GiB/s** |
| echo round trips, 1 KiB | 39.0k/s | 56.8k/s | **58.5k/s** |
| uvicorn, plaintext | 55.3k req/s | 71.9k req/s | **75.9k req/s** |
| uvicorn, 10 KiB body | 52.6k req/s | 68.7k req/s | **73.6k req/s** |
| aiohttp server | 49.0k req/s | 59.8k req/s | **60.6k req/s** |
| aiohttp client | 13.2k req/s | 16.2k req/s | **16.9k req/s** |
| `getaddrinfo`, numeric host | 28.5k/s | 1.57M/s | **1.90M/s** |

Curious how? The [architecture docs](https://zuvloop.marcelotryle.com) explain the design:
argument storage inside handles (no tuple per callback), a native timer heap behind a single
`uv_timer_t`, per-turn vectored write batching, zero-copy reads, and a `getaddrinfo` fast path
for address literals.

## Requirements

- Python 3.14+
- Linux or macOS

## Installation

```console
$ pip install zuvloop
```

To build from source you also need [Zig](https://ziglang.org/download/) 0.16.

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

zuvloop emits plain OpenTelemetry. The only runtime dependency is `opentelemetry-api` — not the
SDK, nothing vendor-specific. Until your application installs a provider, the instruments are
no-ops and cost nothing.

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
runs (only when a real provider is installed, so an uninstrumented program never
pays for sampling).

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

zuvloop is checked against the test suites of the projects that exercise an event loop hardest —
run unmodified, with the loop swapped underneath:

| Suite | Result |
| --- | --- |
| uvicorn | 1257 passed, no failures |
| aiohttp | 4473 passed, 36 failed — 33 of which also fail on stock asyncio |

(For reference: uvloop cannot complete the aiohttp suite — it fails fifteen tests and then hangs.)

There is one intentional difference: **patching `loop.time()` does not move the scheduler**.
zuvloop keeps its timer heap in native code and reads the clock directly, so monkeypatching
`time()` — a trick some test suites use to fast-forward timeouts — changes what `loop.time()`
returns and nothing else. A loop that needs a controllable clock should schedule against one
explicitly.

Also not implemented: `sendfile()` and `sock_sendfile()` raise `NotImplementedError`, and
`get_extra_info("socket")` is not provided (libuv owns the descriptor; handing out a Python
socket for it would risk a double close — `sockname`, `peername`, `family`, `type` and `proto`
are all available). Handles returned by `call_soon` and `call_later` implement the
`asyncio.Handle` and `asyncio.TimerHandle` interfaces but are not instances of those classes.

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

`vendor/libuv` is an unmodified upstream release tarball; see `vendor/README.md`. Update it with
`./vendor/update-libuv.sh <version>`.

## License

This project is licensed under the terms of the MIT license.
