---
icon: lucide/rotate-cw
---

# zuvloop

<p align="center">
    <em>A libuv event loop for asyncio, written in Zig.</em>
</p>

---

**Source Code**: <a href="https://github.com/Kludex/zuvloop" target="_blank">https://github.com/Kludex/zuvloop</a>

---

zuvloop replaces the asyncio event loop with one whose hot paths are implemented
natively and driven by [libuv](https://libuv.org). It is a **drop-in
replacement**: you hand it to `asyncio.run()` and everything you already wrote
keeps working.

It is the same idea as [uvloop](https://github.com/MagicStack/uvloop), with a
[Zig](https://ziglang.org) core instead of Cython, type hints that pass a strict
type checker, and OpenTelemetry instrumentation built in.

The key features are:

* **Fast**: faster than uvloop on most benchmarks in the suite. See
  [Performance](reference/performance.md).
* **Drop-in**: a loop factory. No API of its own to learn, no framework to adopt.
* **Typed**: a strict-mypy-clean Python surface, shipped with a `py.typed`
  marker.
* **Observable**: plain OpenTelemetry spans, counters and gauges, measured in
  Zig. No vendor SDK, and nothing to pay when nobody is listening.

## Installation

```console
$ pip install zuvloop
```

## Example

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

`zuvloop.run()` is `asyncio.run()` with the loop swapped. You can also hand the
factory to asyncio yourself:

```python
asyncio.run(main(), loop_factory=zuvloop.new_event_loop)
```

Nothing else in your program changes. That is the whole integration surface, and
[Running the loop](usage/running.md) covers the rest of it.

## Why a new loop

uvloop has been the answer for a decade, and it is a good one. zuvloop differs in
three ways that matter if you are choosing between them.

**It is faster.** Not by a lot everywhere, but by a lot in the places where a
loop can be. Writes issued during one turn go out as a single vectored syscall,
which is worth 20% on a real ASGI server because a response is sent as a header
write and a body write. Address literals never reach `getaddrinfo`.

**It agrees with asyncio.** Its transports are real `asyncio.Transport`
subclasses, `loop.time()` is `time.monotonic()`, and `get_extra_info("socket")`
returns a `TransportSocket`. Those sound like details until a library asserts on
one. [Compatibility](reference/compatibility.md) records where zuvloop and uvloop
each diverge from the standard library, with the cases measured rather than
claimed.

**It is typed and instrumented.** The Python surface passes `mypy --strict`, and
the loop emits OpenTelemetry without you importing an SDK.

## Requirements

Python 3.14+, on Linux, macOS or Windows. See
[Compatibility](reference/compatibility.md#interpreter-and-platform-policy) for
the Windows platform boundary.
