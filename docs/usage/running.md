# Running the loop

zuvloop is a loop factory. Everything below is a different way to hand that factory
to something that starts a loop.

## Directly

```python
import zuvloop

zuvloop.run(main())
```

`zuvloop.run()` takes the same arguments as `asyncio.run()` — `debug` and
`loop_factory` included — and is exactly `asyncio.run()` with zuvloop's factory as
the default.

## Through asyncio

```python
import asyncio

import zuvloop

asyncio.run(main(), loop_factory=zuvloop.new_event_loop)
```

Prefer this when something else already owns the call to `asyncio.run()`.

`asyncio.Runner` takes the same argument, which is what you want if you need the
loop to outlive a single coroutine:

```python
with asyncio.Runner(loop_factory=zuvloop.new_event_loop) as runner:
    runner.run(setup())
    runner.run(main())
```

## Through a framework

Most frameworks accept a loop factory somewhere. A few examples:

//// tab | uvicorn

```console
$ uvicorn app:app --loop zuvloop_loop:zuvloop_loop_factory
```

Where `zuvloop_loop.py` is:

```python
import zuvloop


def zuvloop_loop_factory():
    return zuvloop.new_event_loop()
```
////

//// tab | anyio

```python
import anyio

import zuvloop

anyio.run(main, backend="asyncio", backend_options={"loop_factory": zuvloop.new_event_loop})
```
////

//// tab | pytest

```python
import pytest

import zuvloop


@pytest.fixture
def anyio_backend():
    return "asyncio", {"loop_factory": zuvloop.new_event_loop}
```
////

## Creating a loop by hand

```python
import zuvloop

loop = zuvloop.new_event_loop()
try:
    loop.run_until_complete(main())
finally:
    loop.close()
```

`zuvloop.new_event_loop()` returns a [`zuvloop.EventLoop`](../reference/api.md#zuvloopeventloop), which is an
`asyncio.AbstractEventLoop`. Close it when you are done: the loop owns a libuv
loop, a self-pipe and any transports still open, and closing is what releases
them.

/// warning | One loop per thread

A loop belongs to the thread that runs it, as in asyncio. `call_soon_threadsafe`
is the method other threads should call, and signal handlers may only be
registered from the main thread.
[Scheduling from another thread](#scheduling-from-another-thread) covers what
the other scheduling calls do.
///

## Debug mode

```python
zuvloop.run(main(), debug=True)
```

Debug mode times every callback and reports the slow ones through
`call_exception_handler`, the same as asyncio. zuvloop also turns each into an
OpenTelemetry span carrying the awaiting call graph — see
[Instrumentation](instrumentation.md).

The timing is done in Zig with `uv_hrtime()`, so enabling debug mode costs one
clock read per callback rather than a Python-level wrapper.

Debug mode also rejects a `call_soon()` from the wrong thread, with the same
`RuntimeError` as asyncio. The next section covers what happens without it.

## Scheduling from another thread

While a thread is inside `run_forever()`, the ready queue, the timer heap and
the libuv handles belong to that thread. A call from any other thread is handed
over through the thread-safe inbox, which wakes the loop, or it is refused:

| Call | Debug off | Debug on |
| --- | --- | --- |
| `call_soon_threadsafe()` | Queued, and the loop is woken | The same |
| `call_soon()` | Handled as `call_soon_threadsafe()` | `RuntimeError` |
| `call_later()`, `call_at()` | `RuntimeError` | `RuntimeError` |

asyncio accepts all three without debug mode and leaves the outcome to chance:
the loop is not woken, so the callback runs whenever something else wakes it.
zuvloop routes `call_soon()` through the thread-safe path, so it runs promptly.
A timer cannot be handed over that way, because the caller gets back a handle
that only the loop thread may cancel, so those two calls are refused. Schedule
the timer from the loop thread:

```python
loop.call_soon_threadsafe(loop.call_later, 5, callback)
```

Outside a run, the loop accepts all of them from any thread.

That window is wider than `is_running()`. After `stop()`, the loop still flushes
the writes its last callbacks queued, and `is_running()` is already false while
it does. Until that flush finishes, `close()` and a second `run_forever()` raise
`RuntimeError` too, with or without debug mode.
