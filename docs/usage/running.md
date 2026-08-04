# Running the loop

zuv is a loop factory. Everything below is a different way to hand that factory
to something that starts a loop.

## Directly

```python
import zuv

zuv.run(main())
```

`zuv.run()` takes the same arguments as `asyncio.run()` — `debug` and
`loop_factory` included — and is exactly `asyncio.run()` with zuv's factory as
the default.

## Through asyncio

```python
import asyncio

import zuv

asyncio.run(main(), loop_factory=zuv.new_event_loop)
```

Prefer this when something else already owns the call to `asyncio.run()`.

`asyncio.Runner` takes the same argument, which is what you want if you need the
loop to outlive a single coroutine:

```python
with asyncio.Runner(loop_factory=zuv.new_event_loop) as runner:
    runner.run(setup())
    runner.run(main())
```

## Through a framework

Most frameworks accept a loop factory somewhere. A few examples:

//// tab | uvicorn

```console
$ uvicorn app:app --loop zuv_loop:zuv_loop_factory
```

Where `zuv_loop.py` is:

```python
import zuv


def zuv_loop_factory():
    return zuv.new_event_loop()
```
////

//// tab | anyio

```python
import anyio

import zuv

anyio.run(main, backend="asyncio", backend_options={"loop_factory": zuv.new_event_loop})
```
////

//// tab | pytest

```python
import pytest

import zuv


@pytest.fixture
def anyio_backend():
    return "asyncio", {"loop_factory": zuv.new_event_loop}
```
////

## Creating a loop by hand

```python
import zuv

loop = zuv.new_event_loop()
try:
    loop.run_until_complete(main())
finally:
    loop.close()
```

`zuv.new_event_loop()` returns a [`zuv.EventLoop`](../reference/api.md#zuveventloop), which is an
`asyncio.AbstractEventLoop`. Close it when you are done: the loop owns a libuv
loop, a self-pipe and any transports still open, and closing is what releases
them.

/// warning | One loop per thread

A loop belongs to the thread that runs it, as in asyncio. `call_soon_threadsafe`
is the only method other threads may call, and signal handlers may only be
registered from the main thread.
///

## Debug mode

```python
zuv.run(main(), debug=True)
```

Debug mode times every callback and reports the slow ones through
`call_exception_handler`, the same as asyncio. zuv also turns each into an
OpenTelemetry span carrying the awaiting call graph — see
[Instrumentation](instrumentation.md).

The timing is done in Zig with `uv_hrtime()`, so enabling debug mode costs one
clock read per callback rather than a Python-level wrapper.
