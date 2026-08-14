# API reference

zuvloop's own surface is small on purpose. Everything else you use is asyncio's.

## Running

### `zuvloop.run`

```python
zuvloop.run(coro, *, debug=None, loop_factory=zuvloop.new_event_loop)
```

`asyncio.run()` with zuvloop's loop as the default. Takes the same arguments and has
the same semantics, including cancelling remaining tasks and shutting down async
generators and the default executor.

### `zuvloop.new_event_loop`

```python
zuvloop.new_event_loop() -> zuvloop.EventLoop
```

Creates a loop. This is the callable to hand to `asyncio.run()`,
`asyncio.Runner`, anyio, uvicorn or pytest — see
[Running the loop](../usage/running.md).

## Types

### `zuvloop.EventLoop`

An `asyncio.AbstractEventLoop`. Every method asyncio declares is implemented,
including `sock_sendfile` and `sendfile`, which use the `sendfile(2)` system
call and fall back to a read-and-write loop where the syscall cannot serve —
see [Sending files](../usage/networking.md#sending-files).

Beyond asyncio's surface it carries the usual knobs — `set_debug`,
`slow_callback_duration`, `set_exception_handler`, `set_default_executor`.

### `zuvloop.Server`

An `asyncio.AbstractServer`, returned by `create_server` and
`create_unix_server`. Supports `close()`, `close_clients()`, `abort_clients()`,
`wait_closed()`, `start_serving()`, `serve_forever()`, `sockets` and use as an
async context manager.

### `zuvloop.Transport`

The native stream transport, an `asyncio.Transport` subclass. Used for TCP, Unix
sockets and pipes. You get one from `create_connection`, a server's
`connection_made`, or `connect_read_pipe` / `connect_write_pipe`.

### `zuvloop.DatagramTransport`

The native datagram transport, an `asyncio.DatagramTransport` subclass, returned
by `create_datagram_endpoint`.

### `zuvloop.Handle` and `zuvloop.TimerHandle`

Returned by `call_soon`, `call_later` and `call_at`. They carry `cancel()`,
`cancelled()`, and `when()` on the timer.

/// warning

The lean handle returned by `call_soon` is not an `asyncio.Handle` subclass. Code
that checks `isinstance(handle, asyncio.Handle)` will not recognise it. It still
implements `cancel()` and `cancelled()`.

`TimerHandle` is a real `asyncio.TimerHandle`, so timer handles order and compare
by deadline. `call_soon_threadsafe` returns an `asyncio.Handle` subclass with the
cross-thread cancellation synchronization Python 3.14 requires.
///

## Instrumentation

Everything is automatic: spans and counters are emitted as events happen, and
the loop gauges are sampled while the loop runs whenever a real OpenTelemetry
meter provider is installed. See [Instrumentation](../usage/instrumentation.md).

### `EventLoop.metrics_interval`

```python
loop.metrics_interval = 10.0  # seconds, the default
```

How often the gauges are sampled. Assign before running the loop.

### `zuvloop.Instrumentation`

The per-loop instrumentation state, reachable as `loop._instrumentation`.

## Utilities

### `zuvloop.libuv_version`

```python
zuvloop.libuv_version() -> str
```

The vendored libuv version, as a string. Useful in bug reports, and as a cheap
check that the extension imported.
