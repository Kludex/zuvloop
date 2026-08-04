# API reference

zuv's own surface is small on purpose. Everything else you use is asyncio's.

## Running

### `zuv.run`

```python
zuv.run(coro, *, debug=None, loop_factory=zuv.new_event_loop)
```

`asyncio.run()` with zuv's loop as the default. Takes the same arguments and has
the same semantics, including cancelling remaining tasks and shutting down async
generators and the default executor.

### `zuv.new_event_loop`

```python
zuv.new_event_loop() -> zuv.EventLoop
```

Creates a loop. This is the callable to hand to `asyncio.run()`,
`asyncio.Runner`, anyio, uvicorn or pytest — see
[Running the loop](../usage/running.md).

## Types

### `zuv.EventLoop`

An `asyncio.AbstractEventLoop`. Every method asyncio declares is implemented;
`sock_sendfile` and `sendfile` raise `NotImplementedError`, which asyncio treats
as a signal to fall back.

Beyond asyncio's surface it carries the usual knobs — `set_debug`,
`slow_callback_duration`, `set_exception_handler`, `set_default_executor`.

### `zuv.Server`

An `asyncio.AbstractServer`, returned by `create_server` and
`create_unix_server`. Supports `close()`, `close_clients()`, `abort_clients()`,
`wait_closed()`, `start_serving()`, `serve_forever()`, `sockets` and use as an
async context manager.

### `zuv.Transport`

The native stream transport, an `asyncio.Transport` subclass. Used for TCP, Unix
sockets and pipes. You get one from `create_connection`, a server's
`connection_made`, or `connect_read_pipe` / `connect_write_pipe`.

### `zuv.DatagramTransport`

The native datagram transport, an `asyncio.DatagramTransport` subclass, returned
by `create_datagram_endpoint`.

### `zuv.Handle` and `zuv.TimerHandle`

Returned by `call_soon`, `call_later` and `call_at`. They carry `cancel()`,
`cancelled()`, and `when()` on the timer.

/// warning

These are not `asyncio.Handle` subclasses. Code that checks
`isinstance(handle, asyncio.Handle)` will not recognise them. uvloop's handles
have the same property.
///

## Instrumentation

### `zuv.instrument`

```python
zuv.instrument(interval=1.0) -> None
```

Starts periodic sampling of the loop gauges on the running loop. Spans and
counters need no setup; only the gauges are sampled. See
[Instrumentation](../usage/instrumentation.md).

### `zuv.Instrumentation`

The per-loop instrumentation state, reachable as `loop._instrumentation`. Useful
if you want to drive sampling yourself rather than through `instrument()`.

### `zuv.MetricsReporter`

The callable the native sampler invokes with each batch of gauge values.

## Utilities

### `zuv.libuv_version`

```python
zuv.libuv_version() -> str
```

The vendored libuv version, as a string. Useful in bug reports, and as a cheap
check that the extension imported.
