# Instrumentation

zuvloop emits plain [OpenTelemetry](https://opentelemetry.io). Its only runtime
dependency is `opentelemetry-api` — not the SDK, and nothing vendor-specific.

Slow callbacks are always timed with two native clock reads. OpenTelemetry hands
back proxy instruments whose methods do nothing until an application installs a
provider, and Python reporting runs only when a callback exceeds the threshold.
Keeping the inexpensive native measurement active means a provider installed
while the loop is already running receives subsequent events.

**The measurement happens in Zig. Python only records it.**

## Signals

| Signal | Kind | Measured by |
| --- | --- | --- |
| `zuvloop.slow_callback` | span, with real start and end timestamps | `uv_hrtime()` around the callback |
| `zuvloop.unhandled_exception` | span, with the exception recorded | the loop's error path |
| `zuvloop.slow_callbacks`, `zuvloop.unhandled_exceptions` | counters | as above |
| `zuvloop.callback_duration` | histogram | `uv_hrtime()` |
| `zuvloop.loop_count`, `events`, `events_waiting`, `idle_time_ns`, `callbacks_run`, `ready`, `timers`, `watchers` | gauges | native counters and `uv_metrics_info()`, sampled on a dedicated `uv_timer_t` |

## Collecting it

Anything that speaks OpenTelemetry collects it, and nothing needs to be turned
on. `logfire.configure()` is one such thing, and zuvloop does not import logfire
to work with it:

```python
import logfire
import zuvloop


async def main() -> None: ...


logfire.configure()  # installs the OTel providers
zuvloop.run(main())
```

Spans and counters are emitted as the events happen. The gauges are sampled
automatically while the loop runs, but only when a real meter provider is
installed - without one there would be nowhere for the numbers to go, so the
sampler never starts. The default interval is 10 seconds; set
`loop.metrics_interval` before running the loop to change it:

```python
loop = zuvloop.new_event_loop()
loop.metrics_interval = 1.0
```

## Slow callbacks

Slow-callback spans carry the awaiting call graph, captured with
`asyncio.format_call_graph()`, so you see *why* the callback was running rather
than just its repr.

The span's duration is reconstructed backwards from the loop's own monotonic
measurement, so it covers the callback itself rather than the moment it was
reported.

Slow callbacks are monitored continuously, independent of asyncio debug mode or
when the OpenTelemetry provider is installed. Metrics-only configurations skip
span and call-graph construction. Set the threshold as you would on any loop:

```python
loop.slow_callback_duration = 0.05
```

/// note | Why the gauges are synchronous

They are pushed from the loop's timer rather than pulled by an observable
instrument. The values are live loop state, and an observable instrument's
callback would run on the exporter's collection thread while the loop thread is
mutating them. Pushing from the loop's own timer is what makes reading them safe.

The sampler runs on its own `uv_timer_t` and is unreferenced, so it neither
enters the callback queue nor keeps the loop alive.
///
