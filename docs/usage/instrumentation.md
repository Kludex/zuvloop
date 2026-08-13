# Instrumentation

zuvloop emits plain [OpenTelemetry](https://opentelemetry.io). Its only runtime
dependency is `opentelemetry-api` — not the SDK, and nothing vendor-specific.

Until an application installs a provider, OpenTelemetry hands back proxy
instruments whose methods do nothing and slow-callback timing stays off.
Providers can be installed before the loop starts or from inside it —
`logfire.configure()` in `main()` works: zuvloop checks at each `run_forever()`
entry and re-checks every `metrics_interval` seconds (10 by default) while the
loop runs, so a provider configured after start arms slow-callback timing
within one interval.

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
automatically while the loop runs and published when a real meter provider is
installed - without one the snapshot is dropped, since there is nowhere for
the numbers to go. The same timer is what notices a provider configured after
the loop started. The default interval is 10 seconds; set
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

Slow callbacks are monitored whenever an OpenTelemetry tracing or metrics
provider is installed - whether it was there when the loop started or appeared
up to `metrics_interval` seconds ago; asyncio debug mode does not need to be
enabled. Metrics-only configurations skip span and call-graph construction.

A slow callback is reported as a warning, not an error: the span's status is
left unset - OpenTelemetry has no warning status - and the severity travels as
a `logfire.level_num` attribute, which backends that do not know it ignore.

Set the threshold as you would on any loop:

```python
loop.slow_callback_duration = 0.05
```

Set the threshold to infinity to disable slow-callback monitoring while keeping
other OpenTelemetry signals enabled. This takes the native fast path and skips
the per-callback clock reads as well as spans and metrics:

```python
import math

loop.slow_callback_duration = math.inf
```

/// note | Why the gauges are synchronous

They are pushed from the loop's timer rather than pulled by an observable
instrument. The values are live loop state, and an observable instrument's
callback would run on the exporter's collection thread while the loop thread is
mutating them. Pushing from the loop's own timer is what makes reading them safe.

The sampler runs on its own `uv_timer_t` and is unreferenced, so it neither
enters the callback queue nor keeps the loop alive.
///
