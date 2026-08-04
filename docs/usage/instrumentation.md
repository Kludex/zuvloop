# Instrumentation

zuv emits plain [OpenTelemetry](https://opentelemetry.io). Its only runtime
dependency is `opentelemetry-api` — not the SDK, and nothing vendor-specific.

Until an application installs a provider, OpenTelemetry hands back proxy
instruments whose methods do nothing. An uninstrumented program pays nothing, so
there is no flag to turn this off.

**The measurement happens in Zig. Python only records it.**

## Signals

| Signal | Kind | Measured by |
| --- | --- | --- |
| `zuv.slow_callback` | span, with real start and end timestamps | `uv_hrtime()` around the callback |
| `zuv.unhandled_exception` | span, with the exception recorded | the loop's error path |
| `zuv.slow_callbacks`, `zuv.unhandled_exceptions` | counters | as above |
| `zuv.callback_duration` | histogram | `uv_hrtime()` |
| `zuv.loop_count`, `events`, `events_waiting`, `idle_time_ns`, `callbacks_run`, `ready`, `timers`, `watchers` | gauges | native counters and `uv_metrics_info()`, sampled on a dedicated `uv_timer_t` |

## Collecting it

Anything that speaks OpenTelemetry collects it. `logfire.configure()` is one such
thing, and zuv does not import logfire to work with it:

```python
import logfire
import zuv


async def main() -> None:
    zuv.instrument()  # start the periodic loop gauges
    ...


logfire.configure()  # installs the OTel providers
zuv.run(main())
```

[`zuv.instrument()`](../reference/api.md#zuvinstrument) starts the gauge sampler. Spans and counters
need no setup — they are emitted as the events happen.

## Slow callbacks

Slow-callback spans carry the awaiting call graph, captured with
`asyncio.format_call_graph()`, so you see *why* the callback was running rather
than just its repr.

The span's duration is reconstructed backwards from the loop's own monotonic
measurement, so it covers the callback itself rather than the moment it was
reported.

Set the threshold as you would on any loop:

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
