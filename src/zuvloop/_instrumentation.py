from __future__ import annotations

import asyncio
import functools
import time
from typing import Any

from opentelemetry import metrics, trace
from opentelemetry.trace import Status, StatusCode

_NAMESPACE = "zuvloop"


@functools.cache
def _meter() -> metrics.Meter:
    return metrics.get_meter(_NAMESPACE)


@functools.cache
def _tracer() -> trace.Tracer:
    return trace.get_tracer(_NAMESPACE)


@functools.cache
def _counter(name: str, description: str) -> metrics.Counter:
    return _meter().create_counter(f"{_NAMESPACE}.{name}", description=description)


@functools.cache
def _histogram(name: str, description: str, unit: str) -> metrics.Histogram:
    return _meter().create_histogram(f"{_NAMESPACE}.{name}", unit=unit, description=description)


@functools.cache
def _gauge(name: str, description: str) -> metrics._Gauge:
    return _meter().create_gauge(f"{_NAMESPACE}.{name}", description=description)


class Instrumentation:
    """Loop telemetry, emitted through the OpenTelemetry API.

    Nothing is exported until the application installs a provider. Until then
    OpenTelemetry hands back proxy instruments whose methods do nothing, so a
    program that never configures tracing pays only for the calls below - all of
    which sit on paths that are already exceptional. `logfire.configure()` counts
    as installing a provider, as does any other OpenTelemetry setup.
    """

    def report_slow_callback(self, handle: object, duration: float) -> None:
        _counter("slow_callbacks", "Callbacks that exceeded slow_callback_duration").add(1)
        _histogram("callback_duration", "Duration of slow callbacks", "s").record(duration)

        # The loop timed the callback with a monotonic clock, so the span is
        # reconstructed backwards from now rather than started after the fact.
        ended = time.time_ns()
        span = _tracer().start_span(
            f"{_NAMESPACE}.slow_callback",
            start_time=ended - int(duration * 1e9),
            attributes=_without_none(
                {
                    "code.callback": repr(handle),
                    "duration": duration,
                    "asyncio.call_graph": capture_call_graph(handle),
                }
            ),
        )
        span.set_status(Status(StatusCode.ERROR, "callback exceeded slow_callback_duration"))
        span.end(end_time=ended)

    def report_exception(self, context: dict[str, Any]) -> None:
        _counter("unhandled_exceptions", "Exceptions routed to the loop exception handler").add(1)
        exception = context.get("exception")
        message = context.get("message") or "Unhandled exception in event loop"
        attributes = {key: repr(value) for key, value in context.items() if key not in ("message", "exception")}

        span = _tracer().start_span(f"{_NAMESPACE}.unhandled_exception", attributes=attributes)
        if isinstance(exception, BaseException):
            span.record_exception(exception)
        span.set_status(Status(StatusCode.ERROR, message))
        span.end()


def _without_none(attributes: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in attributes.items() if value is not None}


def capture_call_graph(handle: object = None) -> str | None:
    """Render the chain of coroutines a slow callback belongs to.

    Uses `asyncio.format_call_graph`, added in 3.14, so a slow callback carries
    the *reason* it was running rather than just its own repr. A task's step runs
    as a bound method of the task, and the duration is only known once the step
    has returned - by then the task is no longer current, so it is recovered from
    the handle instead.
    """
    task = getattr(getattr(handle, "_callback", None), "__self__", None)
    if not isinstance(task, asyncio.Task):
        task = asyncio.current_task()
    if task is None:
        return None
    return asyncio.format_call_graph(task)


_GAUGES = {
    "loop_count": "libuv loop iterations",
    "events": "Events libuv processed",
    "events_waiting": "Events pending when libuv last polled",
    "idle_time_ns": "Nanoseconds libuv spent idle",
    "callbacks_run": "Callbacks the loop has executed",
    "ready": "Callbacks queued for the next iteration",
    "timers": "Scheduled timers",
    "watchers": "Descriptors watched via add_reader/add_writer",
}


def publish_metrics(snapshot: dict[str, int]) -> None:
    """Record a snapshot taken by the loop's native sampler.

    Deliberately synchronous gauges rather than observable ones: the values are
    loop state, and an observable instrument's callback would run on the
    exporter's collection thread while the loop thread is mutating them.
    """
    for name, value in snapshot.items():
        _gauge(name, _GAUGES[name]).set(value)
