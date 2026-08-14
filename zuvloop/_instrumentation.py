from __future__ import annotations

import asyncio
import functools
import time
from collections.abc import Mapping

from opentelemetry import metrics, trace
from opentelemetry.metrics import NoOpMeterProvider
from opentelemetry.trace import NoOpTracerProvider, ProxyTracerProvider, Status, StatusCode
from opentelemetry.util.types import AttributeValue

_NAMESPACE = "zuvloop"
_MAX_ATTRIBUTE_LENGTH = 4096


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
        try:
            _counter("slow_callbacks", "Callbacks that exceeded slow_callback_duration").add(1)
            _histogram("callback_duration", "Duration of slow callbacks", "s").record(duration)
            if not tracing_provider_installed():
                return

            # The loop timed the callback with a monotonic clock, so the span is
            # reconstructed backwards from now rather than started after the fact.
            ended = time.time_ns()
            graph = capture_call_graph(handle)
            attributes: dict[str, AttributeValue] = {
                "code.callback": _safe_repr(handle),
                "duration": duration,
                "logfire.level_num": 13,
            }
            if graph is not None:
                attributes["asyncio.call_graph"] = _bounded(graph)
            span = _tracer().start_span(
                f"{_NAMESPACE}.slow_callback",
                start_time=ended - int(duration * 1e9),
                attributes=attributes,
            )
            span.end(end_time=ended)
        except BaseException:
            # Instrumentation runs on exception and slow-callback paths. An
            # exporter, provider, user __repr__, or call-graph failure must not
            # replace the application failure that led here.
            return

    def report_exception(self, context: Mapping[str, object]) -> None:
        try:
            _counter("unhandled_exceptions", "Exceptions routed to the loop exception handler").add(1)
            exception = context.get("exception")
            message = context.get("message") or "Unhandled exception in event loop"
            attributes = {
                key: _safe_repr(value) for key, value in context.items() if key not in ("message", "exception")
            }

            span = _tracer().start_span(f"{_NAMESPACE}.unhandled_exception", attributes=attributes)
            if isinstance(exception, BaseException):
                span.record_exception(exception)
            span.set_status(Status(StatusCode.ERROR, _bounded(str(message))))
            span.end()
        except BaseException:
            return


def _bounded(value: str) -> str:
    if len(value) <= _MAX_ATTRIBUTE_LENGTH:
        return value
    return value[: _MAX_ATTRIBUTE_LENGTH - 1] + "…"


def _safe_repr(value: object) -> str:
    try:
        return _bounded(repr(value))
    except BaseException:
        return f"<{type(value).__name__} repr failed>"


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
    if task is None or task.done():
        return None
    return asyncio.format_call_graph(task)


def tracing_provider_installed() -> bool:
    """Whether the application has installed a real tracing provider."""
    provider = trace.get_tracer_provider()
    return not isinstance(provider, (NoOpTracerProvider, ProxyTracerProvider))


def instrumentation_provider_installed() -> bool:
    """Whether slow callbacks have somewhere to be exported."""
    return tracing_provider_installed() or metrics_provider_installed()


def metrics_provider_installed() -> bool:
    """Whether the application has installed a real meter provider.

    Before that happens, OpenTelemetry hands out proxies whose instruments do
    nothing - sampling the loop to feed them would be pure overhead.
    """
    provider = metrics.get_meter_provider()
    if isinstance(provider, NoOpMeterProvider):
        return False
    real = getattr(provider, "_real_meter_provider", _MISSING)
    # A _ProxyMeterProvider delegates only once set_meter_provider() ran.
    return real is _MISSING or real is not None


_MISSING = object()


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
