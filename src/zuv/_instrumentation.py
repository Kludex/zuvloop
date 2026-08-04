from __future__ import annotations

import asyncio
import functools
from typing import Any

import logfire_api as logfire

_NAMESPACE = "zuv"


@functools.cache
def _counter(name: str, description: str) -> Any:
    return logfire.DEFAULT_LOGFIRE_INSTANCE.metric_counter(f"{_NAMESPACE}.{name}", description=description)


@functools.cache
def _histogram(name: str, description: str, unit: str) -> Any:
    return logfire.DEFAULT_LOGFIRE_INSTANCE.metric_histogram(f"{_NAMESPACE}.{name}", unit=unit, description=description)


@functools.cache
def _gauge(name: str, description: str) -> Any:
    return logfire.DEFAULT_LOGFIRE_INSTANCE.metric_gauge(f"{_NAMESPACE}.{name}", description=description)


class Instrumentation:
    """Loop telemetry, emitted through logfire.

    Instruments are created on first use rather than with the loop, so a program
    that never hits a slow callback and never raises never touches logfire - and
    `logfire-api` keeps the whole thing a no-op until an application configures it.
    """

    def report_slow_callback(self, handle: object, duration: float) -> None:
        _counter("slow_callbacks", "Callbacks that exceeded slow_callback_duration").add(1)
        _histogram("callback_duration", "Duration of slow callbacks", "s").record(duration)
        logfire.warn(
            "Executing {handle} took {duration} seconds",
            handle=repr(handle),
            duration=duration,
            call_graph=capture_call_graph(handle),
        )

    def report_exception(self, context: dict[str, Any]) -> None:
        _counter("unhandled_exceptions", "Exceptions routed to the loop exception handler").add(1)
        exception = context.get("exception")
        attributes = {key: repr(value) for key, value in context.items() if key not in ("message", "exception")}
        logfire.error(
            context.get("message") or "Unhandled exception in event loop",
            _exc_info=exception if isinstance(exception, BaseException) else False,
            **attributes,
        )


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
    """Receive a snapshot from the loop's native sampler and record it."""
    for name, value in snapshot.items():
        _gauge(name, _GAUGES[name]).set(value)
