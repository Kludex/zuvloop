from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any, TypeVar

from ._instrumentation import publish_metrics
from ._loop import EventLoop

_T = TypeVar("_T")


def new_event_loop() -> EventLoop:
    """Create a libuv-backed event loop."""
    return EventLoop()


def run(main: Coroutine[Any, Any, _T], *, debug: bool | None = None) -> _T:
    """Run `main` on a libuv-backed loop, mirroring `asyncio.run`."""
    with asyncio.Runner(debug=debug, loop_factory=new_event_loop) as runner:
        return runner.run(main)


class MetricsReporter:
    """Publishes the loop's counters to logfire.

    Sampling runs on a dedicated libuv timer inside the extension, so it never
    enters the callback queue; Python is handed a finished snapshot.
    """

    def __init__(self, loop: EventLoop, interval: float) -> None:
        self._loop = loop
        loop._start_metrics(interval, publish_metrics)

    def cancel(self) -> None:
        self._loop._stop_metrics()


def instrument(loop: EventLoop | None = None, *, interval: float = 10.0) -> MetricsReporter:
    """Report loop counters to logfire every `interval` seconds.

    Slow callbacks and unhandled exceptions are always reported; this adds the
    periodic gauges, which need a running loop to sample from.
    """
    target = loop if loop is not None else asyncio.get_running_loop()
    if not isinstance(target, EventLoop):
        raise TypeError(f"zuv.instrument() needs a zuv event loop, got {target!r}")
    return MetricsReporter(target, interval)
