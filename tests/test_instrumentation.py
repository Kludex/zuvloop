from __future__ import annotations

import asyncio
import time
from typing import Any

import logfire_api as logfire
import pytest

import zuv
from conftest import running_loop
from zuv._instrumentation import capture_call_graph, publish_metrics

pytestmark = pytest.mark.anyio


class Recorder:
    """Captures what the instrumentation layer sends to logfire."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.warnings: list[tuple[str, dict[str, Any]]] = []
        self.errors: list[tuple[str, dict[str, Any]]] = []
        monkeypatch.setattr(logfire, "warn", self._warn)
        monkeypatch.setattr(logfire, "error", self._error)

    def _warn(self, message: str, **attributes: Any) -> None:
        self.warnings.append((message, attributes))

    def _error(self, message: str, **attributes: Any) -> None:
        self.errors.append((message, attributes))


@pytest.fixture
def recorder(monkeypatch: pytest.MonkeyPatch) -> Recorder:
    return Recorder(monkeypatch)


async def test_slow_callbacks_are_reported(recorder: Recorder) -> None:
    loop = running_loop()
    loop.set_debug(True)
    loop.slow_callback_duration = 0.01
    try:
        loop.call_soon(time.sleep, 0.05)
        await asyncio.sleep(0.1)
    finally:
        loop.set_debug(False)
        loop.slow_callback_duration = 0.1
    assert recorder.warnings
    message, attributes = recorder.warnings[0]
    assert "took" in message
    assert attributes["duration"] >= 0.01
    assert "Handle" in attributes["handle"]


async def test_slow_callbacks_inside_a_task_carry_the_call_graph(recorder: Recorder) -> None:
    loop = running_loop()
    loop.set_debug(True)
    loop.slow_callback_duration = 0.01

    async def slow() -> None:
        time.sleep(0.05)
        await asyncio.sleep(0.05)

    try:
        task = loop.create_task(slow(), name="slow-step")
        await task
    finally:
        loop.set_debug(False)
        loop.slow_callback_duration = 0.1

    graphs = [attributes["call_graph"] for _message, attributes in recorder.warnings]
    assert any(graph is not None and "slow-step" in graph for graph in graphs)


async def test_the_call_graph_is_absent_outside_a_task() -> None:
    loop = running_loop()
    captured = loop.create_future()
    loop.call_soon(lambda: captured.set_result(capture_call_graph(loop.call_soon(print))))
    assert await captured is None


async def test_unhandled_exceptions_are_reported(recorder: Recorder) -> None:
    loop = running_loop()

    def boom() -> None:
        raise ValueError("kaboom")

    previous = loop.get_exception_handler()
    loop.set_exception_handler(None)
    try:
        loop.call_soon(boom)
        await asyncio.sleep(0.05)
    finally:
        loop.set_exception_handler(previous)
    assert recorder.errors
    message, attributes = recorder.errors[0]
    assert message == "Exception in callback"
    assert isinstance(attributes["_exc_info"], ValueError)
    assert "Handle" in attributes["handle"]


async def test_a_custom_exception_handler_takes_over() -> None:
    loop = running_loop()
    seen: list[dict[str, Any]] = []
    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: seen.append(context))
    assert loop.get_exception_handler() is not None
    try:
        loop.call_soon(lambda: 1 / 0)
        await asyncio.sleep(0.05)
    finally:
        loop.set_exception_handler(previous)
    assert isinstance(seen[0]["exception"], ZeroDivisionError)


async def test_a_failing_exception_handler_falls_back(recorder: Recorder) -> None:
    loop = running_loop()

    def broken(_loop: Any, _context: dict[str, Any]) -> None:
        raise RuntimeError("handler is broken")

    previous = loop.get_exception_handler()
    loop.set_exception_handler(broken)
    try:
        loop.call_soon(lambda: 1 / 0)
        await asyncio.sleep(0.05)
    finally:
        loop.set_exception_handler(previous)
    assert any(message == "Unhandled error in exception handler" for message, _ in recorder.errors)


async def test_a_context_without_an_exception_is_still_reported(recorder: Recorder) -> None:
    loop = running_loop()
    loop.default_exception_handler({"message": "just a note", "detail": 42})
    assert ("just a note", {"_exc_info": False, "detail": "42"}) in recorder.errors


async def test_a_context_without_a_message_gets_a_default(recorder: Recorder) -> None:
    loop = running_loop()
    loop.default_exception_handler({})
    assert recorder.errors[0][0] == "Unhandled exception in event loop"


async def test_metrics_are_published_to_gauges(monkeypatch: pytest.MonkeyPatch) -> None:
    recorded: dict[str, int] = {}

    class Gauge:
        def __init__(self, name: str) -> None:
            self.name = name

        def set(self, value: int) -> None:
            recorded[self.name] = value

    monkeypatch.setattr("zuv._instrumentation._gauge", lambda name, _description: Gauge(name))
    publish_metrics({"ready": 3, "timers": 4})
    assert recorded == {"ready": 3, "timers": 4}


async def test_instrument_samples_on_a_native_timer(monkeypatch: pytest.MonkeyPatch) -> None:
    snapshots: list[dict[str, int]] = []
    monkeypatch.setattr("zuv._runner.publish_metrics", snapshots.append)
    reporter = zuv.instrument(interval=0.02)
    try:
        await asyncio.sleep(0.09)
    finally:
        reporter.cancel()
    assert len(snapshots) >= 2
    assert snapshots[0].keys() == running_loop()._metrics().keys()
    before = len(snapshots)
    await asyncio.sleep(0.05)
    assert len(snapshots) == before


async def test_instrument_accepts_an_explicit_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("zuv._runner.publish_metrics", lambda _snapshot: None)
    loop = running_loop()
    reporter = zuv.instrument(loop, interval=5.0)
    reporter.cancel()


async def test_instrument_rejects_a_foreign_loop() -> None:
    other = asyncio.new_event_loop()
    try:
        with pytest.raises(TypeError, match="needs a zuv event loop"):
            zuv.instrument(other)  # type: ignore[arg-type]
    finally:
        other.close()


async def test_the_sampling_interval_must_be_positive() -> None:
    loop = running_loop()
    with pytest.raises(ValueError, match="must be positive"):
        loop._start_metrics(0, print)


async def test_a_failing_sampler_callback_is_reported() -> None:
    loop = running_loop()
    seen: list[dict[str, Any]] = []

    def boom(_snapshot: dict[str, int]) -> None:
        raise RuntimeError("sampler failed")

    previous = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: seen.append(context))
    loop._start_metrics(0.02, boom)
    try:
        await asyncio.sleep(0.06)
    finally:
        loop._stop_metrics()
        loop.set_exception_handler(previous)
    assert seen[0]["message"] == "Exception in the metrics sampler"


async def test_metrics_on_a_closed_loop_are_zero() -> None:
    loop = zuv.new_event_loop()
    loop.close()
    assert loop._metrics()["loop_count"] == 0


async def test_publishing_metrics_reaches_real_gauges() -> None:
    loop = running_loop()
    publish_metrics(loop._metrics())
