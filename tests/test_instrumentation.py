from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
from opentelemetry.trace import StatusCode

import zuvloop
from conftest import Telemetry, attribute, running_loop
from zuvloop._instrumentation import capture_call_graph, publish_metrics

pytestmark = pytest.mark.anyio


async def test_slow_callbacks_are_reported(telemetry: Telemetry) -> None:
    loop = running_loop()
    loop.set_debug(True)
    loop.slow_callback_duration = 0.01
    try:
        loop.call_soon(time.sleep, 0.05)
        await asyncio.sleep(0.1)
    finally:
        loop.set_debug(False)
        loop.slow_callback_duration = 0.1

    span = telemetry.spans("zuvloop.slow_callback")[0]
    assert span.status.status_code is StatusCode.ERROR
    assert attribute(span, "duration") >= 0.01
    assert "Handle" in str(attribute(span, "code.callback"))
    assert telemetry.counted("zuvloop.slow_callbacks") >= 1


async def test_a_slow_callback_span_covers_the_time_it_took(telemetry: Telemetry) -> None:
    """The loop times with a monotonic clock, so the span is rebuilt backwards."""
    loop = running_loop()
    loop.set_debug(True)
    loop.slow_callback_duration = 0.01
    try:
        loop.call_soon(time.sleep, 0.05)
        await asyncio.sleep(0.1)
    finally:
        loop.set_debug(False)
        loop.slow_callback_duration = 0.1

    span = telemetry.spans("zuvloop.slow_callback")[0]
    assert span.end_time is not None and span.start_time is not None
    measured = (span.end_time - span.start_time) / 1e9
    assert measured == pytest.approx(attribute(span, "duration"), abs=1e-6)
    assert measured >= 0.05


async def test_slow_callbacks_inside_a_task_carry_the_call_graph(telemetry: Telemetry) -> None:
    loop = running_loop()
    loop.set_debug(True)
    loop.slow_callback_duration = 0.01

    async def slow() -> None:
        time.sleep(0.05)
        await asyncio.sleep(0.05)

    try:
        await loop.create_task(slow(), name="slow-step")
    finally:
        loop.set_debug(False)
        loop.slow_callback_duration = 0.1

    spans = telemetry.spans("zuvloop.slow_callback")
    graphs = [span.attributes.get("asyncio.call_graph") for span in spans if span.attributes]
    assert any(graph is not None and "slow-step" in str(graph) for graph in graphs)


async def test_the_call_graph_is_absent_outside_a_task() -> None:
    loop = running_loop()
    captured = loop.create_future()
    loop.call_soon(lambda: captured.set_result(capture_call_graph(loop.call_soon(print))))
    assert await captured is None


async def test_unhandled_exceptions_are_reported(telemetry: Telemetry) -> None:
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

    span = telemetry.spans("zuvloop.unhandled_exception")[0]
    assert span.status.status_code is StatusCode.ERROR
    assert span.status.description == "Exception in callback"
    assert "Handle" in str(attribute(span, "handle"))
    assert span.events[0].name == "exception"
    assert span.events[0].attributes is not None
    assert span.events[0].attributes["exception.type"] == "ValueError"
    assert telemetry.counted("zuvloop.unhandled_exceptions") >= 1


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


async def test_a_failing_exception_handler_falls_back(telemetry: Telemetry) -> None:
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

    descriptions = [span.status.description for span in telemetry.spans("zuvloop.unhandled_exception")]
    assert "Unhandled error in exception handler" in descriptions


async def test_a_context_without_an_exception_is_still_reported(telemetry: Telemetry) -> None:
    loop = running_loop()
    loop.default_exception_handler({"message": "just a note", "detail": 42})
    span = telemetry.spans("zuvloop.unhandled_exception")[0]
    assert span.status.description == "just a note"
    assert attribute(span, "detail") == "42"
    assert span.events == ()


async def test_a_context_without_a_message_gets_a_default(telemetry: Telemetry) -> None:
    loop = running_loop()
    loop.default_exception_handler({})
    assert telemetry.spans("zuvloop.unhandled_exception")[0].status.description == "Unhandled exception in event loop"


async def test_metrics_are_published_as_gauges(telemetry: Telemetry) -> None:
    publish_metrics({"ready": 3, "timers": 4})
    assert telemetry.metric("zuvloop.ready") == 3
    assert telemetry.metric("zuvloop.timers") == 4


async def test_instrument_samples_on_a_native_timer(monkeypatch: pytest.MonkeyPatch) -> None:
    snapshots: list[dict[str, int]] = []
    monkeypatch.setattr("zuvloop._runner.publish_metrics", snapshots.append)
    reporter = zuvloop.instrument(interval=0.02)
    try:
        await asyncio.sleep(0.09)
    finally:
        reporter.cancel()
    assert len(snapshots) >= 2
    assert snapshots[0].keys() == running_loop()._metrics().keys()
    before = len(snapshots)
    await asyncio.sleep(0.05)
    assert len(snapshots) == before


async def test_instrument_reaches_the_exporter(telemetry: Telemetry) -> None:
    reporter = zuvloop.instrument(interval=0.02)
    try:
        await asyncio.sleep(0.05)
    finally:
        reporter.cancel()
    assert telemetry.counted("zuvloop.callbacks_run") > 0


async def test_instrument_accepts_an_explicit_loop() -> None:
    reporter = zuvloop.instrument(running_loop(), interval=5.0)
    reporter.cancel()


async def test_instrument_rejects_a_foreign_loop() -> None:
    other = asyncio.new_event_loop()
    try:
        with pytest.raises(TypeError, match="needs a zuvloop event loop"):
            zuvloop.instrument(other)  # type: ignore[arg-type]
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
    loop = zuvloop.new_event_loop()
    loop.close()
    assert loop._metrics()["loop_count"] == 0


async def test_the_stdlib_call_graph_apis_work_on_this_loop() -> None:
    """3.14's introspection reads real asyncio.Task objects, which is what zuvloop schedules."""
    import io

    started = asyncio.Event()

    async def leaf() -> None:
        started.set()
        await asyncio.sleep(5)

    async def branch() -> None:
        await leaf()

    task = running_loop().create_task(branch(), name="introspected-branch")
    await started.wait()

    graph = asyncio.capture_call_graph(task)
    assert graph is not None
    assert graph.future is task

    rendered = asyncio.format_call_graph(task)
    assert "leaf" in rendered

    buffer = io.StringIO()
    asyncio.print_call_graph(task, file=buffer)
    assert "branch" in buffer.getvalue()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
