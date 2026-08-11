from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping

import pytest
from opentelemetry.trace import StatusCode

import zuvloop
from conftest import Telemetry, attribute, collect_contexts, numeric_attribute, running_loop
from zuvloop._instrumentation import (
    capture_call_graph,
    instrumentation_provider_installed,
    metrics_provider_installed,
    publish_metrics,
    tracing_provider_installed,
)

pytestmark = pytest.mark.anyio


async def test_slow_callbacks_are_reported_without_debug_mode(telemetry: Telemetry) -> None:
    loop = running_loop()
    assert not loop.get_debug()
    loop.slow_callback_duration = 0.01

    def slow_callback() -> None:
        time.sleep(0.05)

    try:
        loop.call_soon(slow_callback)
        await asyncio.sleep(0.1)
    finally:
        loop.slow_callback_duration = 0.1

    span = next(
        span
        for span in telemetry.spans("zuvloop.slow_callback")
        if "slow_callback" in str(attribute(span, "code.callback"))
    )
    # A slow callback is a warning, not an error: the span status stays unset
    # and the severity is carried as Logfire's numeric level.
    assert span.status.status_code is StatusCode.UNSET
    assert numeric_attribute(span, "logfire.level_num") == 13
    assert numeric_attribute(span, "duration") >= 0.01
    assert "Handle" in str(attribute(span, "code.callback"))
    assert telemetry.counted("zuvloop.slow_callbacks") >= 1


async def test_an_infinite_threshold_disables_slow_callback_reports(telemetry: Telemetry) -> None:
    loop = running_loop()
    loop.slow_callback_duration = float("inf")

    def unreported_slow_callback() -> None:
        time.sleep(0.05)

    try:
        loop.call_soon(unreported_slow_callback)
        await asyncio.sleep(0.1)
    finally:
        loop.slow_callback_duration = 0.1

    callbacks = [str(attribute(span, "code.callback")) for span in telemetry.spans("zuvloop.slow_callback")]
    assert not any("unreported_slow_callback" in callback for callback in callbacks)


async def test_a_slow_callback_span_covers_the_time_it_took(telemetry: Telemetry) -> None:
    """The loop times with a monotonic clock, so the span is rebuilt backwards."""
    loop = running_loop()
    loop.set_debug(True)
    loop.slow_callback_duration = 0.01

    def slow_callback() -> None:
        time.sleep(0.05)

    try:
        loop.call_soon(slow_callback)
        await asyncio.sleep(0.1)
    finally:
        loop.set_debug(False)
        loop.slow_callback_duration = 0.1

    span = next(
        span
        for span in telemetry.spans("zuvloop.slow_callback")
        if "slow_callback" in str(attribute(span, "code.callback"))
    )
    assert span.end_time is not None and span.start_time is not None
    measured = (span.end_time - span.start_time) / 1e9
    assert measured == pytest.approx(numeric_attribute(span, "duration"), abs=1e-6)
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
    previous = loop.get_exception_handler()
    seen = collect_contexts(loop)
    assert loop.get_exception_handler() is not None
    try:
        loop.call_soon(lambda: 1 / 0)
        await asyncio.sleep(0.05)
    finally:
        loop.set_exception_handler(previous)
    assert isinstance(seen[0]["exception"], ZeroDivisionError)


async def test_a_failing_exception_handler_falls_back(telemetry: Telemetry) -> None:
    loop = running_loop()

    def broken(_loop: asyncio.AbstractEventLoop, _context: Mapping[str, object]) -> None:
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


async def test_gauges_sample_on_a_native_timer(monkeypatch: pytest.MonkeyPatch) -> None:
    snapshots: list[dict[str, int]] = []
    monkeypatch.setattr("zuvloop._base.publish_metrics", snapshots.append)

    def main() -> None:
        loop = zuvloop.new_event_loop()
        loop.metrics_interval = 0.02
        try:
            loop.run_until_complete(asyncio.sleep(0.09))
        finally:
            loop.close()

    await asyncio.to_thread(main)
    assert len(snapshots) >= 2
    assert snapshots[0].keys() == running_loop()._metrics().keys()
    before = len(snapshots)
    await asyncio.sleep(0.05)
    assert len(snapshots) == before


async def test_monitoring_arms_when_a_provider_appears_mid_run(
    telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Providers configured from inside the loop - logfire.configure() in
    main() - must still arm slow-callback monitoring."""
    installed = False
    monkeypatch.setattr("zuvloop._base.instrumentation_provider_installed", lambda: installed)

    def main() -> None:
        loop = zuvloop.new_event_loop()
        loop.metrics_interval = 0.02
        loop.slow_callback_duration = 0.01

        def blocks_before_provider() -> None:
            time.sleep(0.02)

        def blocks_after_provider() -> None:
            time.sleep(0.02)

        async def scenario() -> None:
            nonlocal installed
            loop.call_soon(blocks_before_provider)
            await asyncio.sleep(0.01)
            installed = True
            # More than metrics_interval, so the sampler has re-checked.
            await asyncio.sleep(0.06)
            loop.call_soon(blocks_after_provider)
            await asyncio.sleep(0.05)

        try:
            loop.run_until_complete(scenario())
        finally:
            loop.close()

    await asyncio.to_thread(main)
    callbacks = [str(attribute(span, "code.callback")) for span in telemetry.spans("zuvloop.slow_callback")]
    assert not any("blocks_before_provider" in callback for callback in callbacks)
    assert any("blocks_after_provider" in callback for callback in callbacks)


async def test_provider_checks_stop_once_a_provider_is_found(monkeypatch: pytest.MonkeyPatch) -> None:
    """The armed flags are latches: after a provider says yes, later sampler
    ticks publish without asking again."""
    snapshots: list[dict[str, int]] = []
    checks = 0
    installed = False

    def probe() -> bool:
        nonlocal checks
        checks += 1
        return installed

    monkeypatch.setattr("zuvloop._base.publish_metrics", snapshots.append)
    monkeypatch.setattr("zuvloop._base.metrics_provider_installed", probe)

    def main() -> None:
        loop = zuvloop.new_event_loop()
        loop.metrics_interval = 0.02

        async def scenario() -> None:
            nonlocal installed
            await asyncio.sleep(0.05)
            assert not snapshots
            installed = True
            await asyncio.sleep(0.05)
            assert snapshots
            checks_when_armed = checks
            published = len(snapshots)
            await asyncio.sleep(0.05)
            assert len(snapshots) > published
            assert checks == checks_when_armed

        try:
            loop.run_until_complete(scenario())
        finally:
            loop.close()

    await asyncio.to_thread(main)


async def test_gauges_stay_unpublished_without_a_meter_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    snapshots: list[dict[str, int]] = []
    monkeypatch.setattr("zuvloop._base.publish_metrics", snapshots.append)
    monkeypatch.setattr("zuvloop._base.metrics_provider_installed", lambda: False)

    def main() -> None:
        loop = zuvloop.new_event_loop()
        loop.metrics_interval = 0.02
        try:
            loop.run_until_complete(asyncio.sleep(0.07))
        finally:
            loop.close()

    await asyncio.to_thread(main)
    assert snapshots == []


async def test_gauges_reach_the_exporter(telemetry: Telemetry) -> None:
    def main() -> None:
        loop = zuvloop.new_event_loop()
        loop.metrics_interval = 0.02
        try:
            loop.run_until_complete(asyncio.sleep(0.05))
        finally:
            loop.close()

    await asyncio.to_thread(main)
    assert telemetry.counted("zuvloop.callbacks_run") > 0


def test_real_providers_are_detected(telemetry: Telemetry) -> None:
    # The telemetry fixture installs SDK providers for the whole session.
    assert instrumentation_provider_installed()
    assert tracing_provider_installed()
    assert metrics_provider_installed()


def test_a_metrics_provider_enables_instrumentation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("zuvloop._instrumentation.tracing_provider_installed", lambda: False)
    monkeypatch.setattr("zuvloop._instrumentation.metrics_provider_installed", lambda: True)
    assert instrumentation_provider_installed()


def test_no_provider_disables_instrumentation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("zuvloop._instrumentation.tracing_provider_installed", lambda: False)
    monkeypatch.setattr("zuvloop._instrumentation.metrics_provider_installed", lambda: False)
    assert not instrumentation_provider_installed()


def test_a_noop_tracing_provider_is_not_detected(monkeypatch: pytest.MonkeyPatch) -> None:
    from opentelemetry.trace import NoOpTracerProvider

    monkeypatch.setattr("zuvloop._instrumentation.trace.get_tracer_provider", lambda: NoOpTracerProvider())
    assert not tracing_provider_installed()


def test_an_unset_proxy_tracing_provider_is_not_detected(monkeypatch: pytest.MonkeyPatch) -> None:
    from opentelemetry.trace import ProxyTracerProvider

    monkeypatch.setattr("zuvloop._instrumentation.trace.get_tracer_provider", lambda: ProxyTracerProvider())
    assert not tracing_provider_installed()


def test_metrics_only_slow_callbacks_skip_span_construction(
    telemetry: Telemetry, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("zuvloop._instrumentation.tracing_provider_installed", lambda: False)

    captured_call_graphs: list[object] = []
    monkeypatch.setattr("zuvloop._instrumentation.capture_call_graph", captured_call_graphs.append)
    zuvloop.Instrumentation().report_slow_callback(object(), 0.2)

    assert captured_call_graphs == []
    assert telemetry.spans("zuvloop.slow_callback") == []
    assert telemetry.counted("zuvloop.slow_callbacks") >= 1


def test_a_noop_provider_is_not_detected(monkeypatch: pytest.MonkeyPatch) -> None:
    from opentelemetry.metrics import NoOpMeterProvider

    monkeypatch.setattr("zuvloop._instrumentation.metrics.get_meter_provider", NoOpMeterProvider)
    assert not metrics_provider_installed()


def test_an_unset_proxy_provider_is_not_detected(monkeypatch: pytest.MonkeyPatch) -> None:
    from opentelemetry.metrics._internal import _ProxyMeterProvider

    monkeypatch.setattr("zuvloop._instrumentation.metrics.get_meter_provider", _ProxyMeterProvider)
    assert not metrics_provider_installed()


async def test_gauges_stay_off_without_a_meter_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    snapshots: list[dict[str, int]] = []
    monkeypatch.setattr("zuvloop._base.publish_metrics", snapshots.append)
    monkeypatch.setattr("zuvloop._base.metrics_provider_installed", lambda: False)

    def main() -> None:
        loop = zuvloop.new_event_loop()
        loop.metrics_interval = 0.02
        try:
            loop.run_until_complete(asyncio.sleep(0.09))
        finally:
            loop.close()

    await asyncio.to_thread(main)
    assert snapshots == []


async def test_the_sampling_interval_must_be_positive() -> None:
    loop = running_loop()
    with pytest.raises(ValueError, match="must be positive"):
        loop._start_metrics(0, print)


async def test_a_failing_sampler_callback_is_reported() -> None:
    loop = running_loop()

    def boom(_snapshot: dict[str, int]) -> None:
        raise RuntimeError("sampler failed")

    previous = loop.get_exception_handler()
    seen = collect_contexts(loop)
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
