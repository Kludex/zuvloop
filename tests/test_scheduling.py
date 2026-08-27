from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import threading
import time
from collections.abc import Coroutine

import pytest

import zuvloop
from tests.conftest import running_loop

pytestmark = pytest.mark.anyio


async def test_call_soon_runs_in_order() -> None:
    loop = running_loop()
    seen: list[int] = []
    for index in range(5):
        loop.call_soon(seen.append, index)
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert seen == [0, 1, 2, 3, 4]


async def test_call_soon_passes_many_arguments() -> None:
    loop = running_loop()
    captured: list[tuple[int, ...]] = []
    loop.call_soon(lambda *args: captured.append(args), 1, 2, 3, 4, 5, 6)
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert captured == [(1, 2, 3, 4, 5, 6)]


async def test_call_soon_runs_in_the_calling_context() -> None:
    variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable")
    variable.set("outer")
    loop = running_loop()
    seen: list[str] = []
    loop.call_soon(lambda: seen.append(variable.get()))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert seen == ["outer"]


async def test_call_soon_accepts_an_explicit_context() -> None:
    variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable", default="default")
    context = contextvars.copy_context()
    context.run(variable.set, "explicit")
    loop = running_loop()
    seen: list[str] = []
    loop.call_soon(lambda: seen.append(variable.get()), context=context)
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert seen == ["explicit"]


def test_call_soon_keeps_empty_contexts_independent() -> None:
    loop = zuvloop.new_event_loop()
    variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable", default="default")
    seen: list[str] = []

    loop.call_soon(variable.set, "first callback")
    loop.call_soon(lambda: seen.append(variable.get()))
    loop.call_soon(loop.stop)
    try:
        loop.run_forever()
    finally:
        loop.close()
    assert seen == ["default"]


def test_call_soon_exposes_its_context_before_running() -> None:
    loop = zuvloop.new_event_loop()
    variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable", default="default")
    seen: list[str] = []
    handle = loop.call_soon(lambda: seen.append(variable.get()))
    context = handle.get_context()
    context.run(variable.set, "modified")
    loop.call_soon(loop.stop)
    try:
        loop.run_forever()
    finally:
        loop.close()
    assert handle.get_context() is context
    assert seen == ["modified"]


async def test_call_soon_rejects_unknown_keywords() -> None:
    loop = running_loop()
    with pytest.raises(TypeError, match="context"):
        loop.call_soon(print, unexpected=1)  # type: ignore[call-arg]


async def test_call_soon_requires_a_callback() -> None:
    loop = running_loop()
    with pytest.raises(TypeError, match="requires a callback"):
        loop.call_soon()  # type: ignore[call-arg]


async def test_cancelled_callback_does_not_run() -> None:
    loop = running_loop()
    seen: list[str] = []
    handle = loop.call_soon(seen.append, "nope")
    handle.cancel()
    assert handle.cancelled() is True
    assert "cancelled" in repr(handle)
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert seen == []


@pytest.mark.parametrize("args", [(1, 2, 3), (1, 2, 3, 4, 5, 6)])
async def test_callback_can_cancel_its_own_handle_during_vectorcall(args: tuple[int, ...]) -> None:
    loop = running_loop()
    handles: list[zuvloop.Handle] = []
    seen: list[tuple[int, ...]] = []

    def callback(*received: int) -> None:
        handles[0].cancel()
        seen.append(received)

    handles.append(loop.call_soon(callback, *args))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert seen == [args]
    assert handles[0].cancelled()


async def test_handle_repr_names_the_callback() -> None:
    loop = running_loop()
    handle = loop.call_soon(print)
    assert "print" in repr(handle)
    handle.cancel()


async def test_call_later_never_fires_early() -> None:
    loop = running_loop()
    delay = 0.05
    started = loop.time()
    await asyncio.sleep(delay)
    assert loop.time() - started >= delay


async def test_call_later_ordering() -> None:
    loop = running_loop()
    seen: list[str] = []
    loop.call_later(0.03, seen.append, "third")
    loop.call_later(0.01, seen.append, "first")
    loop.call_later(0.02, seen.append, "second")
    await asyncio.sleep(0.08)
    assert seen == ["first", "second", "third"]


@pytest.mark.parametrize("delay", [float("inf"), 1e300])
async def test_oversized_timer_delays_are_effectively_infinite(delay: float) -> None:
    loop = running_loop()
    seen: list[str] = []
    later = loop.call_later(delay, seen.append, "later")
    absolute = loop.call_at(delay, seen.append, "absolute")
    try:
        await asyncio.wait_for(asyncio.sleep(0.01), timeout=float("inf"))
        assert seen == []
    finally:
        later.cancel()
        absolute.cancel()


async def test_loop_time_is_time_monotonic() -> None:
    # Not merely monotonic: callers mix the two clocks, so they have to be one.
    loop = running_loop()
    assert loop.time() == pytest.approx(time.monotonic(), abs=0.01)


async def test_call_at_accepts_a_time_monotonic_deadline() -> None:
    loop = running_loop()
    done = loop.create_future()
    loop.call_at(time.monotonic() + 0.01, done.set_result, None)
    await asyncio.wait_for(done, 1)


async def test_call_at_preserves_the_deadline_number_type() -> None:
    loop = running_loop()
    integer = loop.call_at(2**31, print)
    floating = loop.call_at(float(2**31), print)
    try:
        assert type(integer.when()) is int
        assert type(integer._when) is int
        assert type(floating.when()) is float
        assert type(floating._when) is float
    finally:
        integer.cancel()
        floating.cancel()


async def test_call_at_uses_the_loop_clock() -> None:
    loop = running_loop()
    done = loop.create_future()
    handle = loop.call_at(loop.time() + 0.01, done.set_result, None)
    assert handle.when() > loop.time()
    await done


async def test_call_later_returns_a_real_timer_handle() -> None:
    """`call_soon` trades this for a leaner handle; a timer can afford the base."""
    loop = running_loop()
    handle = loop.call_later(30, print)
    assert isinstance(handle, asyncio.TimerHandle)
    assert isinstance(handle, asyncio.Handle)
    handle.cancel()


async def test_timer_handles_order_by_their_deadline() -> None:
    """The ordering comes from the base class, which reads `_when`."""
    loop = running_loop()
    later = loop.call_later(60, print)
    sooner = loop.call_later(30, print)

    assert sooner < later
    assert later > sooner
    assert sorted([later, sooner]) == [sooner, later]
    assert sooner == sooner
    assert sooner != later
    assert isinstance(hash(sooner), int)
    assert sooner._when < later._when
    assert sooner._cancelled is False
    assert sooner._scheduled is True
    assert sooner._callback is print
    assert sooner._args == ()

    sooner.cancel()
    later.cancel()
    assert sooner.cancelled() and later.cancelled()
    # Cancelling drops the arguments rather than emptying them, and leaves the
    # handle in the heap until something compacts it - both as asyncio reports.
    assert sooner._callback is None
    assert sooner._args is None
    assert sooner._scheduled is True


async def test_a_fired_timer_is_no_longer_scheduled() -> None:
    """`BaseEventLoop` clears the flag when the handle leaves its heap for the
    ready queue, and so does the native one."""
    loop = running_loop()
    done = loop.create_future()
    handle = loop.call_later(0.01, done.set_result, None)
    assert handle._scheduled is True
    await done
    await asyncio.sleep(0)
    assert handle._scheduled is False


async def test_a_cancelled_timer_stops_being_scheduled_when_it_leaves_the_heap() -> None:
    """Cancelling alone leaves it in the heap, as it does in asyncio. Reaching the
    deadline retires it, and so does the compaction a run of cancellations sets off."""
    loop = running_loop()
    due = loop.call_later(0.01, print)
    due.cancel()
    assert due._scheduled is True

    pending = loop.call_later(30, print)
    pending.cancel()
    for _ in range(400):
        loop.call_later(30, print).cancel()

    await asyncio.sleep(0.05)
    assert due._scheduled is False
    assert pending._scheduled is False


async def test_call_later_requires_a_delay_and_a_callback() -> None:
    loop = running_loop()
    with pytest.raises(TypeError, match="delay and a callback"):
        loop.call_later(0.1)  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="time and a callback"):
        loop.call_at(0.1)  # type: ignore[call-arg]


async def test_negative_delay_runs_promptly() -> None:
    loop = running_loop()
    done = loop.create_future()
    loop.call_later(-10, done.set_result, "now")
    assert await done == "now"


async def test_cancelling_many_timers_compacts_the_heap() -> None:
    loop = running_loop()
    handles = [loop.call_later(30, print) for _ in range(400)]
    for handle in handles:
        handle.cancel()
    assert loop._metrics()["timers"] <= 400
    done = loop.create_future()
    loop.call_later(0.01, done.set_result, None)
    await done


async def test_call_soon_threadsafe_from_another_thread() -> None:
    loop = running_loop()
    done = loop.create_future()

    def worker() -> None:
        loop.call_soon_threadsafe(done.set_result, "from thread")

    threading.Thread(target=worker).start()
    assert await done == "from thread"


def test_call_soon_threadsafe_accepts_concurrent_producers() -> None:
    loop = zuvloop.new_event_loop()
    worker_count = 8
    callbacks_per_worker = 2_000
    expected = worker_count * callbacks_per_worker
    seen = 0

    def callback() -> None:
        nonlocal seen
        seen += 1
        if seen == expected:
            loop.stop()

    def producer() -> None:
        for _ in range(callbacks_per_worker):
            loop.call_soon_threadsafe(callback)

    threads = [threading.Thread(target=producer) for _ in range(worker_count)]
    for thread in threads:
        thread.start()
    try:
        loop.run_forever()
    finally:
        for thread in threads:
            thread.join()
        loop.close()
    assert seen == expected


def test_call_soon_threadsafe_captures_the_worker_context() -> None:
    loop = zuvloop.new_event_loop()
    variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable", default="default")
    seen: list[str] = []

    def worker() -> None:
        variable.set("scheduled")
        loop.call_soon_threadsafe(lambda: seen.append(variable.get()))
        variable.set("changed later")
        loop.call_soon_threadsafe(loop.stop)

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join()
    try:
        loop.run_forever()
    finally:
        loop.close()
    assert seen == ["scheduled"]


def test_call_soon_threadsafe_keeps_empty_contexts_independent() -> None:
    loop = zuvloop.new_event_loop()
    variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable", default="default")
    seen: list[str] = []

    loop.call_soon_threadsafe(variable.set, "first callback")
    loop.call_soon_threadsafe(lambda: seen.append(variable.get()))
    loop.call_soon_threadsafe(loop.stop)
    try:
        loop.run_forever()
    finally:
        loop.close()
    assert seen == ["default"]


def test_call_soon_threadsafe_exposes_its_context_before_running() -> None:
    loop = zuvloop.new_event_loop()
    variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable", default="default")
    seen: list[str] = []
    handle = loop.call_soon_threadsafe(lambda: seen.append(variable.get()))
    context = handle.get_context()
    context.run(variable.set, "modified")
    loop.call_soon_threadsafe(loop.stop)
    try:
        loop.run_forever()
    finally:
        loop.close()
    assert handle.get_context() is context
    assert seen == ["modified"]


async def test_call_soon_threadsafe_validates_its_arguments() -> None:
    loop = running_loop()
    with pytest.raises(TypeError, match="requires a callback"):
        loop.call_soon_threadsafe()  # type: ignore[call-arg]


async def test_call_soon_threadsafe_returns_the_stdlib_threadsafe_handle() -> None:
    loop = running_loop()
    handle = loop.call_soon_threadsafe(lambda: None)
    assert isinstance(handle, asyncio.events._ThreadSafeHandle)  # type: ignore[attr-defined]
    assert isinstance(handle, asyncio.Handle)
    handle.cancel()


def test_threadsafe_cancel_from_another_thread_waits_for_the_running_callback() -> None:
    loop = zuvloop.new_event_loop()
    started = threading.Event()
    finished = threading.Event()
    results: list[str] = []
    observed: list[bool] = []

    def callback(arg: str) -> None:
        started.set()
        results.append(arg)
        time.sleep(0.2)
        finished.set()

    def canceller() -> None:
        try:
            handle = loop.call_soon_threadsafe(callback, "ran")
            started.wait(5)
            handle.cancel()
            observed.append(finished.is_set())
            observed.append(handle.cancelled())
        finally:
            loop.call_soon_threadsafe(loop.stop)

    thread = threading.Thread(target=canceller)
    loop.call_soon(thread.start)
    loop.run_forever()
    thread.join()
    loop.close()
    assert results == ["ran"]
    assert observed == [True, True]


def test_threadsafe_cancelled_from_another_thread_waits_for_the_running_callback() -> None:
    loop = zuvloop.new_event_loop()
    started = threading.Event()
    finished = threading.Event()
    observed: list[bool] = []

    def callback() -> None:
        started.set()
        time.sleep(0.2)
        finished.set()

    def checker() -> None:
        try:
            handle = loop.call_soon_threadsafe(callback)
            started.wait(5)
            observed.append(handle.cancelled())
            observed.append(finished.is_set())
        finally:
            loop.call_soon_threadsafe(loop.stop)

    thread = threading.Thread(target=checker)
    loop.call_soon(thread.start)
    loop.run_forever()
    thread.join()
    loop.close()
    assert observed == [False, True]


def test_threadsafe_cancel_inside_its_own_callback_completes_the_run() -> None:
    loop = zuvloop.new_event_loop()
    handle_ready: concurrent.futures.Future[asyncio.Handle] = concurrent.futures.Future()
    results: list[str] = []
    observed: list[bool] = []

    def callback(arg: str) -> None:
        try:
            handle = handle_ready.result(5)
            handle.cancel()
            observed.append(handle.cancelled())
            results.append(arg)
        finally:
            loop.stop()

    def scheduler() -> None:
        handle_ready.set_result(loop.call_soon_threadsafe(callback, "ran"))

    thread = threading.Thread(target=scheduler)
    loop.call_soon(thread.start)
    loop.run_forever()
    thread.join()
    loop.close()
    assert results == ["ran"]
    assert observed == [True]


def test_threadsafe_cancel_before_the_loop_runs_it() -> None:
    loop = zuvloop.new_event_loop()
    gate = threading.Event()
    results: list[str] = []
    observed: list[bool] = []

    def scheduler() -> None:
        try:
            handle = loop.call_soon_threadsafe(results.append, "never")
            handle.cancel()
            handle.cancel()
            observed.append(handle.cancelled())
        finally:
            gate.set()
            loop.call_soon_threadsafe(loop.stop)

    # The gate holds the loop thread until the cancellation is done, so the
    # callback cannot win the race and run before `cancel` reaches it.
    loop.call_soon(gate.wait, 5)
    thread = threading.Thread(target=scheduler)
    thread.start()
    loop.run_forever()
    thread.join()
    loop.close()
    assert results == []
    assert observed == [True]


def test_threadsafe_cancel_releases_every_waiting_thread() -> None:
    loop = zuvloop.new_event_loop()
    started = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    handle_ready: concurrent.futures.Future[asyncio.Handle] = concurrent.futures.Future()
    observed: list[bool] = []

    def callback() -> None:
        started.set()
        release.wait(5)
        finished.set()

    def waiter() -> None:
        handle = handle_ready.result(5)
        started.wait(5)
        handle.cancel()
        observed.append(finished.is_set())

    def driver() -> None:
        try:
            handle_ready.set_result(loop.call_soon_threadsafe(callback))
            waiters = [threading.Thread(target=waiter) for _ in range(2)]
            for waiting in waiters:
                waiting.start()
            started.wait(5)
            time.sleep(0.1)
            release.set()
            for waiting in waiters:
                waiting.join()
        finally:
            release.set()
            loop.call_soon_threadsafe(loop.stop)

    thread = threading.Thread(target=driver)
    loop.call_soon(thread.start)
    loop.run_forever()
    thread.join()
    loop.close()
    assert observed == [True, True]


async def test_run_in_executor_uses_a_thread() -> None:
    loop = running_loop()
    result = await loop.run_in_executor(None, lambda: threading.current_thread().name)
    assert result.startswith("zuvloop")


async def test_run_in_executor_accepts_an_explicit_executor() -> None:
    import concurrent.futures

    loop = running_loop()
    with concurrent.futures.ThreadPoolExecutor(1) as executor:
        assert await loop.run_in_executor(executor, time.monotonic) > 0
    loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(1))
    assert await loop.run_in_executor(None, lambda: "explicit default") == "explicit default"


async def test_task_factory_round_trip() -> None:
    loop = running_loop()
    assert loop.get_task_factory() is None
    made: list[str] = []

    def factory(
        inner_loop: asyncio.AbstractEventLoop, coro: Coroutine[None, None, None], **kwargs: object
    ) -> asyncio.Task[None]:
        made.append("called")
        return asyncio.Task(coro, loop=inner_loop, **kwargs)  # type: ignore[arg-type]

    loop.set_task_factory(factory)
    assert loop.get_task_factory() is factory
    await loop.create_task(asyncio.sleep(0))
    loop.set_task_factory(None)
    assert made == ["called"]


async def test_eager_task_factory_is_supported() -> None:
    loop = running_loop()
    loop.set_task_factory(asyncio.eager_task_factory)

    async def immediate() -> str:
        return "eager"

    task = loop.create_task(immediate())
    assert task.done()
    assert await task == "eager"
    loop.set_task_factory(None)


async def test_debug_mode_round_trip() -> None:
    loop = running_loop()
    assert loop.get_debug() is False
    loop.set_debug(True)
    assert loop.get_debug() is True
    loop.set_debug(False)


async def test_slow_callback_duration_is_settable() -> None:
    loop = running_loop()
    assert loop.slow_callback_duration == pytest.approx(0.1)
    loop.slow_callback_duration = 0.5
    assert loop.slow_callback_duration == pytest.approx(0.5)
    loop.slow_callback_duration = 0.1


async def test_metrics_report_loop_activity() -> None:
    loop = running_loop()
    await asyncio.sleep(0.01)
    metrics = loop._metrics()
    assert metrics["callbacks_run"] > 0
    assert metrics["loop_count"] > 0
    assert metrics["watchers"] >= 1
    assert set(metrics) == {
        "loop_count",
        "events",
        "events_waiting",
        "idle_time_ns",
        "callbacks_run",
        "ready",
        "timers",
        "watchers",
    }


async def test_timer_handle_cancelled_hook_is_accepted() -> None:
    loop = running_loop()
    handle = loop.call_later(10, print)
    handle.cancel()
    loop._timer_handle_cancelled(handle)


async def test_repr_describes_the_loop() -> None:
    loop = running_loop()
    assert "EventLoop running=True closed=False" in repr(loop)


async def test_libuv_version_is_reported() -> None:
    assert zuvloop.libuv_version().count(".") == 2


async def test_handles_are_weak_referenceable() -> None:
    """asyncio's handles support weak references, so these must too."""
    import weakref

    loop = running_loop()
    handle = loop.call_soon(print)
    timer = loop.call_later(30, print)
    try:
        assert weakref.ref(handle)() is handle
        assert weakref.ref(timer)() is timer
    finally:
        handle.cancel()
        timer.cancel()
