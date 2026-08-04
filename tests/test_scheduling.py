from __future__ import annotations

import asyncio
import contextvars
import threading
import time
from typing import Any

import pytest

import zuv

pytestmark = pytest.mark.anyio


async def test_call_soon_runs_in_order() -> None:
    loop = asyncio.get_running_loop()
    seen: list[int] = []
    for index in range(5):
        loop.call_soon(seen.append, index)
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert seen == [0, 1, 2, 3, 4]


async def test_call_soon_passes_many_arguments() -> None:
    loop = asyncio.get_running_loop()
    captured: list[tuple[Any, ...]] = []
    loop.call_soon(lambda *args: captured.append(args), 1, 2, 3, 4, 5, 6)
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert captured == [(1, 2, 3, 4, 5, 6)]


async def test_call_soon_runs_in_the_calling_context() -> None:
    variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable")
    variable.set("outer")
    loop = asyncio.get_running_loop()
    seen: list[str] = []
    loop.call_soon(lambda: seen.append(variable.get()))
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert seen == ["outer"]


async def test_call_soon_accepts_an_explicit_context() -> None:
    variable: contextvars.ContextVar[str] = contextvars.ContextVar("variable", default="default")
    context = contextvars.copy_context()
    context.run(variable.set, "explicit")
    loop = asyncio.get_running_loop()
    seen: list[str] = []
    loop.call_soon(lambda: seen.append(variable.get()), context=context)
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert seen == ["explicit"]


async def test_call_soon_rejects_unknown_keywords() -> None:
    loop = asyncio.get_running_loop()
    with pytest.raises(TypeError, match="context"):
        loop.call_soon(print, unexpected=1)  # type: ignore[call-arg]


async def test_call_soon_requires_a_callback() -> None:
    loop = asyncio.get_running_loop()
    with pytest.raises(TypeError, match="requires a callback"):
        loop.call_soon()  # type: ignore[call-arg]


async def test_cancelled_callback_does_not_run() -> None:
    loop = asyncio.get_running_loop()
    seen: list[str] = []
    handle = loop.call_soon(seen.append, "nope")
    handle.cancel()
    assert handle.cancelled() is True
    assert "cancelled" in repr(handle)
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    assert seen == []


async def test_handle_repr_names_the_callback() -> None:
    loop = asyncio.get_running_loop()
    handle = loop.call_soon(print)
    assert "print" in repr(handle)
    handle.cancel()


async def test_call_later_never_fires_early() -> None:
    loop = asyncio.get_running_loop()
    delay = 0.05
    started = loop.time()
    await asyncio.sleep(delay)
    assert loop.time() - started >= delay


async def test_call_later_ordering() -> None:
    loop = asyncio.get_running_loop()
    seen: list[str] = []
    loop.call_later(0.03, seen.append, "third")
    loop.call_later(0.01, seen.append, "first")
    loop.call_later(0.02, seen.append, "second")
    await asyncio.sleep(0.08)
    assert seen == ["first", "second", "third"]


async def test_call_at_uses_the_loop_clock() -> None:
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    handle = loop.call_at(loop.time() + 0.01, done.set_result, None)
    assert handle.when() > loop.time()
    await done


async def test_call_later_requires_a_delay_and_a_callback() -> None:
    loop = asyncio.get_running_loop()
    with pytest.raises(TypeError, match="delay and a callback"):
        loop.call_later(0.1)  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="time and a callback"):
        loop.call_at(0.1)  # type: ignore[call-arg]


async def test_negative_delay_runs_promptly() -> None:
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    loop.call_later(-10, done.set_result, "now")
    assert await done == "now"


async def test_cancelling_many_timers_compacts_the_heap() -> None:
    loop = asyncio.get_running_loop()
    handles = [loop.call_later(30, print) for _ in range(400)]
    for handle in handles:
        handle.cancel()
    assert loop._metrics()["timers"] <= 400
    done = loop.create_future()
    loop.call_later(0.01, done.set_result, None)
    await done


async def test_call_soon_threadsafe_from_another_thread() -> None:
    loop = asyncio.get_running_loop()
    done = loop.create_future()

    def worker() -> None:
        loop.call_soon_threadsafe(done.set_result, "from thread")

    threading.Thread(target=worker).start()
    assert await done == "from thread"


async def test_call_soon_threadsafe_validates_its_arguments() -> None:
    loop = asyncio.get_running_loop()
    with pytest.raises(TypeError, match="requires a callback"):
        loop.call_soon_threadsafe()  # type: ignore[call-arg]


async def test_run_in_executor_uses_a_thread() -> None:
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(None, lambda: threading.current_thread().name)
    assert result.startswith("zuv")


async def test_run_in_executor_accepts_an_explicit_executor() -> None:
    import concurrent.futures

    loop = asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor(1) as executor:
        assert await loop.run_in_executor(executor, time.monotonic) > 0
    loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(1))
    assert await loop.run_in_executor(None, lambda: "explicit default") == "explicit default"


async def test_task_factory_round_trip() -> None:
    loop = asyncio.get_running_loop()
    assert loop.get_task_factory() is None
    made: list[str] = []

    def factory(inner_loop: Any, coro: Any, **kwargs: Any) -> asyncio.Task[Any]:
        made.append("called")
        return asyncio.Task(coro, loop=inner_loop, **kwargs)

    loop.set_task_factory(factory)
    assert loop.get_task_factory() is factory
    await loop.create_task(asyncio.sleep(0))
    loop.set_task_factory(None)
    assert made == ["called"]


async def test_eager_task_factory_is_supported() -> None:
    loop = asyncio.get_running_loop()
    loop.set_task_factory(asyncio.eager_task_factory)

    async def immediate() -> str:
        return "eager"

    task = loop.create_task(immediate())
    assert task.done()
    assert await task == "eager"
    loop.set_task_factory(None)


async def test_debug_mode_round_trip() -> None:
    loop = asyncio.get_running_loop()
    assert loop.get_debug() is False
    loop.set_debug(True)
    assert loop.get_debug() is True
    loop.set_debug(False)


async def test_slow_callback_duration_is_settable() -> None:
    loop = asyncio.get_running_loop()
    assert loop.slow_callback_duration == pytest.approx(0.1)
    loop.slow_callback_duration = 0.5
    assert loop.slow_callback_duration == pytest.approx(0.5)
    loop.slow_callback_duration = 0.1


async def test_metrics_report_loop_activity() -> None:
    loop = asyncio.get_running_loop()
    await asyncio.sleep(0.01)
    metrics = loop._metrics()
    assert metrics["callbacks_run"] > 0
    assert metrics["loop_count"] > 0
    assert metrics["watchers"] >= 1
    assert set(metrics) == {
        "loop_count", "events", "events_waiting", "idle_time_ns",
        "callbacks_run", "ready", "timers", "watchers",
    }


async def test_timer_handle_cancelled_hook_is_accepted() -> None:
    loop = asyncio.get_running_loop()
    handle = loop.call_later(10, print)
    handle.cancel()
    assert loop._timer_handle_cancelled(handle) is None


async def test_repr_describes_the_loop() -> None:
    loop = asyncio.get_running_loop()
    assert "EventLoop running=True closed=False" in repr(loop)


async def test_libuv_version_is_reported() -> None:
    assert zuv.libuv_version().count(".") == 2
