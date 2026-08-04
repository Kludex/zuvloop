from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import time
from typing import Any

import pytest

import zuv

pytestmark = pytest.mark.anyio


def test_run_returns_the_coroutine_result() -> None:
    async def main() -> str:
        await asyncio.sleep(0)
        return "done"

    assert zuv.run(main()) == "done"


def test_run_honours_debug_mode() -> None:
    async def main() -> bool:
        return asyncio.get_running_loop().get_debug()

    assert zuv.run(main(), debug=True) is True


def test_asyncio_run_accepts_the_loop_factory() -> None:
    async def main() -> str:
        return type(asyncio.get_running_loop()).__name__

    assert asyncio.run(main(), loop_factory=zuv.new_event_loop) == "EventLoop"


def test_stop_runs_the_rest_of_the_batch(loop: zuv.EventLoop) -> None:
    seen: list[str] = []
    loop.call_soon(seen.append, "first")
    loop.call_soon(loop.stop)
    loop.call_soon(seen.append, "queued before stop ran")
    loop.run_forever()
    assert seen == ["first", "queued before stop ran"]


def test_callbacks_scheduled_after_stop_wait_for_the_next_run(loop: zuv.EventLoop) -> None:
    seen: list[str] = []

    def stop_then_schedule() -> None:
        loop.stop()
        loop.call_soon(seen.append, "next run")

    loop.call_soon(stop_then_schedule)
    loop.run_forever()
    assert seen == []
    loop.call_soon(loop.stop)
    loop.run_forever()
    assert seen == ["next run"]


def test_run_until_complete_returns_a_result(loop: zuv.EventLoop) -> None:
    async def main() -> int:
        await asyncio.sleep(0)
        return 7

    assert loop.run_until_complete(main()) == 7


def test_run_until_complete_propagates_exceptions(loop: zuv.EventLoop) -> None:
    async def main() -> None:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        loop.run_until_complete(main())


def test_run_until_complete_accepts_a_future(loop: zuv.EventLoop) -> None:
    future = loop.create_future()
    loop.call_soon(future.set_result, "ready")
    assert loop.run_until_complete(future) == "ready"


def test_run_until_complete_rejects_a_loop_stopped_early(loop: zuv.EventLoop) -> None:
    async def main() -> None:
        await asyncio.sleep(10)

    loop.call_later(0.01, loop.stop)
    with pytest.raises(RuntimeError, match="stopped before Future completed"):
        loop.run_until_complete(main())


def test_system_exit_escapes_the_loop(loop: zuv.EventLoop) -> None:
    def raiser() -> None:
        raise SystemExit(3)

    loop.call_soon(raiser)
    with pytest.raises(SystemExit):
        loop.run_forever()


def test_system_exit_escapes_run_until_complete(loop: zuv.EventLoop) -> None:
    async def main() -> None:
        await asyncio.sleep(10)

    def raiser() -> None:
        raise KeyboardInterrupt

    loop.call_later(0.01, raiser)
    with pytest.raises(KeyboardInterrupt):
        loop.run_until_complete(main())


def test_a_running_loop_cannot_be_run_again(loop: zuv.EventLoop) -> None:
    errors: list[type[BaseException]] = []

    def reenter() -> None:
        try:
            loop.run_forever()
        except RuntimeError:
            errors.append(RuntimeError)
        loop.stop()

    loop.call_soon(reenter)
    loop.run_forever()
    assert errors == [RuntimeError]


def test_a_second_loop_cannot_run_inside_a_running_one() -> None:
    async def main() -> None:
        other = zuv.new_event_loop()
        try:
            with pytest.raises(RuntimeError, match="another loop is running"):
                other.run_forever()
        finally:
            other.close()

    zuv.run(main())


def test_a_running_loop_cannot_be_closed(loop: zuv.EventLoop) -> None:
    errors: list[str] = []

    def attempt() -> None:
        try:
            loop.close()
        except RuntimeError as exc:
            errors.append(str(exc))
        loop.stop()

    loop.call_soon(attempt)
    loop.run_forever()
    assert errors == ["Cannot close a running event loop"]


def test_closing_twice_is_harmless(loop: zuv.EventLoop) -> None:
    loop.close()
    loop.close()
    assert loop.is_closed() is True


def test_a_closed_loop_rejects_work(loop: zuv.EventLoop) -> None:
    loop.close()
    for call in (
        lambda: loop.call_soon(print),
        lambda: loop.call_later(0, print),
        lambda: loop.call_soon_threadsafe(print),
        lambda: loop.run_forever(),
        lambda: loop.run_in_executor(None, print),
        lambda: loop.add_reader(0, print),
    ):
        with pytest.raises(RuntimeError, match="closed"):
            call()

    coro = asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="closed"):
        loop.create_task(coro)
    coro.close()


def test_close_shuts_down_the_default_executor(loop: zuv.EventLoop) -> None:
    async def main() -> None:
        await loop.run_in_executor(None, time.monotonic)

    loop.run_until_complete(main())
    assert loop._default_executor is not None
    loop.close()
    assert loop._default_executor is None


async def test_shutdown_default_executor_joins_threads() -> None:
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, time.monotonic)
    await loop.shutdown_default_executor()
    with pytest.raises(RuntimeError, match="Executor shutdown"):
        await loop.run_in_executor(None, time.monotonic)
    loop._executor_shutdown_called = False
    loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(1))


async def test_shutdown_default_executor_without_one_is_a_no_op() -> None:
    loop = zuv.new_event_loop()
    try:
        await loop.shutdown_default_executor()
    finally:
        loop.close()


def test_shutdown_default_executor_times_out(loop: zuv.EventLoop) -> None:
    release = threading.Event()

    async def main() -> None:
        await loop.run_in_executor(None, release.wait)

    loop.create_task(main())
    loop.run_until_complete(asyncio.sleep(0.05))
    with pytest.warns(RuntimeWarning, match="did not finish joining"):
        loop.run_until_complete(loop.shutdown_default_executor(0.01))
    release.set()


async def test_shutdown_asyncgens_closes_generators() -> None:
    loop = asyncio.get_running_loop()
    closed: list[str] = []

    async def generator() -> Any:
        try:
            yield 1
            yield 2
        finally:
            closed.append("closed")

    agen = generator()
    assert await anext(agen) == 1
    await loop.shutdown_asyncgens()
    assert closed == ["closed"]
    loop._asyncgens_shutdown_called = False


async def test_shutdown_asyncgens_reports_failures() -> None:
    loop = asyncio.get_running_loop()
    reported: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _loop, context: reported.append(context))

    async def generator() -> Any:
        try:
            yield 1
        finally:
            raise ValueError("close failed")

    agen = generator()
    assert await anext(agen) == 1
    await loop.shutdown_asyncgens()
    assert "asynchronous generator" in reported[0]["message"]
    loop.set_exception_handler(None)
    loop._asyncgens_shutdown_called = False


async def test_shutdown_asyncgens_without_generators_is_a_no_op() -> None:
    loop = zuv.new_event_loop()
    try:
        await loop.shutdown_asyncgens()
    finally:
        loop.close()


def test_asyncgens_scheduled_after_shutdown_warn(loop: zuv.EventLoop) -> None:
    async def generator() -> Any:
        yield 1

    async def main() -> None:
        await loop.shutdown_asyncgens()
        agen = generator()
        with pytest.warns(ResourceWarning, match="was scheduled after"):
            assert await anext(agen) == 1
        await agen.aclose()

    loop.run_until_complete(main())


def test_a_loop_runs_on_a_worker_thread() -> None:
    results: list[str] = []

    def worker() -> None:
        loop = zuv.new_event_loop()
        try:
            results.append(loop.run_until_complete(_greet()))
        finally:
            loop.close()

    async def _greet() -> str:
        await asyncio.sleep(0)
        return "from thread"

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join()
    assert results == ["from thread"]


async def test_create_task_names_tasks() -> None:
    loop = asyncio.get_running_loop()
    task = loop.create_task(asyncio.sleep(0), name="named")
    assert task.get_name() == "named"
    await task


async def test_tasks_are_visible_to_asyncio_introspection() -> None:
    async def worker() -> None:
        await asyncio.sleep(0.05)

    task = asyncio.get_running_loop().create_task(worker(), name="introspected")
    await asyncio.sleep(0)
    assert task in asyncio.all_tasks()
    assert asyncio.current_task() is not None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
