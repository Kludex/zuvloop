from __future__ import annotations

import asyncio
import functools
import threading
from collections.abc import Callable

import pytest

import zuvloop


@pytest.mark.parametrize("method", ["soon", "later", "later_pending", "at", "at_pending"])
def test_debug_scheduling_during_loop_startup(method: str) -> None:
    for _ in range(25):
        loop = zuvloop.new_event_loop()
        loop.set_debug(True)
        gate = threading.Barrier(2, timeout=5)
        started = threading.Event()
        release = threading.Event()
        seen: list[int] = []
        accepted = 0
        schedulers: dict[str, Callable[[Callable[[], None]], asyncio.Handle | zuvloop.Handle]] = {
            "soon": loop.call_soon,
            "later": functools.partial(loop.call_later, 0),
            "later_pending": functools.partial(loop.call_later, 0.001),
            "at": functools.partial(loop.call_at, loop.time()),
            "at_pending": functools.partial(loop.call_at, loop.time() + 0.001),
        }
        schedule = schedulers[method]

        def hold_loop() -> None:
            started.set()
            assert release.wait(5)
            loop.call_later(0.01, loop.stop)

        def callback() -> None:
            seen.append(threading.get_ident())

        def run() -> None:
            gate.wait()
            loop.run_forever()

        loop.call_soon(hold_loop)
        thread = threading.Thread(target=run)
        thread.start()
        try:
            for phase in ("idle", "starting", "running"):
                if phase == "starting":
                    gate.wait()
                elif phase == "running":
                    assert started.wait(5)
                try:
                    schedule(callback)
                except RuntimeError as exc:
                    assert phase != "idle"
                    assert "Non-thread-safe operation" in str(exc)
                else:
                    assert phase != "running"
                    accepted += 1
            assert started.wait(5)
            with pytest.raises(RuntimeError, match="Non-thread-safe operation"):
                schedule(callback)
        finally:
            release.set()
            thread.join(5)
            assert not thread.is_alive()
            loop.close()
        assert len(seen) == accepted
        assert all(ident == thread.ident for ident in seen)
