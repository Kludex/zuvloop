from __future__ import annotations

import contextvars
import sys
import threading
import time

import pytest

import zuvloop


@pytest.mark.skipif(sys._is_gil_enabled(), reason="Requires free-threaded critical-section suspension")
@pytest.mark.parametrize("transition", ["start", "close"])
def test_inbox_drain_rechecks_loop_state_after_handle_contention(
    transition: str,
) -> None:  # pragma: no cover - free-threaded CI
    for _ in range(10):
        loop = zuvloop.new_event_loop()
        loop.set_debug(True)
        repr_entered = threading.Event()
        release_repr = threading.Event()
        scheduling = threading.Event()
        scheduled = threading.Event()
        errors: list[str] = []
        accepted: list[bool] = []

        class Callback:
            def __call__(self) -> None:
                pass

            def __repr__(self) -> str:
                repr_entered.set()
                deadline = time.monotonic() + 10
                while not release_repr.is_set() and time.monotonic() < deadline:
                    pass
                return "callback"

        def schedule() -> None:
            scheduling.set()
            try:
                loop.call_soon(lambda: None, context=contextvars.Context())
            except RuntimeError as exc:
                errors.append(str(exc))
            else:
                accepted.append(True)
            finally:
                scheduled.set()

        handle = loop.call_soon_threadsafe(Callback(), context=contextvars.Context())
        holder = threading.Thread(target=repr, args=(handle,), daemon=True)
        scheduler = threading.Thread(target=schedule, daemon=True)
        runner = threading.Thread(target=loop.run_forever, daemon=True)
        holder.start()
        try:
            assert repr_entered.wait(5)
            scheduler.start()
            assert scheduling.wait(5)
            assert not scheduled.wait(0.1)
            if transition == "start":
                runner.start()
                deadline = time.monotonic() + 5
                while not loop.is_running() and time.monotonic() < deadline:
                    time.sleep(0.001)
                assert loop.is_running()
                expected = "Non-thread-safe operation invoked on an event loop other than the current one"
            else:
                loop.close()
                expected = "Event loop is closed"
            release_repr.set()
            assert scheduled.wait(5)
            assert errors == [expected]
            assert accepted == []
        finally:
            release_repr.set()
            if not loop.is_closed():
                loop.call_soon_threadsafe(loop.stop, context=contextvars.Context())
            for thread in (holder, scheduler, runner):
                if thread.ident is not None:
                    thread.join(5)
                assert not thread.is_alive()
            loop.close()
