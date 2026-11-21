from __future__ import annotations

import contextvars
import os
import subprocess
import sys
import sysconfig
import threading
import time

import pytest

import zuvloop

pytestmark = pytest.mark.skipif(
    not sysconfig.get_config_var("Py_GIL_DISABLED"), reason="Requires free-threaded critical-section suspension"
)

CLOSED = "Event loop is closed"
FOREIGN_THREAD = "Non-thread-safe operation invoked on an event loop other than the current one"


def test_call_soon_sees_a_loop_started_while_it_waited_on_the_candidate() -> None:  # pragma: no cover - free-threaded
    run_untraced("start")


def test_call_soon_sees_a_loop_closed_while_it_waited_on_the_candidate() -> None:  # pragma: no cover - free-threaded
    run_untraced("close")


def test_call_soon_threadsafe_sees_a_loop_closed_while_it_waited_on_the_candidate() -> None:  # pragma: no cover
    run_untraced("close_threadsafe")


def test_call_soon_sees_a_loop_closed_while_it_waited_on_a_second_candidate() -> None:  # pragma: no cover
    run_untraced("close_on_second_candidate")


def run_untraced(scenario: str) -> None:  # pragma: no cover - free-threaded
    # Tracing can suspend the handle lock that the scenario must keep held.
    env = os.environ.copy()
    env.pop("COVERAGE_PROCESS_START", None)
    subprocess.run(
        [sys.executable, "-I", "-X", "gil=0", os.path.abspath(__file__), scenario],
        check=True,
        timeout=60,
        env=env,
    )


class SlowRepr:  # pragma: no cover - untraced subprocess workload
    """A callback whose `repr` holds its handle's lock until released."""

    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()

    def __call__(self) -> None:
        pass

    def __repr__(self) -> str:
        self.entered.set()
        deadline = time.monotonic() + 10
        # Spinning keeps the thread attached, so the handle's critical section stays held.
        while not self.release.is_set() and time.monotonic() < deadline:
            pass
        return "callback"


def contend(scenario: str) -> None:  # pragma: no cover - untraced subprocess workload
    """Makes a scheduler wait on the thread-safe candidate's lock, then changes the loop under it."""
    assert sys.version_info >= (3, 13) and not sys._is_gil_enabled()
    thread_errors: list[threading.ExceptHookArgs] = []
    threading.excepthook = thread_errors.append
    for _ in range(10):
        loop = zuvloop.new_event_loop()
        loop.set_debug(True)
        # A callback only runs once the loop has published its owner thread, which `is_running()` does not wait for.
        owned = threading.Event()
        loop.call_soon(owned.set)
        first = SlowRepr()
        second = SlowRepr()
        scheduled = threading.Event()
        outcome: list[str] = []

        def schedule() -> None:
            call = loop.call_soon_threadsafe if scenario == "close_threadsafe" else loop.call_soon
            try:
                call(int, context=contextvars.Context())
            except RuntimeError as exc:
                outcome.append(str(exc))
            else:
                outcome.append("accepted")
            finally:
                scheduled.set()

        candidate = loop.call_soon_threadsafe(first, context=contextvars.Context())
        threads = [
            threading.Thread(target=repr, args=(candidate,), daemon=True),
            threading.Thread(target=schedule, daemon=True),
        ]
        try:
            threads[0].start()
            assert first.entered.wait(5)
            threads[1].start()
            assert not scheduled.wait(0.1), "the scheduler did not wait on the candidate"
            if scenario == "start":
                threads.append(threading.Thread(target=loop.run_forever, daemon=True))
                threads[2].start()
                assert owned.wait(5)
            elif scenario == "close_on_second_candidate":
                # The scheduler has taken the first candidate, so this one is installed behind its back.
                next_candidate = loop.call_soon_threadsafe(second, context=contextvars.Context())
                threads.append(threading.Thread(target=repr, args=(next_candidate,), daemon=True))
                threads[2].start()
                assert second.entered.wait(5)
                first.release.set()
                assert not scheduled.wait(0.1), "the scheduler did not wait on the second candidate"
                loop.close()
            else:
                loop.close()
            first.release.set()
            second.release.set()
            assert scheduled.wait(5)
            assert outcome == [FOREIGN_THREAD if scenario == "start" else CLOSED]
        finally:
            first.release.set()
            second.release.set()
            if not loop.is_closed():
                loop.call_soon_threadsafe(loop.stop, context=contextvars.Context())
            for thread in threads:
                if thread.ident is not None:
                    thread.join(5)
                assert not thread.is_alive()
            loop.close()
    assert not thread_errors


if __name__ == "__main__":  # pragma: no cover - untraced subprocess workload
    contend(sys.argv[1])
