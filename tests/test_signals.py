from __future__ import annotations

import asyncio
import gc
import os
import signal
import threading
import weakref

import pytest

import zuvloop
from conftest import running_loop

pytestmark = pytest.mark.anyio


async def test_a_signal_reaches_its_handler() -> None:
    loop = running_loop()
    received = loop.create_future()
    loop.add_signal_handler(signal.SIGUSR1, received.set_result, "delivered")
    try:
        os.kill(os.getpid(), signal.SIGUSR1)
        assert await asyncio.wait_for(received, 2) == "delivered"
    finally:
        loop.remove_signal_handler(signal.SIGUSR1)


async def test_a_signal_wakes_a_parked_loop() -> None:
    loop = running_loop()
    received = loop.create_future()
    loop.add_signal_handler(signal.SIGUSR2, received.set_result, None)
    try:
        loop.call_later(0.05, os.kill, os.getpid(), signal.SIGUSR2)
        await asyncio.wait_for(received, 2)
    finally:
        loop.remove_signal_handler(signal.SIGUSR2)


async def test_signal_handlers_can_be_removed() -> None:
    loop = running_loop()
    assert loop.remove_signal_handler(signal.SIGUSR1) is False
    loop.add_signal_handler(signal.SIGUSR1, print)
    assert loop.remove_signal_handler(signal.SIGUSR1) is True
    assert loop.remove_signal_handler(signal.SIGUSR1) is False


async def test_sigint_is_restored_to_the_default_handler() -> None:
    loop = running_loop()
    original = signal.getsignal(signal.SIGINT)
    loop.add_signal_handler(signal.SIGINT, print)
    assert loop.remove_signal_handler(signal.SIGINT) is True
    assert signal.getsignal(signal.SIGINT) is signal.default_int_handler
    signal.signal(signal.SIGINT, original)


async def test_signal_handlers_validate_their_arguments() -> None:
    loop = running_loop()
    with pytest.raises(TypeError, match="must be an int"):
        loop.add_signal_handler("SIGUSR1", print)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="invalid signal number"):
        loop.add_signal_handler(9999, print)
    with pytest.raises(TypeError, match="coroutines cannot be used"):
        loop.add_signal_handler(signal.SIGUSR1, asyncio.sleep)
    with pytest.raises(ValueError, match="invalid signal number"):
        loop.remove_signal_handler(9999)


async def test_uncatchable_signals_are_rejected() -> None:
    loop = running_loop()
    with pytest.raises(RuntimeError, match="cannot be caught"):
        loop.add_signal_handler(signal.SIGKILL, print)


def test_signal_handlers_require_the_main_thread() -> None:
    errors: list[str] = []

    def worker() -> None:
        loop = zuvloop.new_event_loop()
        try:
            loop.add_signal_handler(signal.SIGUSR1, print)
        except RuntimeError as exc:
            errors.append(str(exc))
        finally:
            loop.close()

    thread = threading.Thread(target=worker)
    thread.start()
    thread.join()
    assert errors == ["signal handlers can only be added from the main thread"]


def test_close_removes_signal_handlers_and_releases_the_loop() -> None:
    original = signal.getsignal(signal.SIGUSR1)
    loop = zuvloop.new_event_loop()
    loop_ref = weakref.ref(loop)
    try:
        loop.add_signal_handler(signal.SIGUSR1, print)

        loop.close()

        assert signal.getsignal(signal.SIGUSR1) is signal.SIG_DFL
        del loop
        gc.collect()
        assert loop_ref() is None
    finally:
        signal.signal(signal.SIGUSR1, original)


def test_unclosed_loop_finalization_restores_signal_state() -> None:
    original = signal.getsignal(signal.SIGUSR1)
    loop = zuvloop.new_event_loop()
    loop_ref = weakref.ref(loop)
    try:
        loop.add_signal_handler(signal.SIGUSR1, print)

        with pytest.warns(ResourceWarning, match="unclosed event loop"):
            del loop
            gc.collect()

        assert loop_ref() is None
        assert signal.set_wakeup_fd(-1) == -1
        assert signal.getsignal(signal.SIGUSR1) is signal.SIG_DFL
    finally:
        signal.set_wakeup_fd(-1)
        signal.signal(signal.SIGUSR1, original)


def test_worker_thread_finalization_restores_signal_state_on_main_thread() -> None:
    original = signal.getsignal(signal.SIGUSR1)
    loop = zuvloop.new_event_loop()
    loop.add_signal_handler(signal.SIGUSR1, print)
    loop_ref = weakref.ref(loop)
    owner = [loop]
    del loop

    def finalize() -> None:
        owned = owner.pop()
        del owned
        gc.collect()

    try:
        with pytest.warns(ResourceWarning, match="unclosed event loop"):
            thread = threading.Thread(target=finalize)
            thread.start()
            thread.join()

        gc.collect()
        assert loop_ref() is None
        assert signal.set_wakeup_fd(-1) == -1
        assert signal.getsignal(signal.SIGUSR1) is signal.SIG_DFL
    finally:
        signal.set_wakeup_fd(-1)
        signal.signal(signal.SIGUSR1, original)


def test_a_signal_between_loop_runs_is_delivered_on_the_next_run() -> None:
    original = signal.getsignal(signal.SIGUSR1)
    loop = zuvloop.new_event_loop()
    received: list[str] = []
    try:
        loop.add_signal_handler(signal.SIGUSR1, received.append, "delivered")
        loop.run_until_complete(asyncio.sleep(0))

        os.kill(os.getpid(), signal.SIGUSR1)
        loop.run_until_complete(asyncio.sleep(0.01))

        assert received == ["delivered"]
    finally:
        loop.remove_signal_handler(signal.SIGUSR1)
        loop.close()
        signal.signal(signal.SIGUSR1, original)


def test_a_signal_whose_handler_was_removed_before_draining_is_dropped() -> None:
    original = signal.getsignal(signal.SIGUSR1)
    loop = zuvloop.new_event_loop()
    received: list[str] = []
    try:
        loop.add_signal_handler(signal.SIGUSR1, received.append, "delivered")
        loop.run_until_complete(asyncio.sleep(0))

        # The wakeup byte is already in the pipe when the handler goes away.
        os.kill(os.getpid(), signal.SIGUSR1)
        loop.remove_signal_handler(signal.SIGUSR1)
        loop.run_until_complete(asyncio.sleep(0.01))

        assert received == []
    finally:
        loop.close()
        signal.signal(signal.SIGUSR1, original)


def test_finalizing_a_running_loop_leaves_it_running() -> None:
    loop = zuvloop.new_event_loop()

    async def finalize() -> None:
        with pytest.warns(ResourceWarning, match="unclosed event loop"):
            loop.__del__()
        assert loop.is_running()
        assert not loop.is_closed()

    try:
        loop.run_until_complete(finalize())
    finally:
        loop.close()


def test_an_old_loop_finalizer_does_not_clobber_a_new_signal_owner() -> None:
    original = signal.getsignal(signal.SIGUSR1)
    old = zuvloop.new_event_loop()
    owner = zuvloop.new_event_loop()
    received: list[str] = []
    try:
        old.add_signal_handler(signal.SIGUSR1, print)
        owner.add_signal_handler(signal.SIGUSR1, received.append, "delivered")
        held = [old]
        del old

        def finalize_old_owner() -> None:
            abandoned = held.pop()
            del abandoned
            gc.collect()

        with pytest.warns(ResourceWarning, match="unclosed event loop"):
            thread = threading.Thread(target=finalize_old_owner)
            thread.start()
            thread.join()
        # Run the pending main-thread cleanup after the replacement owns both
        # global resources; neither one may be reset by the old token.
        gc.collect()

        os.kill(os.getpid(), signal.SIGUSR1)
        owner.run_until_complete(asyncio.sleep(0.01))
        assert received == ["delivered"]
    finally:
        owner.remove_signal_handler(signal.SIGUSR1)
        owner.close()
        signal.signal(signal.SIGUSR1, original)


def test_a_loop_without_handlers_does_not_disable_another_loops_signals() -> None:
    original = signal.getsignal(signal.SIGUSR1)
    owner = zuvloop.new_event_loop()
    other = zuvloop.new_event_loop()
    received: list[str] = []
    try:
        owner.add_signal_handler(signal.SIGUSR1, received.append, "delivered")
        owner.run_until_complete(asyncio.sleep(0))
        other.run_until_complete(asyncio.sleep(0))

        os.kill(os.getpid(), signal.SIGUSR1)
        owner.run_until_complete(asyncio.sleep(0.01))

        assert received == ["delivered"]
    finally:
        owner.remove_signal_handler(signal.SIGUSR1)
        owner.close()
        other.close()
        signal.signal(signal.SIGUSR1, original)


def test_a_closed_loop_rejects_signal_handlers(loop: zuvloop.EventLoop) -> None:
    loop.close()
    with pytest.raises(RuntimeError, match="closed"):
        loop.add_signal_handler(signal.SIGUSR1, print)


async def test_a_signal_nobody_registered_is_ignored() -> None:
    """The wakeup fd carries every signal, not only the ones the loop wants."""
    original = signal.getsignal(signal.SIGUSR2)
    signal.signal(signal.SIGUSR2, lambda *_: None)  # installed outside the loop
    try:
        os.kill(os.getpid(), signal.SIGUSR2)
        await asyncio.sleep(0.05)
    finally:
        signal.signal(signal.SIGUSR2, original)


async def test_an_application_may_install_its_own_handler() -> None:
    """uvicorn replaces the handler with signal.signal; delivery must survive."""
    loop = running_loop()
    received = loop.create_future()
    seen: list[int] = []
    loop.add_signal_handler(signal.SIGUSR1, received.set_result, "loop handler")
    original = signal.getsignal(signal.SIGUSR1)
    signal.signal(signal.SIGUSR1, lambda signum, _frame: seen.append(signum))
    try:
        os.kill(os.getpid(), signal.SIGUSR1)
        assert await asyncio.wait_for(received, 2) == "loop handler"
        assert seen == [signal.SIGUSR1]
    finally:
        signal.signal(signal.SIGUSR1, original)
        loop.remove_signal_handler(signal.SIGUSR1)
