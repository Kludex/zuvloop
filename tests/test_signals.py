from __future__ import annotations

import asyncio
import gc
import os
import signal
import threading
import weakref

import pytest

import zuv
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
        loop = zuv.new_event_loop()
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
    loop = zuv.new_event_loop()
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


def test_a_closed_loop_rejects_signal_handlers(loop: zuv.EventLoop) -> None:
    loop.close()
    with pytest.raises(RuntimeError, match="closed"):
        loop.add_signal_handler(signal.SIGUSR1, print)
