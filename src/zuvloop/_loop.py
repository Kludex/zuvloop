from __future__ import annotations

import asyncio
import contextvars
import inspect
import signal
import threading
from collections.abc import Callable
from types import FrameType
from typing import Any

from ._connect import ConnectionOperations


class EventLoop(ConnectionOperations):
    """An asyncio event loop backed by libuv.

    Callback scheduling, timers, descriptor watching and the stream data path
    are implemented in Zig; this class supplies the parts of the asyncio
    interface that run once per loop, per connection, or on failure.
    """

    def __repr__(self) -> str:
        return f"<{type(self).__name__} running={self.is_running()} closed={self.is_closed()} debug={self.get_debug()}>"

    def add_signal_handler(  # type: ignore[override]  # typeshed ties args to the callback with a TypeVarTuple
        self, sig: int, callback: Callable[..., object], *args: Any
    ) -> None:
        if asyncio.iscoroutine(callback) or inspect.iscoroutinefunction(callback):
            raise TypeError("coroutines cannot be used with add_signal_handler()")
        self._check_closed()
        _check_signal(sig)
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError("signal handlers can only be added from the main thread")
        self._signal_handlers[sig] = (callback, args, contextvars.copy_context())
        try:
            # Installing any Python handler is what makes CPython write the
            # signal number to the wakeup fd; the loop dispatches from there.
            signal.signal(sig, _noop_signal_handler)
            signal.siginterrupt(sig, False)
            self._attach_wakeup_fd()
        except OSError as exc:
            del self._signal_handlers[sig]
            raise RuntimeError(f"sig {sig} cannot be caught") from exc

    def remove_signal_handler(self, sig: int) -> bool:
        _check_signal(sig)
        if self._signal_handlers.pop(sig, None) is None:
            return False
        handler = signal.default_int_handler if sig == signal.SIGINT else signal.SIG_DFL
        signal.signal(sig, handler)
        self._detach_wakeup_fd()
        return True


def _noop_signal_handler(signum: int, frame: FrameType | None) -> None:
    """Delivery happens through the wakeup fd; this only keeps it installed."""


def _check_signal(sig: int) -> None:
    if not isinstance(sig, int):
        raise TypeError(f"sig must be an int, not {sig!r}")
    if sig not in signal.valid_signals():
        raise ValueError(f"invalid signal number {sig}")
