from __future__ import annotations

import asyncio
import contextvars
import inspect
import signal
import threading
from collections.abc import Callable
from types import FrameType
from typing import Any

from ._base import SignalOwner, _signal_owners
from ._connect import ConnectionOperations

_MISSING_SIGNAL_OWNER = object()


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
        entry = (callback, args, contextvars.copy_context())
        previous_entry = self._signal_handlers.get(sig)
        previous_owner = _signal_owners.get(sig, _MISSING_SIGNAL_OWNER)

        # Publish ownership before touching process-global state. A pending
        # cleanup from an older loop can run between any two Python calls below;
        # seeing the new token makes it leave this registration alone.
        self._signal_handlers[sig] = entry
        _signal_owners[sig] = self._signal_owner
        wakeup_state: tuple[int, SignalOwner | None, bool] | None = None
        try:
            # Attach before signal.signal(), which implicitly enables syscall
            # interruption. If attachment fails, rollback has no siginterrupt
            # state to reconstruct (Python exposes a setter but no getter).
            wakeup_state = self._attach_wakeup_fd()
            # Installing any Python handler is what makes CPython write the
            # signal number to the wakeup fd; the loop dispatches from there.
            signal.signal(sig, _noop_signal_handler)
        except OSError as exc:
            previous_finalized = isinstance(previous_owner, SignalOwner) and previous_owner.is_finalized()
            if previous_finalized:
                # A pending finalizer already skipped this provisional owner.
                # Complete its reset instead of resurrecting that loop.
                handler = signal.default_int_handler if sig == signal.SIGINT else signal.SIG_DFL
                signal.signal(sig, handler)

            if previous_entry is None:
                del self._signal_handlers[sig]
            else:
                self._signal_handlers[sig] = previous_entry
            if isinstance(previous_owner, SignalOwner) and not previous_finalized:
                _signal_owners[sig] = previous_owner
                # Finalization can run after the snapshot but before this dict
                # assignment. Revalidate the token at the commit point; if it
                # raced us, cleanup either removed it already or we do so now.
                if (  # pragma: no cover - defensive recheck after deferred cleanup
                    previous_owner.is_finalized() and _signal_owners.get(sig) is previous_owner
                ):
                    _signal_owners.pop(sig, None)
                    handler = signal.default_int_handler if sig == signal.SIGINT else signal.SIG_DFL
                    signal.signal(sig, handler)
            else:
                _signal_owners.pop(sig, None)

            self._restore_wakeup_fd(wakeup_state)
            raise RuntimeError(f"sig {sig} cannot be caught") from exc

        signal.siginterrupt(sig, False)

    def remove_signal_handler(self, sig: int) -> bool:
        _check_signal(sig)
        if self._signal_handlers.pop(sig, None) is None:
            return False
        if _signal_owners.get(sig) is self._signal_owner:
            del _signal_owners[sig]
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
