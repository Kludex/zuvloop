from __future__ import annotations

import asyncio
import concurrent.futures
import os
import signal
import socket
import sys
import threading
import warnings
import weakref
from asyncio import events as _events
from collections.abc import Callable, Coroutine
from contextvars import Context
from functools import partial
from typing import Any

from . import _zuvloop
from ._instrumentation import (
    Instrumentation,
    instrumentation_provider_installed,
    metrics_provider_installed,
    publish_metrics,
)

_ExceptionHandler = Callable[[asyncio.AbstractEventLoop, dict[str, Any]], object]


# Signal dispositions and the wakeup fd are process-global. Tokens let delayed
# finalization tell whether another loop has replaced the state it installed.
class SignalOwner:
    def __init__(self) -> None:
        self.finalized = False


_signal_owners: dict[int, SignalOwner] = {}
_wakeup_fd_owner: SignalOwner | None = None


# The native methods are stricter than typeshed's AbstractEventLoop, which
# types several of them with TypeVarTuples this class cannot reproduce.
class LoopBase(_zuvloop.Loop, asyncio.AbstractEventLoop):  # type: ignore[misc]
    """Lifecycle, task creation, executors and error reporting.

    Scheduling primitives (`call_soon`, `call_later`, `time`, the reader and
    writer registrations) come from the Zig extension; everything here is
    orchestration that runs once per loop or once per connection.
    """

    # How often the native sampler fires while the loop runs. It publishes the
    # loop gauges when a meter provider is installed, and it is also how quickly
    # a provider configured after the loop started is noticed. Assign before
    # running the loop to change it.
    metrics_interval: float = 10.0

    def __init__(self) -> None:
        self._exception_handler: _ExceptionHandler | None = None
        self._task_factory: Callable[..., asyncio.Task[Any]] | None = None
        self._default_executor: concurrent.futures.Executor | None = None
        self._executor_shutdown_called = False
        self._asyncgens: weakref.WeakSet[Any] = weakref.WeakSet()
        self._asyncgens_shutdown_called = False
        self._signal_handlers: dict[int, tuple[Callable[..., object], tuple[Any, ...], Context]] = {}
        # Which `sock_*` call owns the watcher currently on each (fd, write).
        self._sock_watchers: dict[tuple[int, bool], object] = {}
        self._wakeup_fd_attached = False
        self._signal_owner = SignalOwner()
        self._instrumentation = Instrumentation()
        self._monitoring_armed = False
        self._metrics_armed = False
        self._setup_self_pipe()

    def __del__(self, _warn: Callable[..., object] = warnings.warn) -> None:
        if not self.is_closed():
            try:
                _warn(f"unclosed event loop {self!r}", ResourceWarning, source=self)
            except BaseException:
                # Exceptions escaping a finalizer are only reported as
                # unraisable; warning filters must not prevent cleanup.
                pass
            if not self.is_running():
                if self._signal_handlers and threading.current_thread() is not threading.main_thread():
                    wakeup_fd = self._csock.fileno()
                    try:
                        self._defer_close(
                            partial(
                                _finish_deferred_signal_cleanup,
                                tuple(self._signal_handlers),
                                wakeup_fd,
                                self._signal_owner,
                            )
                        )
                    except BaseException:  # pragma: no cover - CPython pending-call queue exhaustion
                        try:
                            _warn(
                                f"could not close signal-owning event loop {self!r} from the main thread",
                                ResourceWarning,
                                source=self,
                            )
                        except BaseException:
                            pass
                        return
                    # Keep the descriptor installed in CPython alive until the
                    # pending callback can disable it on the main thread.
                    self._csock.detach()
                    self._signal_handlers.clear()
                    self._wakeup_fd_attached = False
                try:
                    self.close()
                except BaseException:
                    # A partially initialized or externally damaged loop may
                    # not have a fully usable self-pipe during finalization.
                    pass

    # -- lifecycle ---------------------------------------------------------

    def run_forever(self) -> None:
        self._check_closed()
        self._check_runnable()
        old_hooks = sys.get_asyncgen_hooks()
        self._attach_wakeup_fd()
        _events._set_running_loop(self)
        sys.set_asyncgen_hooks(firstiter=self._asyncgen_firstiter, finalizer=self._asyncgen_finalizer)
        self._monitoring_armed = instrumentation_provider_installed()
        self._metrics_armed = metrics_provider_installed()
        self._set_slow_callback_monitoring(self._monitoring_armed)
        self._start_metrics(self.metrics_interval, self._on_sample)
        try:
            self._run()
        finally:
            self._set_slow_callback_monitoring(False)
            self._stop_metrics()
            sys.set_asyncgen_hooks(*old_hooks)
            _events._set_running_loop(None)
            self._detach_wakeup_fd()

    def run_until_complete(self, future: Any) -> Any:
        self._check_closed()
        self._check_runnable()
        new_task = not isinstance(future, asyncio.Future)
        task = asyncio.ensure_future(future, loop=self)
        if new_task:
            task._log_destroy_pending = False
        task.add_done_callback(_stop_when_done)
        try:
            self.run_forever()
        except BaseException:
            if new_task and task.done() and not task.cancelled():
                task.exception()
            raise
        finally:
            task.remove_done_callback(_stop_when_done)
        if not task.done():
            raise RuntimeError("Event loop stopped before Future completed.")
        return task.result()

    def close(self) -> None:
        if self.is_running():
            raise RuntimeError("Cannot close a running event loop")
        if self.is_closed():
            return
        for sig in tuple(self._signal_handlers):
            self.remove_signal_handler(sig)
        self._teardown_self_pipe()
        executor = self._default_executor
        self._default_executor = None
        self._close()
        if executor is not None:
            executor.shutdown(wait=False)

    async def shutdown_asyncgens(self) -> None:
        self._asyncgens_shutdown_called = True
        closing = list(self._asyncgens)
        if not closing:
            return
        self._asyncgens.clear()
        results = await asyncio.gather(*(agen.aclose() for agen in closing), return_exceptions=True)
        for result, agen in zip(results, closing, strict=True):
            if isinstance(result, BaseException):
                self.call_exception_handler(
                    {
                        "message": f"an error occurred during closing of asynchronous generator {agen!r}",
                        "exception": result,
                        "asyncgen": agen,
                    }
                )

    async def shutdown_default_executor(self, timeout: float | None = None) -> None:
        self._executor_shutdown_called = True
        executor = self._default_executor
        if executor is None:
            return
        future = self.create_future()
        thread = threading.Thread(target=_shutdown_executor, args=(self, future, executor))
        thread.start()
        try:
            async with asyncio.timeout(timeout):
                await future
        except TimeoutError:
            warnings.warn("The executor did not finish joining its threads within the timeout", RuntimeWarning, 2)
            executor.shutdown(wait=False)
        else:
            thread.join()

    # -- futures and tasks -------------------------------------------------

    def create_future(self) -> asyncio.Future[Any]:
        return asyncio.Future(loop=self)

    def create_task[T](
        self,
        coro: Coroutine[Any, Any, T],
        *,
        name: str | None = None,
        context: Context | None = None,
        **kwargs: Any,
    ) -> asyncio.Task[T]:
        self._check_closed()
        if self._task_factory is None:
            return asyncio.Task(coro, loop=self, name=name, context=context, **kwargs)
        return self._task_factory(self, coro, name=name, context=context, **kwargs)

    def set_task_factory(  # type: ignore[override]  # typeshed's _TaskFactory is narrower than asyncio accepts
        self, factory: Callable[..., asyncio.Task[Any]] | None
    ) -> None:
        self._task_factory = factory

    def get_task_factory(self) -> Callable[..., asyncio.Task[Any]] | None:
        return self._task_factory

    # -- executors ---------------------------------------------------------

    def run_in_executor[T](  # type: ignore[override]  # typeshed loses the return type
        self, executor: concurrent.futures.Executor | None, func: Callable[..., T], *args: Any
    ) -> asyncio.Future[T]:
        self._check_closed()
        if executor is None:
            if self._executor_shutdown_called:
                raise RuntimeError("Executor shutdown has been called")
            executor = self._default_executor
            if executor is None:
                executor = concurrent.futures.ThreadPoolExecutor(thread_name_prefix="zuvloop")
                self._default_executor = executor
        return asyncio.futures.wrap_future(executor.submit(func, *args), loop=self)

    def set_default_executor(self, executor: concurrent.futures.Executor) -> None:
        self._default_executor = executor

    # -- error reporting ---------------------------------------------------

    def get_exception_handler(self) -> _ExceptionHandler | None:
        return self._exception_handler

    def set_exception_handler(self, handler: _ExceptionHandler | None) -> None:
        self._exception_handler = handler

    def default_exception_handler(self, context: dict[str, Any]) -> None:
        self._instrumentation.report_exception(context)

    def call_exception_handler(self, context: dict[str, Any]) -> None:
        if self._exception_handler is None:
            self.default_exception_handler(context)
            return
        try:
            self._exception_handler(self, context)
        except SystemExit, KeyboardInterrupt:
            raise
        except BaseException as exc:
            self.default_exception_handler(
                {"message": "Unhandled error in exception handler", "exception": exc, "context": context}
            )

    def _on_slow_callback(self, handle: object, duration: float) -> None:
        self._instrumentation.report_slow_callback(handle, duration)

    def _on_sample(self, snapshot: dict[str, int]) -> None:
        if not self._monitoring_armed and instrumentation_provider_installed():
            self._monitoring_armed = True
            self._set_slow_callback_monitoring(True)
        if not self._metrics_armed:
            self._metrics_armed = metrics_provider_installed()
        if self._metrics_armed:
            publish_metrics(snapshot)

    # -- internals ---------------------------------------------------------

    def _check_closed(self) -> None:
        if self.is_closed():
            raise RuntimeError("Event loop is closed")

    def _check_runnable(self) -> None:
        if self.is_running():
            raise RuntimeError("This event loop is already running")
        if _events._get_running_loop() is not None:
            raise RuntimeError("Cannot run the event loop while another loop is running")

    def _asyncgen_firstiter(self, agen: Any) -> None:
        if self._asyncgens_shutdown_called:
            warnings.warn(
                f"asynchronous generator {agen!r} was scheduled after loop.shutdown_asyncgens() call",
                ResourceWarning,
                source=self,
            )
        self._asyncgens.add(agen)

    def _asyncgen_finalizer(self, agen: Any) -> None:
        self._asyncgens.discard(agen)
        if not self.is_closed():
            self.call_soon_threadsafe(self.create_task, agen.aclose())

    def _setup_self_pipe(self) -> None:
        self._ssock, self._csock = socket.socketpair()
        self._ssock.setblocking(False)
        self._csock.setblocking(False)
        self.add_reader(self._ssock.fileno(), self._drain_self_pipe, self._ssock)

    def _teardown_self_pipe(self) -> None:
        self.remove_reader(self._ssock.fileno())
        self._ssock.close()
        self._csock.close()

    def _attach_wakeup_fd(self) -> tuple[int, SignalOwner | None, bool] | None:
        # Only the main thread may own the wakeup fd, and only it runs Python
        # signal handlers - so a loop on any other thread simply skips this.
        if threading.current_thread() is threading.main_thread() and self._signal_handlers:
            global _wakeup_fd_owner
            previous_owner = _wakeup_fd_owner
            previous_attached = self._wakeup_fd_attached
            # Publish the provisional token before the syscall. A pending old
            # cleanup can run as soon as the C call returns; it must already see
            # that the descriptor has changed hands.
            _wakeup_fd_owner = self._signal_owner
            try:
                previous_fd = signal.set_wakeup_fd(self._csock.fileno())
            except OSError:
                if previous_owner is None or not previous_owner.finalized:
                    _wakeup_fd_owner = previous_owner
                else:
                    # A skipped stale cleanup may have closed its descriptor;
                    # never put that invalid owner back.
                    signal.set_wakeup_fd(-1)
                    _wakeup_fd_owner = None
                raise
            self._wakeup_fd_attached = True
            return (previous_fd, previous_owner, previous_attached)
        return None

    def _restore_wakeup_fd(self, previous: tuple[int, SignalOwner | None, bool] | None, *, abandon: bool) -> None:
        if previous is None:
            return
        global _wakeup_fd_owner
        # Stale cleanup skips a replacement token, so this transaction remains
        # the wakeup owner until it either commits or restores this snapshot.
        assert _wakeup_fd_owner is self._signal_owner
        fd, owner, was_attached = previous
        signal.set_wakeup_fd(-1 if abandon else fd)
        _wakeup_fd_owner = None if abandon else owner
        self._wakeup_fd_attached = False if abandon else was_attached

    def _detach_wakeup_fd(self) -> None:
        # Registered handlers must keep the fd active between separate calls
        # to run_forever/run_until_complete. A signal arriving while the loop
        # is stopped remains queued in the self-pipe for the next run.
        if (
            threading.current_thread() is threading.main_thread()
            and self._wakeup_fd_attached
            and not self._signal_handlers
        ):
            global _wakeup_fd_owner
            if _wakeup_fd_owner is self._signal_owner:
                signal.set_wakeup_fd(-1)
                _wakeup_fd_owner = None
            self._wakeup_fd_attached = False

    def _add_reader(self, fd: int, callback: Callable[..., object], *args: object) -> None:
        """asyncio's own name for `add_reader`; its child watcher uses it."""
        self.add_reader(fd, callback, *args)

    def _remove_reader(self, fd: int) -> bool:
        return self.remove_reader(fd)

    def _stop_serving(self, sock: socket.socket) -> None:
        """asyncio's own name for the hook `asyncio.base_events.Server.close` calls."""
        self.remove_reader(sock.fileno())
        sock.close()

    def _drain_self_pipe(self, sock: socket.socket) -> None:
        """Read the wakeup bytes; each one is a signal number to dispatch.

        Signals arrive through `signal.set_wakeup_fd` rather than from the
        Python-level handler, because applications legitimately install their own
        handler with `signal.signal` afterwards - uvicorn does - and that must not
        silently unhook the loop's own delivery.
        """
        try:
            while True:
                for signum in sock.recv(4096):
                    self._dispatch_signal(signum)
        except BlockingIOError, InterruptedError:
            pass

    def _dispatch_signal(self, signum: int) -> None:
        entry = self._signal_handlers.get(signum)
        if entry is None:
            return
        callback, args, context = entry
        self.call_soon(callback, *args, context=context)


def _stop_when_done(future: asyncio.Future[Any]) -> None:
    asyncio.futures._get_loop(future).stop()  # type: ignore[attr-defined]


def _finish_deferred_signal_cleanup(signals: tuple[int, ...], wakeup_fd: int, owner: SignalOwner) -> None:
    global _wakeup_fd_owner
    owner.finalized = True
    if _wakeup_fd_owner is owner:
        signal.set_wakeup_fd(-1)
        _wakeup_fd_owner = None
    try:
        for sig in signals:
            if _signal_owners.get(sig) is owner:
                del _signal_owners[sig]
                handler = signal.default_int_handler if sig == signal.SIGINT else signal.SIG_DFL
                signal.signal(sig, handler)
    finally:
        os.close(wakeup_fd)


def _shutdown_executor(loop: LoopBase, future: asyncio.Future[None], executor: concurrent.futures.Executor) -> None:
    try:
        executor.shutdown(wait=True)
    finally:
        # shutdown_default_executor() abandons this thread when it times out, so
        # the loop can be long closed by the time the executor finishes.
        try:
            loop.call_soon_threadsafe(asyncio.futures._set_result_unless_cancelled, future, None)  # type: ignore[attr-defined]
        except RuntimeError:
            # close() can win the race after call_soon_threadsafe starts.
            if not loop.is_closed():  # pragma: no cover - only a closed loop raises here
                raise
