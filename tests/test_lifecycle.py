from __future__ import annotations

import asyncio
import concurrent.futures
import gc
import socket
import sys
import threading
import time
import weakref
from collections.abc import AsyncGenerator, Callable, Mapping

import pytest

import zuvloop
from tests.conftest import collect_contexts, running_loop
from zuvloop._base import _shutdown_executor
from zuvloop._connect import ConnectionOperations

pytestmark = pytest.mark.anyio

# A socketpair is an AF_UNIX socket on POSIX - a pipe to libuv - and an
# AF_INET socket on Windows, where only the TCP spelling can adopt it.
PAIR_KIND = 0 if sys.platform == "win32" else 1


def test_run_returns_the_coroutine_result() -> None:
    async def main() -> str:
        await asyncio.sleep(0)
        return "done"

    assert zuvloop.run(main()) == "done"


def test_self_pipe_drain_stops_watching_at_eof() -> None:
    loop = zuvloop.new_event_loop()
    errors: list[BaseException] = []

    def run() -> None:
        try:
            loop.run_forever()
        except BaseException as exc:  # pragma: no cover - assertion diagnostic
            errors.append(exc)

    loop._csock.close()
    loop.call_later(0.02, loop.stop)
    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    thread.join(1)
    try:
        assert not thread.is_alive(), "event loop kept polling the self-pipe at EOF"
        assert errors == []
        assert loop.remove_reader(loop._ssock.fileno()) is False
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(1)
        if not loop.is_running():  # pragma: no branch - watchdog failure may leave a daemon spinning
            loop.close()


def test_run_honours_debug_mode() -> None:
    async def main() -> bool:
        return running_loop().get_debug()

    assert zuvloop.run(main(), debug=True) is True


def test_failed_transport_open_does_not_corrupt_the_loop() -> None:
    loop = zuvloop.new_event_loop()
    try:
        with pytest.raises(OSError):
            loop._make_transport(-1, 0, asyncio.Protocol(), None, None, None)
    finally:
        loop.close()


def test_failed_datagram_open_does_not_reopen_the_closing_handle() -> None:
    loop = zuvloop.new_event_loop()
    try:
        with pytest.raises(OSError):
            loop._make_datagram_transport(-1, socket.AF_INET, False, asyncio.DatagramProtocol(), {})
    finally:
        loop.close()


def test_failed_transport_open_does_not_retain_the_loop() -> None:
    loop = zuvloop.new_event_loop()
    loop_ref = weakref.ref(loop)

    with pytest.raises(OSError):
        loop._make_transport(-1, 0, asyncio.Protocol(), None, None, None)

    loop._ssock.close()
    loop._csock.close()
    del loop
    gc.collect()

    assert loop_ref() is None


def test_failed_transport_construction_does_not_adopt_the_descriptor() -> None:
    loop = zuvloop.new_event_loop()
    left, right = socket.socketpair()
    try:
        with pytest.raises(AttributeError, match="connection_made"):
            loop._make_transport(left.fileno(), PAIR_KIND, object(), None, None, None)  # type: ignore[arg-type]
        loop.close()
        left.sendall(b"still owned")
        assert right.recv(11) == b"still owned"
    finally:
        left.close()
        right.close()
        loop.close()


async def test_failed_socket_view_construction_does_not_adopt_the_descriptor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    loop = running_loop()
    left, right = socket.socketpair()

    def fail_to_wrap_socket(_sock: socket.socket) -> None:
        raise RuntimeError("cannot wrap socket")

    monkeypatch.setattr("zuvloop._connect.trsock.TransportSocket", fail_to_wrap_socket)
    try:
        with pytest.raises(RuntimeError, match="cannot wrap socket"):
            ConnectionOperations._attach_transport(loop, left, asyncio.Protocol(), None, None)

        assert left.fileno() != -1
        left.sendall(b"still owned")
        assert right.recv(11) == b"still owned"
    finally:
        left.close()
        right.close()


@pytest.mark.parametrize("self_cycle", [False, True])
async def test_transport_releases_the_inherited_extra_slot(self_cycle: bool) -> None:
    class Token:
        pass

    loop = running_loop()
    left, right = socket.socketpair()
    transport = loop._make_transport(left.fileno(), PAIR_KIND, asyncio.Protocol(), None, None, None)
    left.detach()
    token: object = transport if self_cycle else Token()
    reference = weakref.ref(token)
    setattr(transport, "_extra", token)

    transport.close()
    right.close()
    await asyncio.sleep(0)
    del token, transport
    gc.collect()

    assert reference() is None


def test_loop_close_releases_open_transports() -> None:
    gc.collect()
    before = sum(type(obj) is zuvloop.Transport for obj in gc.get_objects())
    loop = zuvloop.new_event_loop()
    left, right = socket.socketpair()
    transport = loop._make_transport(left.fileno(), PAIR_KIND, asyncio.Protocol(), None, None, None)
    left.detach()
    right.close()

    loop.close()
    del transport, loop
    gc.collect()

    assert sum(type(obj) is zuvloop.Transport for obj in gc.get_objects()) == before


def test_live_loop_keeps_an_open_transport_alive() -> None:
    class Receiver(asyncio.Protocol):
        def __init__(self) -> None:
            self.received = bytearray()

        def data_received(self, data: bytes) -> None:
            self.received += data

    loop = zuvloop.new_event_loop()
    left, right = socket.socketpair()
    protocol = Receiver()
    transport = loop._make_transport(left.fileno(), PAIR_KIND, protocol, None, None, None)
    left.detach()
    loop.run_until_complete(asyncio.sleep(0))
    transport_ref = weakref.ref(transport)

    del transport
    gc.collect()

    assert transport_ref() is not None
    right.sendall(b"still watched")
    loop.run_until_complete(asyncio.sleep(0.01))
    assert protocol.received == b"still watched"

    live_transport = transport_ref()
    assert live_transport is not None
    live_transport.close()
    right.close()
    loop.run_until_complete(asyncio.sleep(0))
    loop.close()


def test_loop_close_rejects_writes_from_buffer_finalizers() -> None:
    transport_box: list[asyncio.Transport] = []

    class ReentrantBuffer(bytearray):
        def __del__(self) -> None:
            transport_box[0].write(b"reentrant")

    loop = zuvloop.new_event_loop()
    left, right = socket.socketpair()
    transport = loop._make_transport(left.fileno(), PAIR_KIND, asyncio.Protocol(), None, None, None)
    left.detach()
    loop.run_until_complete(asyncio.sleep(0))
    transport_box.append(transport)
    payload = ReentrantBuffer(b"pending")
    transport.write(payload)
    del payload

    loop.close()
    transport_box.clear()
    loop_ref = weakref.ref(loop)
    transport_ref = weakref.ref(transport)
    del transport, loop
    gc.collect()
    right.close()

    assert loop_ref() is None
    assert transport_ref() is None


def test_pending_flush_does_not_retain_an_abandoned_loop() -> None:
    loop = zuvloop.new_event_loop()
    left, right = socket.socketpair()
    transport = loop._make_transport(left.fileno(), PAIR_KIND, asyncio.Protocol(), None, None, None)
    left.detach()
    loop.run_until_complete(asyncio.sleep(0))
    transport.write(b"pending")
    loop_ref = weakref.ref(loop)
    transport_ref = weakref.ref(transport)
    right.close()

    with pytest.warns(ResourceWarning, match="unclosed event loop"):
        del transport, loop
        gc.collect()

    assert loop_ref() is None
    assert transport_ref() is None


@pytest.mark.skipif(sys.platform == "win32", reason="Windows loopback takes any write whole; none is left in flight")
def test_in_flight_write_does_not_retain_an_abandoned_loop() -> None:
    loop = zuvloop.new_event_loop()
    left, right = socket.socketpair()
    left.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
    transport = loop._make_transport(left.fileno(), PAIR_KIND, asyncio.Protocol(), None, None, None)
    left.detach()
    loop.run_until_complete(asyncio.sleep(0))

    transport.write(b"x" * (8 << 20))
    loop.run_until_complete(asyncio.sleep(0))
    assert transport.get_write_buffer_size() > 0
    loop_ref = weakref.ref(loop)
    transport_ref = weakref.ref(transport)

    with pytest.warns(ResourceWarning, match="unclosed event loop"):
        del transport, loop
        gc.collect()
    right.close()

    assert loop_ref() is None
    assert transport_ref() is None


def test_loop_close_releases_pending_dns_requests() -> None:
    loop = zuvloop.new_event_loop()
    futures = [loop._getaddrinfo(f"zuvloop-pending-{index}.invalid", 80, 0, 0, 0, 0) for index in range(128)]
    loop_ref = weakref.ref(loop)
    future_refs = [weakref.ref(future) for future in futures]

    started = time.monotonic()
    loop.close()
    assert time.monotonic() - started < 0.1
    del futures, loop
    gc.collect()

    assert loop_ref() is None
    assert all(ref() is None for ref in future_refs)


def test_pending_dns_does_not_retain_an_abandoned_loop() -> None:
    loop = zuvloop.new_event_loop()
    future = loop._getaddrinfo("zuvloop-abandoned.invalid", 80, 0, 0, 0, 0)
    loop_ref = weakref.ref(loop)
    future_ref = weakref.ref(future)

    with pytest.warns(ResourceWarning, match="unclosed event loop"):
        del future, loop
        gc.collect()

    assert loop_ref() is None
    assert future_ref() is None


def test_threadsafe_inbox_does_not_retain_an_abandoned_loop() -> None:
    loop = zuvloop.new_event_loop()
    handle = loop.call_soon_threadsafe(lambda: None)
    loop_ref = weakref.ref(loop)
    handle_ref = weakref.ref(handle)

    with pytest.warns(ResourceWarning, match="unclosed event loop"):
        del handle, loop
        gc.collect()

    assert loop_ref() is None
    assert handle_ref() is None


def test_close_tracks_a_retained_threadsafe_handle_cycle() -> None:
    loop = zuvloop.new_event_loop()
    handles: list[asyncio.Handle] = []
    handle = loop.call_soon_threadsafe(handles.clear)
    handles.append(handle)
    loop_ref = weakref.ref(loop)
    handle_ref = weakref.ref(handle)
    loop.close()

    del handle, handles, loop
    gc.collect()

    assert loop_ref() is None
    assert handle_ref() is None


def test_retained_threadsafe_handle_cycle_does_not_retain_an_abandoned_loop() -> None:
    loop = zuvloop.new_event_loop()
    handles: list[asyncio.Handle] = []
    handle = loop.call_soon_threadsafe(handles.clear)
    handle.get_context()
    handles.append(handle)
    loop_ref = weakref.ref(loop)
    handle_ref = weakref.ref(handle)

    with pytest.warns(ResourceWarning, match="unclosed event loop"):
        del handle, handles, loop
        gc.collect()

    assert loop_ref() is None
    assert handle_ref() is None


def test_retained_threadsafe_handle_keeps_its_loop_reachable() -> None:
    loop = zuvloop.new_event_loop()
    handles: list[asyncio.Handle] = []
    handle = loop.call_soon_threadsafe(handles.clear)
    handles.append(handle)
    loop_ref = weakref.ref(loop)

    del handles, loop
    gc.collect()

    retained_loop = loop_ref()
    assert retained_loop is not None
    retained_loop.close()
    del handle, retained_loop
    gc.collect()


def test_asyncio_run_accepts_the_loop_factory() -> None:
    async def main() -> str:
        return type(running_loop()).__name__

    assert asyncio.run(main(), loop_factory=zuvloop.new_event_loop) == "EventLoop"


def test_stop_runs_the_rest_of_the_batch(loop: zuvloop.EventLoop) -> None:
    seen: list[str] = []
    loop.call_soon(seen.append, "first")
    loop.call_soon(loop.stop)
    loop.call_soon(seen.append, "queued before stop ran")
    loop.run_forever()
    assert seen == ["first", "queued before stop ran"]


def test_callbacks_scheduled_after_stop_wait_for_the_next_run(loop: zuvloop.EventLoop) -> None:
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


def test_run_until_complete_returns_a_result(loop: zuvloop.EventLoop) -> None:
    async def main() -> int:
        await asyncio.sleep(0)
        return 7

    assert loop.run_until_complete(main()) == 7


def test_run_until_complete_propagates_exceptions(loop: zuvloop.EventLoop) -> None:
    async def main() -> None:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        loop.run_until_complete(main())


def test_run_until_complete_accepts_a_future(loop: zuvloop.EventLoop) -> None:
    future = loop.create_future()
    loop.call_soon(future.set_result, "ready")
    assert loop.run_until_complete(future) == "ready"


def test_run_until_complete_rejects_a_loop_stopped_early(loop: zuvloop.EventLoop) -> None:
    async def main() -> None:
        await asyncio.sleep(10)

    loop.call_later(0.01, loop.stop)
    with pytest.raises(RuntimeError, match="stopped before Future completed"):
        loop.run_until_complete(main())


def test_system_exit_escapes_the_loop(loop: zuvloop.EventLoop) -> None:
    def raiser() -> None:
        raise SystemExit(3)

    loop.call_soon(raiser)
    with pytest.raises(SystemExit):
        loop.run_forever()


def test_system_exit_escapes_run_until_complete(loop: zuvloop.EventLoop) -> None:
    async def main() -> None:
        await asyncio.sleep(10)

    def raiser() -> None:
        raise KeyboardInterrupt

    loop.call_later(0.01, raiser)
    with pytest.raises(KeyboardInterrupt):
        loop.run_until_complete(main())


def test_a_running_loop_cannot_be_run_again(loop: zuvloop.EventLoop) -> None:
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
        other = zuvloop.new_event_loop()
        try:
            with pytest.raises(RuntimeError, match="another loop is running"):
                other.run_forever()
        finally:
            other.close()

    zuvloop.run(main())


def test_a_running_loop_cannot_be_closed(loop: zuvloop.EventLoop) -> None:
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


def test_closing_twice_is_harmless(loop: zuvloop.EventLoop) -> None:
    loop.close()
    loop.close()
    assert loop.is_closed() is True


def test_a_closed_loop_rejects_work(loop: zuvloop.EventLoop) -> None:
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


def test_close_shuts_down_the_default_executor(loop: zuvloop.EventLoop) -> None:
    async def main() -> None:
        await loop.run_in_executor(None, time.monotonic)

    loop.run_until_complete(main())
    assert loop._default_executor is not None
    loop.close()
    assert loop._default_executor is None


async def test_shutdown_default_executor_joins_threads() -> None:
    loop = running_loop()
    await loop.run_in_executor(None, time.monotonic)
    await loop.shutdown_default_executor()
    with pytest.raises(RuntimeError, match="Executor shutdown"):
        await loop.run_in_executor(None, time.monotonic)
    loop._executor_shutdown_called = False
    loop.set_default_executor(concurrent.futures.ThreadPoolExecutor(1))


async def test_shutdown_default_executor_without_one_is_a_no_op() -> None:
    loop = zuvloop.new_event_loop()
    try:
        await loop.shutdown_default_executor()
    finally:
        loop.close()


def test_shutdown_default_executor_times_out(loop: zuvloop.EventLoop) -> None:
    release = threading.Event()

    async def main() -> None:
        await loop.run_in_executor(None, release.wait)

    loop.create_task(main())
    loop.run_until_complete(asyncio.sleep(0.05))
    with pytest.warns(RuntimeWarning, match="did not finish joining"):
        loop.run_until_complete(loop.shutdown_default_executor(0.01))

    # The abandoned thread outlives the loop; releasing it only once the loop is
    # closed is what the timeout path leaves behind in practice.
    loop.close()
    release.set()
    time.sleep(0.2)


def test_executor_shutdown_tolerates_close_racing_with_notification() -> None:
    class ClosingLoop:
        closed = False

        def call_soon_threadsafe(self, callback: Callable[..., object], *args: object) -> None:
            self.closed = True
            raise RuntimeError("Event loop is closed")

        def is_closed(self) -> bool:
            return self.closed

    class FinishedExecutor:
        def shutdown(self, *, wait: bool) -> None:
            assert wait

    future_loop = zuvloop.new_event_loop()
    try:
        fake_future = future_loop.create_future()
        _shutdown_executor(ClosingLoop(), fake_future, FinishedExecutor())
    finally:
        future_loop.close()


async def test_shutdown_asyncgens_closes_generators() -> None:
    loop = running_loop()
    closed: list[str] = []

    async def generator() -> AsyncGenerator[int]:
        try:
            yield 1
        finally:
            closed.append("closed")

    agen = generator()
    assert await anext(agen) == 1
    await loop.shutdown_asyncgens()
    assert closed == ["closed"]
    loop._asyncgens_shutdown_called = False


async def test_shutdown_asyncgens_reports_failures() -> None:
    loop = running_loop()
    reported = collect_contexts(loop)

    async def generator() -> AsyncGenerator[int]:
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
    loop = zuvloop.new_event_loop()
    try:
        await loop.shutdown_asyncgens()
    finally:
        loop.close()


def test_asyncgens_scheduled_after_shutdown_warn(loop: zuvloop.EventLoop) -> None:
    async def generator() -> AsyncGenerator[int]:
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
        loop = zuvloop.new_event_loop()
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
    loop = running_loop()
    task = loop.create_task(asyncio.sleep(0), name="named")
    assert task.get_name() == "named"
    await task


async def test_tasks_are_visible_to_asyncio_introspection() -> None:
    async def worker() -> None:
        await asyncio.sleep(0.05)

    task = running_loop().create_task(worker(), name="introspected")
    await asyncio.sleep(0)
    assert task in asyncio.all_tasks()
    assert asyncio.current_task() is not None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_an_interrupt_after_the_task_finishes_consumes_its_error(loop: zuvloop.EventLoop) -> None:
    def interrupt() -> None:
        raise KeyboardInterrupt

    async def main() -> None:
        loop.call_soon(interrupt)
        raise ValueError("finished with an error")

    with pytest.raises(KeyboardInterrupt):
        loop.run_until_complete(main())


async def test_an_exception_handler_may_stop_the_program() -> None:
    loop = running_loop()

    def handler(_loop: asyncio.AbstractEventLoop, _context: Mapping[str, object]) -> None:
        raise SystemExit(2)

    previous = loop.get_exception_handler()
    loop.set_exception_handler(handler)
    try:
        with pytest.raises(SystemExit):
            loop.call_exception_handler({"message": "trigger"})
    finally:
        loop.set_exception_handler(previous)


async def test_a_dropped_async_generator_is_finalized() -> None:
    import gc

    closed: list[str] = []

    async def generator() -> AsyncGenerator[int]:
        try:
            yield 1
        finally:
            closed.append("finalized")

    agen = generator()
    assert await anext(agen) == 1
    del agen
    gc.collect()
    await asyncio.sleep(0.05)
    assert closed == ["finalized"]


def test_an_async_generator_outliving_its_loop_is_not_rescheduled() -> None:
    import gc

    kept: list[AsyncGenerator[int]] = []

    async def generator() -> AsyncGenerator[int]:
        yield 1

    async def main() -> None:
        agen = generator()
        assert await anext(agen) == 1
        kept.append(agen)

    loop = zuvloop.new_event_loop()
    loop.run_until_complete(main())
    loop.close()
    kept.clear()
    gc.collect()
