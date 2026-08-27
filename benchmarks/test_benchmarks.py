"""Benchmarks, measured by CodSpeed on every commit.

Each one is parametrised by event loop, so a run records what changed since the
last commit. `pytest-codspeed` does not run coroutines, so every benchmark is
driven through `run_until_complete` on a loop the fixture owns; loop construction
stays outside the measurement.

Do not compare the loops by the `Time (best)` column. `pytest-codspeed` 5.0.3
divides by `iter_per_round` twice - once building the stats and again rendering
the table - and picks that divisor per row from the row's own warmup, so a ratio
between two rows is scaled by a number that has nothing to do with either. It
reported this loop as 0.86x uvloop on `echo_roundtrips` when interleaved
measurement puts it at 1.09x. `compare.py` is the harness for that question.

The HTTP benchmarks are not here. They need a server process and an external load
generator, which is not something a benchmark harness can measure in-process -
see `uvicorn_bench.py`, `aiohttp_bench.py` and `write_batching.py`.
"""

from __future__ import annotations

import asyncio
import os
import socket
import threading
from collections.abc import Callable, Coroutine, Iterator

import pytest
from pytest_codspeed import BenchmarkFixture

import zuvloop

Factory = Callable[[], asyncio.AbstractEventLoop]

LOOPS: dict[str, Factory | None] = {"asyncio": None, "zuvloop": zuvloop.new_event_loop}
try:
    import uvloop
except ImportError:  # pragma: no cover - uvloop is an optional bench dependency
    pass
else:
    LOOPS["uvloop"] = uvloop.new_event_loop


@pytest.fixture(params=list(LOOPS), ids=list(LOOPS))
def loop(request: pytest.FixtureRequest) -> Iterator[asyncio.AbstractEventLoop]:
    factory = LOOPS[request.param]
    loop = factory() if factory is not None else asyncio.new_event_loop()
    yield loop
    loop.close()


def drive(loop: asyncio.AbstractEventLoop, work: Callable[[], Coroutine[None, None, None]]) -> Callable[[], None]:
    return lambda: loop.run_until_complete(work())


# ---------------------------------------------------------------------------
# scheduling


@pytest.mark.benchmark
def test_call_soon(benchmark: BenchmarkFixture, loop: asyncio.AbstractEventLoop) -> None:
    iterations = 10_000

    async def work() -> None:
        done = loop.create_future()
        seen = 0

        def step() -> None:
            nonlocal seen
            seen += 1
            if seen == iterations:
                done.set_result(None)

        for _ in range(iterations):
            loop.call_soon(step)
        await done

    benchmark(drive(loop, work))


@pytest.mark.benchmark
def test_call_soon_threadsafe(benchmark: BenchmarkFixture, loop: asyncio.AbstractEventLoop) -> None:
    """Cross-thread scheduling, where the 3.14 handle contract sets the price."""
    iterations = 10_000

    async def work() -> None:
        done = loop.create_future()

        def worker() -> None:
            for _ in range(iterations):
                loop.call_soon_threadsafe(_noop)
            loop.call_soon_threadsafe(done.set_result, None)

        thread = threading.Thread(target=worker)
        thread.start()
        await done
        thread.join()

    benchmark(drive(loop, work))


@pytest.mark.benchmark
def test_threadsafe_flood_timer_fairness(benchmark: BenchmarkFixture, loop: asyncio.AbstractEventLoop) -> None:
    """Bounded producer flood around a 10 ms timer, with cleanup between samples."""
    delay = 0.01
    producers = 4
    limit_per_producer = 250_000
    accepted_samples: list[int] = []

    class Flood:
        def __init__(self) -> None:
            self.stop = threading.Event()
            self.gate = threading.Event()
            self.accepted = [0] * producers
            self.started = 0.0
            self.fired = 0.0
            self.threads = [
                threading.Thread(target=self.produce, args=(index,), daemon=True) for index in range(producers)
            ]

        def produce(self, index: int) -> None:
            self.gate.wait()
            count = 0
            while count < limit_per_producer and not self.stop.is_set():
                loop.call_soon_threadsafe(_noop)
                count += 1
            self.accepted[index] = count

        def timer_fired(self) -> None:
            self.stop.set()
            self.fired = loop.time()
            loop.stop()

    def setup() -> tuple[tuple[Flood], dict[str, int]]:
        flood = Flood()
        for thread in flood.threads:
            thread.start()
        return (flood,), {}

    def measure(flood: Flood) -> float:
        flood.started = loop.time()
        loop.call_later(delay, flood.timer_fired)
        flood.gate.set()
        loop.run_forever()
        return flood.fired - flood.started

    def teardown(flood: Flood) -> None:
        try:
            flood.stop.set()
            flood.gate.set()
        finally:
            for thread in flood.threads:
                thread.join()

        # Every producer has stopped, so this FIFO marker runs after every
        # callback it accepted and leaves the next benchmark sample clean.
        drained: asyncio.Future[None] = loop.create_future()
        loop.call_soon(drained.set_result, None)
        loop.run_until_complete(drained)
        accepted_samples.append(sum(flood.accepted))

    latency = benchmark.pedantic(measure, setup=setup, teardown=teardown, rounds=10)
    assert latency >= delay
    assert accepted_samples
    assert all(0 < accepted <= producers * limit_per_producer for accepted in accepted_samples)


@pytest.mark.benchmark
def test_call_soon_args(benchmark: BenchmarkFixture, loop: asyncio.AbstractEventLoop) -> None:
    """Arguments are where the handle layout shows: asyncio packs a tuple per call."""
    iterations = 10_000

    async def work() -> None:
        done = loop.create_future()
        seen = 0

        def step(_a: int, _b: int, _c: int) -> None:
            nonlocal seen
            seen += 1
            if seen == iterations:
                done.set_result(None)

        for _ in range(iterations):
            loop.call_soon(step, 1, 2, 3)
        await done

    benchmark(drive(loop, work))


@pytest.mark.benchmark
def test_timers(benchmark: BenchmarkFixture, loop: asyncio.AbstractEventLoop) -> None:
    """Schedule then cancel, never firing: timer bookkeeping on its own."""
    iterations = 10_000

    async def work() -> None:
        for _ in range(iterations):
            loop.call_later(30, _noop).cancel()

    benchmark(drive(loop, work))


@pytest.mark.benchmark
def test_loop_iterations(benchmark: BenchmarkFixture, loop: asyncio.AbstractEventLoop) -> None:
    """One callback per turn, so the poll is included in every iteration."""

    async def work() -> None:
        for _ in range(1_000):
            await asyncio.sleep(0)

    benchmark(drive(loop, work))


@pytest.mark.benchmark
def test_tasks(benchmark: BenchmarkFixture, loop: asyncio.AbstractEventLoop) -> None:
    """Create, schedule and finish a batch of tasks that each yield once."""

    async def tiny_task() -> None:
        await asyncio.sleep(0)

    async def work() -> None:
        await asyncio.gather(*(tiny_task() for _ in range(5_000)))

    benchmark(drive(loop, work))


# ---------------------------------------------------------------------------
# networking


@pytest.mark.benchmark
def test_echo_roundtrips(benchmark: BenchmarkFixture, loop: asyncio.AbstractEventLoop) -> None:
    payload = os.urandom(1024)
    roundtrips = 2_000

    class EchoServer(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport

        def data_received(self, data: bytes) -> None:
            self.transport.write(data)  # type: ignore[attr-defined]

    class EchoClient(asyncio.Protocol):
        def __init__(self) -> None:
            self.pending = 0
            self.remaining = roundtrips
            self.done = loop.create_future()

        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport
            transport.write(payload)  # type: ignore[attr-defined]

        def data_received(self, data: bytes) -> None:
            self.pending += len(data)
            while self.pending >= len(payload):
                self.pending -= len(payload)
                self.remaining -= 1
                if self.remaining == 0:
                    self.done.set_result(None)
                    return
                self.transport.write(payload)  # type: ignore[attr-defined]

    server = loop.run_until_complete(loop.create_server(EchoServer, "127.0.0.1", 0))
    port = server.sockets[0].getsockname()[1]

    async def work() -> None:
        transport, client = await loop.create_connection(EchoClient, "127.0.0.1", port)
        await client.done
        transport.close()

    try:
        benchmark(drive(loop, work))
    finally:
        server.close()
        loop.run_until_complete(server.wait_closed())


@pytest.mark.benchmark
def test_split_response_write(benchmark: BenchmarkFixture, loop: asyncio.AbstractEventLoop) -> None:
    """A response sent as a header write plus a body write, as ASGI does it.

    A loop that sends each write as it arrives spends a syscall per piece.
    """
    head, body = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n", b"ok"
    responses = 1_000

    class Server(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport

        def data_received(self, data: bytes) -> None:
            for _ in range(data.count(b"\n")):
                self.transport.write(head)  # type: ignore[attr-defined]
                self.transport.write(body)  # type: ignore[attr-defined]

    class Client(asyncio.Protocol):
        def __init__(self) -> None:
            self.seen = 0
            self.remaining = responses
            self.done = loop.create_future()

        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport
            transport.write(b"\n")  # type: ignore[attr-defined]

        def data_received(self, data: bytes) -> None:
            self.seen += len(data)
            while self.seen >= len(head) + len(body):
                self.seen -= len(head) + len(body)
                self.remaining -= 1
                if self.remaining == 0:
                    self.done.set_result(None)
                    return
                self.transport.write(b"\n")  # type: ignore[attr-defined]

    server = loop.run_until_complete(loop.create_server(Server, "127.0.0.1", 0))
    port = server.sockets[0].getsockname()[1]

    async def work() -> None:
        transport, client = await loop.create_connection(Client, "127.0.0.1", port)
        await client.done
        transport.close()

    try:
        benchmark(drive(loop, work))
    finally:
        server.close()
        loop.run_until_complete(server.wait_closed())


@pytest.mark.benchmark
def test_bulk_stream(benchmark: BenchmarkFixture, loop: asyncio.AbstractEventLoop) -> None:
    """Bulk transfer with flow control engaged."""
    chunk = b"x" * 65536
    total = 16 << 20

    class Sink(asyncio.Protocol):
        def __init__(self) -> None:
            self.received = 0
            self.done = loop.create_future()
            sinks.append(self)

        def data_received(self, data: bytes) -> None:
            self.received += len(data)
            if self.received >= total and not self.done.done():
                self.done.set_result(None)

    sinks: list[Sink] = []
    server = loop.run_until_complete(loop.create_server(Sink, "127.0.0.1", 0))
    port = server.sockets[0].getsockname()[1]

    async def work() -> None:
        sinks.clear()
        transport, _protocol = await loop.create_connection(asyncio.Protocol, "127.0.0.1", port)
        sent = 0
        while sent < total:
            transport.write(chunk)
            sent += len(chunk)
            if transport.get_write_buffer_size() > (4 << 20):
                await asyncio.sleep(0)
        await sinks[0].done
        transport.close()

    try:
        benchmark(drive(loop, work))
    finally:
        server.close()
        loop.run_until_complete(server.wait_closed())


@pytest.mark.benchmark
def test_getaddrinfo_literal(benchmark: BenchmarkFixture, loop: asyncio.AbstractEventLoop) -> None:
    """An address literal, which is what `create_connection` is handed most often."""

    async def work() -> None:
        for _ in range(200):
            await loop.getaddrinfo("127.0.0.1", 80, type=socket.SOCK_STREAM)

    benchmark(drive(loop, work))


# ---------------------------------------------------------------------------
# processes


@pytest.mark.benchmark
def test_process_spawn(benchmark: BenchmarkFixture, loop: asyncio.AbstractEventLoop) -> None:
    async def work() -> None:
        for _ in range(20):
            process = await asyncio.create_subprocess_exec(
                "/usr/bin/true",
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await process.wait()

    benchmark(drive(loop, work))


@pytest.mark.benchmark
def test_process_pipe(benchmark: BenchmarkFixture, loop: asyncio.AbstractEventLoop) -> None:
    payload = b"p" * (1 << 20)

    async def work() -> None:
        process = await asyncio.create_subprocess_exec(
            "/bin/cat",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _stderr = await process.communicate(payload)
        assert stdout == payload

    benchmark(drive(loop, work))


def _noop() -> None:
    pass
