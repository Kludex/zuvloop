"""Compare zuvloop against uvloop, honestly.

    uv run --group bench python benchmarks/compare.py
    uv run --group bench python benchmarks/compare.py echo timers

`test_benchmarks.py` records what changed between commits, which is what CodSpeed
is for, but its table cannot answer "how does this compare to uvloop" - see the
note there. This alternates the loops inside a single process so machine drift
moves both arms equally, and reports the median as well as the minimum: a minimum
taken across distributions with different spread favours whichever arm is
noisier, and uvloop's spread runs several times zuvloop's on some of these.
"""

from __future__ import annotations

import asyncio
import gc
import os
import socket
import statistics
import sys
import threading
import time
from collections.abc import Callable, Coroutine

import uvloop

import zuvloop

type Factory = Callable[[], asyncio.AbstractEventLoop]
type Workload = Callable[[asyncio.AbstractEventLoop], Callable[[], None]]

PAYLOAD = os.urandom(1024)
REPS = 40


# ---------------------------------------------------------------------------
# workloads
#
# Each builds whatever it needs against one loop and returns the thing to time.
# Setup stays outside the measurement; only the returned callable is timed.


def echo(loop: asyncio.AbstractEventLoop) -> Callable[[], None]:
    """A 1 KiB ping-pong, 2000 times. Latency on one connection."""
    roundtrips = 2000

    class Server(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport

        def data_received(self, data: bytes) -> None:
            self.transport.write(data)  # type: ignore[attr-defined]

    server = loop.run_until_complete(loop.create_server(Server, "127.0.0.1", 0))
    port = server.sockets[0].getsockname()[1]

    class Client(asyncio.Protocol):
        def __init__(self) -> None:
            self.pending = 0
            self.remaining = roundtrips
            self.done = loop.create_future()

        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport
            transport.write(PAYLOAD)  # type: ignore[attr-defined]

        def data_received(self, data: bytes) -> None:
            self.pending += len(data)
            while self.pending >= len(PAYLOAD):
                self.pending -= len(PAYLOAD)
                self.remaining -= 1
                if self.remaining == 0:
                    self.done.set_result(None)
                    return
                self.transport.write(PAYLOAD)  # type: ignore[attr-defined]

    async def once() -> None:
        transport, protocol = await loop.create_connection(Client, "127.0.0.1", port)
        await protocol.done
        transport.close()

    return _driver(loop, once)


def split_write(loop: asyncio.AbstractEventLoop) -> Callable[[], None]:
    """A response written as a header and a body, as ASGI does it."""
    head, body = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\n", b"ok"
    responses = 1000

    class Server(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport

        def data_received(self, data: bytes) -> None:
            for _ in range(data.count(b"\n")):
                self.transport.write(head)  # type: ignore[attr-defined]
                self.transport.write(body)  # type: ignore[attr-defined]

    server = loop.run_until_complete(loop.create_server(Server, "127.0.0.1", 0))
    port = server.sockets[0].getsockname()[1]

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

    async def once() -> None:
        transport, protocol = await loop.create_connection(Client, "127.0.0.1", port)
        await protocol.done
        transport.close()

    return _driver(loop, once)


def bulk(loop: asyncio.AbstractEventLoop) -> Callable[[], None]:
    """16 MiB in one direction, with flow control engaged."""
    chunk = b"x" * 65536
    total = 16 << 20
    # `create_connection` returns on the client's connect; the server's protocol
    # is built when the loop accepts, which need not have happened yet.
    accepted: asyncio.Future[Sink] = loop.create_future()

    class Sink(asyncio.Protocol):
        def __init__(self) -> None:
            self.received = 0
            self.done: asyncio.Future[None] = loop.create_future()
            accepted.set_result(self)

        def data_received(self, data: bytes) -> None:
            self.received += len(data)
            if self.received >= total and not self.done.done():
                self.done.set_result(None)

    server = loop.run_until_complete(loop.create_server(Sink, "127.0.0.1", 0))
    port = server.sockets[0].getsockname()[1]

    async def once() -> None:
        nonlocal accepted
        accepted = loop.create_future()
        transport, _protocol = await loop.create_connection(asyncio.Protocol, "127.0.0.1", port)
        sink = await accepted
        sent = 0
        while sent < total:
            transport.write(chunk)
            sent += len(chunk)
            if transport.get_write_buffer_size() > (4 << 20):
                await asyncio.sleep(0)
        await sink.done
        transport.close()

    return _driver(loop, once)


def call_soon(loop: asyncio.AbstractEventLoop) -> Callable[[], None]:
    """Scheduling on its own: 10000 callbacks, no arguments."""
    iterations = 10_000

    async def once() -> None:
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

    return _driver(loop, once)


def call_soon_threadsafe(loop: asyncio.AbstractEventLoop) -> Callable[[], None]:
    """Cross-thread scheduling: 10000 callbacks issued from a worker thread."""
    iterations = 10_000

    async def once() -> None:
        done = loop.create_future()

        def worker() -> None:
            for _ in range(iterations):
                loop.call_soon_threadsafe(_noop)
            loop.call_soon_threadsafe(done.set_result, None)

        thread = threading.Thread(target=worker)
        thread.start()
        await done
        thread.join()

    return _driver(loop, once)


def timers(loop: asyncio.AbstractEventLoop) -> Callable[[], None]:
    """Timer bookkeeping: schedule and cancel, never firing."""
    iterations = 10_000

    async def once() -> None:
        for _ in range(iterations):
            loop.call_later(30, _noop).cancel()

    return _driver(loop, once)


def getaddrinfo(loop: asyncio.AbstractEventLoop) -> Callable[[], None]:
    """An address literal, which is what create_connection is handed most often."""

    async def once() -> None:
        for _ in range(200):
            await loop.getaddrinfo("127.0.0.1", 80, type=socket.SOCK_STREAM)

    return _driver(loop, once)


def spawn(loop: asyncio.AbstractEventLoop) -> Callable[[], None]:
    """Twenty processes, spawned and reaped."""

    async def once() -> None:
        for _ in range(20):
            process = await asyncio.create_subprocess_exec(
                "/usr/bin/true",
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            await process.wait()

    return _driver(loop, once)


WORKLOADS: dict[str, Workload] = {
    "echo": echo,
    "split_write": split_write,
    "bulk": bulk,
    "call_soon": call_soon,
    "call_soon_threadsafe": call_soon_threadsafe,
    "timers": timers,
    "getaddrinfo": getaddrinfo,
    "spawn": spawn,
}


def _noop() -> None:
    pass


def _driver(loop: asyncio.AbstractEventLoop, once: Callable[[], Coroutine[None, None, None]]) -> Callable[[], None]:
    return lambda: loop.run_until_complete(once())


# ---------------------------------------------------------------------------
# measurement


class Arm:
    """One loop running one workload, and the samples taken against it."""

    def __init__(self, factory: Factory, workload: Workload) -> None:
        self.loop = factory()
        asyncio.set_event_loop(self.loop)
        self.run = workload(self.loop)
        self.wall: list[float] = []
        self.cpu: list[float] = []

    def sample(self) -> None:
        started_cpu = time.process_time()
        started = time.perf_counter()
        self.run()
        self.wall.append(time.perf_counter() - started)
        self.cpu.append(time.process_time() - started_cpu)

    def close(self) -> None:
        self.loop.close()


def compare(name: str, workload: Workload) -> None:
    arms = {
        "zuvloop": Arm(zuvloop.new_event_loop, workload),
        "uvloop": Arm(uvloop.new_event_loop, workload),
    }
    for arm in arms.values():
        for _ in range(3):
            arm.run()

    gc.disable()
    try:
        for rep in range(REPS):
            # Alternating the order too, so neither arm always follows the other.
            for label in list(arms) if rep % 2 == 0 else list(arms)[::-1]:
                arms[label].sample()
    finally:
        gc.enable()

    print(f"\n{name}")
    for label, arm in arms.items():
        spread = statistics.stdev(arm.wall) / statistics.mean(arm.wall) * 100
        print(
            f"  {label:8s} wall min {min(arm.wall) * 1000:8.3f}ms"
            f"  median {statistics.median(arm.wall) * 1000:8.3f}ms"
            f"  (+/- {spread:4.1f}%)   cpu min {min(arm.cpu) * 1000:8.3f}ms"
        )

    fast, slow = arms["zuvloop"], arms["uvloop"]
    ratios = (
        min(slow.wall) / min(fast.wall),
        statistics.median(slow.wall) / statistics.median(fast.wall),
        statistics.median(slow.cpu) / statistics.median(fast.cpu),
    )
    print(f"  {'':8s} zuvloop / uvloop  wall min {ratios[0]:.3f}x  median {ratios[1]:.3f}x  cpu {ratios[2]:.3f}x")

    for arm in arms.values():
        arm.close()


def main() -> int:
    wanted = sys.argv[1:] or list(WORKLOADS)
    unknown = [name for name in wanted if name not in WORKLOADS]
    if unknown:
        print(f"unknown workload(s): {', '.join(unknown)}", file=sys.stderr)
        print(f"available: {', '.join(WORKLOADS)}", file=sys.stderr)
        return 2
    for name in wanted:
        compare(name, WORKLOADS[name])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
