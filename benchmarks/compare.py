"""Compare zuvloop against uvloop on one workload, honestly.

    uv run --group bench python benchmarks/compare.py

`test_benchmarks.py` records what changed between commits, which is what CodSpeed
is for, but its table cannot answer "how does this compare to uvloop" - see the
note there. This alternates the two loops inside a single process so machine
drift moves both arms equally, and reports the median as well as the minimum: a
minimum taken across distributions with different spread favours whichever arm is
noisier, and uvloop's spread here runs several times zuvloop's.
"""

from __future__ import annotations

import asyncio
import gc
import os
import statistics
import time
from collections.abc import Callable

import uvloop

import zuvloop

PAYLOAD = os.urandom(1024)
ROUNDTRIPS = 2000
REPS = 60

Factory = Callable[[], asyncio.AbstractEventLoop]


class Arm:
    """One loop, its echo server, and the samples taken against it."""

    def __init__(self, factory: Factory) -> None:
        self.loop = factory()
        self.wall: list[float] = []
        self.cpu: list[float] = []

        class Server(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                self.transport = transport

            def data_received(self, data: bytes) -> None:
                self.transport.write(data)  # type: ignore[attr-defined]

        self.server = self.loop.run_until_complete(self.loop.create_server(Server, "127.0.0.1", 0))
        self.port: int = self.server.sockets[0].getsockname()[1]

    def _client(self) -> type[asyncio.Protocol]:
        loop = self.loop

        class Client(asyncio.Protocol):
            def __init__(self) -> None:
                self.pending = 0
                self.remaining = ROUNDTRIPS
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

        return Client

    def run(self) -> None:
        client_type = self._client()

        async def once() -> None:
            transport, protocol = await self.loop.create_connection(client_type, "127.0.0.1", self.port)
            await protocol.done  # type: ignore[attr-defined]
            transport.close()

        self.loop.run_until_complete(once())

    def sample(self) -> None:
        started_cpu = time.process_time()
        started = time.perf_counter()
        self.run()
        self.wall.append(time.perf_counter() - started)
        self.cpu.append(time.process_time() - started_cpu)

    def close(self) -> None:
        self.server.close()
        self.loop.run_until_complete(self.server.wait_closed())
        self.loop.close()

    def report(self, name: str) -> None:
        spread = statistics.stdev(self.wall) / statistics.mean(self.wall) * 100
        print(
            f"  {name:8s} wall min {min(self.wall) * 1000:7.2f}ms"
            f"  median {statistics.median(self.wall) * 1000:7.2f}ms"
            f"  (+/- {spread:4.1f}%)   cpu min {min(self.cpu) * 1000:7.2f}ms"
        )


def main() -> None:
    arms = {"zuvloop": Arm(zuvloop.new_event_loop), "uvloop": Arm(uvloop.new_event_loop)}
    for arm in arms.values():
        for _ in range(3):
            arm.run()

    gc.disable()
    try:
        for rep in range(REPS):
            # Alternating the order too, so neither arm always follows the other.
            names = list(arms) if rep % 2 == 0 else list(arms)[::-1]
            for name in names:
                arms[name].sample()
    finally:
        gc.enable()

    for name, arm in arms.items():
        arm.close()
        arm.report(name)

    fast, slow = arms["zuvloop"], arms["uvloop"]
    wall_min = min(slow.wall) / min(fast.wall)
    wall_median = statistics.median(slow.wall) / statistics.median(fast.wall)
    cpu_min = min(slow.cpu) / min(fast.cpu)
    cpu_median = statistics.median(slow.cpu) / statistics.median(fast.cpu)
    print(f"\n  zuvloop / uvloop  wall: min {wall_min:.3f}x  median {wall_median:.3f}x")
    print(f"                     cpu: min {cpu_min:.3f}x  median {cpu_median:.3f}x")


if __name__ == "__main__":
    main()
