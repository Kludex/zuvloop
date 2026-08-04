from __future__ import annotations

import argparse
import asyncio
import os
import socket
import statistics
import sys
import time
from collections.abc import Callable, Coroutine
from typing import Any

import zuv

Factory = Callable[[], asyncio.AbstractEventLoop]
Benchmark = Callable[[], Coroutine[Any, Any, float]]


def loop_factories() -> dict[str, Factory]:
    factories: dict[str, Factory] = {"asyncio": asyncio.new_event_loop, "zuv": zuv.new_event_loop}
    try:
        import uvloop
    except ImportError:
        return factories
    factories["uvloop"] = uvloop.new_event_loop
    return factories


def run_on(factory: Factory, benchmark: Benchmark) -> float:
    loop = factory()
    try:
        return loop.run_until_complete(benchmark())
    finally:
        loop.close()


def _noop() -> None:
    pass


async def bench_call_soon(iterations: int = 200_000) -> float:
    """Dispatch cost: a full batch scheduled, then drained in one go."""
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    seen = 0

    def step() -> None:
        nonlocal seen
        seen += 1
        if seen == iterations:
            done.set_result(None)

    started = time.perf_counter()
    for _ in range(iterations):
        loop.call_soon(step)
    await done
    return iterations / (time.perf_counter() - started)


async def bench_call_soon_args(iterations: int = 200_000) -> float:
    """Same, with arguments - asyncio packs these into a tuple per call."""
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    seen = 0

    def step(_a: int, _b: int, _c: int) -> None:
        nonlocal seen
        seen += 1
        if seen == iterations:
            done.set_result(None)

    started = time.perf_counter()
    for _ in range(iterations):
        loop.call_soon(step, 1, 2, 3)
    await done
    return iterations / (time.perf_counter() - started)


async def bench_timers(iterations: int = 200_000) -> float:
    """Timer churn: schedule then cancel, never firing."""
    loop = asyncio.get_running_loop()
    started = time.perf_counter()
    for _ in range(iterations):
        loop.call_later(30, _noop).cancel()
    return iterations / (time.perf_counter() - started)


async def bench_sleep_zero(iterations: int = 30_000) -> float:
    """Loop iterations per second: one callback per turn, poll included."""
    started = time.perf_counter()
    for _ in range(iterations):
        await asyncio.sleep(0)
    return iterations / (time.perf_counter() - started)


async def bench_echo(payload_size: int = 1024, roundtrips: int = 50_000) -> float:
    """Protocol round trips over a loopback TCP connection."""
    loop = asyncio.get_running_loop()
    payload = os.urandom(payload_size)

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
            while self.pending >= payload_size:
                self.pending -= payload_size
                self.remaining -= 1
                if self.remaining == 0:
                    self.done.set_result(None)
                    return
                self.transport.write(payload)  # type: ignore[attr-defined]

    server = await loop.create_server(EchoServer, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    started = time.perf_counter()
    transport, client = await loop.create_connection(EchoClient, "127.0.0.1", port)
    await client.done
    elapsed = time.perf_counter() - started
    transport.close()
    server.close()
    await server.wait_closed()
    return roundtrips / elapsed


async def bench_stream(total: int = 512 << 20) -> float:
    """Bulk transfer in bytes per second, with flow control engaged."""
    loop = asyncio.get_running_loop()
    chunk = b"x" * 65536
    sinks: list[Sink] = []

    class Sink(asyncio.Protocol):
        def __init__(self) -> None:
            self.received = 0
            self.done = loop.create_future()
            sinks.append(self)

        def data_received(self, data: bytes) -> None:
            self.received += len(data)
            if self.received >= total and not self.done.done():
                self.done.set_result(None)

    server = await loop.create_server(Sink, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    transport, _protocol = await loop.create_connection(asyncio.Protocol, "127.0.0.1", port)

    started = time.perf_counter()
    sent = 0
    while sent < total:
        transport.write(chunk)
        sent += len(chunk)
        if transport.get_write_buffer_size() > (4 << 20):
            await asyncio.sleep(0)
    await sinks[0].done
    elapsed = time.perf_counter() - started

    transport.close()
    server.close()
    await server.wait_closed()
    return total / elapsed


async def bench_getaddrinfo(iterations: int = 5_000) -> float:
    """Resolution rate; zuv resolves on libuv's threadpool, not the executor."""
    loop = asyncio.get_running_loop()
    started = time.perf_counter()
    for _ in range(iterations):
        await loop.getaddrinfo("127.0.0.1", 80, type=socket.SOCK_STREAM)
    return iterations / (time.perf_counter() - started)


BENCHMARKS: dict[str, tuple[Benchmark, str]] = {
    "call_soon": (bench_call_soon, "callbacks/s"),
    "call_soon_args": (bench_call_soon_args, "callbacks/s"),
    "timers": (bench_timers, "timers/s"),
    "sleep_zero": (bench_sleep_zero, "iterations/s"),
    "echo_1kb": (bench_echo, "roundtrips/s"),
    "stream": (bench_stream, "bytes/s"),
    "getaddrinfo": (bench_getaddrinfo, "lookups/s"),
}


def humanise(value: float, unit: str) -> str:
    if unit == "bytes/s":
        return f"{value / (1 << 20):>12,.0f} MiB/s"
    return f"{value:>15,.0f} {unit}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare zuv against asyncio and uvloop.")
    parser.add_argument("--repeat", type=int, default=3, help="runs per benchmark; the best is reported")
    parser.add_argument("--only", nargs="*", choices=sorted(BENCHMARKS), help="benchmarks to run")
    args = parser.parse_args()

    factories = loop_factories()
    baseline = "uvloop" if "uvloop" in factories else "asyncio"
    print(f"python {sys.version.split()[0]}   libuv {zuv.libuv_version()}\n")

    for name in args.only or list(BENCHMARKS):
        benchmark, unit = BENCHMARKS[name]
        print(name)
        results: dict[str, float] = {}
        for label, factory in factories.items():
            samples = [run_on(factory, benchmark) for _ in range(args.repeat)]
            results[label] = max(samples)
            spread = statistics.pstdev(samples) / results[label] if len(samples) > 1 else 0.0
            print(f"  {label:<8}{humanise(results[label], unit)}  (+/- {spread:.1%})")
        print(f"  {'':<8}{'zuv / ' + baseline:>15}  {results['zuv'] / results[baseline]:.2f}x\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
