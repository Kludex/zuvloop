"""Absolute throughput per loop - the numbers behind the README table.

    uv run --group bench python benchmarks/run.py
    uv run --group bench python benchmarks/run.py --only call_soon timers

`compare.py` answers "how does this compare to uvloop" with interleaved wall
times, and CodSpeed tracks what changed between commits. Neither reports the
operations per second a reader can hold against another machine, which is what
the README publishes; this is the harness that produces them. The HTTP rows
come from `uvicorn_bench.py` and `aiohttp_bench.py`, which need a server
process and `oha`.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import socket
import statistics
import sys
import threading
import time
from collections.abc import Callable, Coroutine

import zuvloop

Factory = Callable[[], asyncio.AbstractEventLoop]
Benchmark = Callable[[], Coroutine[None, None, float]]


def loop_factories() -> dict[str, Factory]:
    factories: dict[str, Factory] = {"asyncio": asyncio.new_event_loop, "zuvloop": zuvloop.new_event_loop}
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


async def bench_call_soon_threadsafe(iterations: int = 200_000) -> float:
    """Cross-thread dispatch: a worker thread schedules the whole batch."""
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    started = 0.0

    def worker() -> None:
        nonlocal started
        started = time.perf_counter()
        for _ in range(iterations):
            loop.call_soon_threadsafe(_noop)
        loop.call_soon_threadsafe(done.set_result, None)

    thread = threading.Thread(target=worker)
    thread.start()
    await done
    elapsed = time.perf_counter() - started
    thread.join()
    return iterations / elapsed


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
    """Resolution rate; zuvloop resolves on libuv's threadpool, not the executor."""
    loop = asyncio.get_running_loop()
    started = time.perf_counter()
    for _ in range(iterations):
        await loop.getaddrinfo("127.0.0.1", 80, type=socket.SOCK_STREAM)
    return iterations / (time.perf_counter() - started)


async def bench_process_spawn(iterations: int = 250) -> float:
    """Spawn and reap a minimal process with no pipe traffic."""
    started = time.perf_counter()
    for _ in range(iterations):
        process = await asyncio.create_subprocess_exec(
            "/usr/bin/true",
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        if await process.wait() != 0:
            raise RuntimeError("subprocess exited unsuccessfully")
    return iterations / (time.perf_counter() - started)


async def bench_process_pipe(repetitions: int = 12, payload_size: int = 4 << 20) -> float:
    """Round-trip bytes through a subprocess's stdin and stdout pipes."""
    payload = b"p" * payload_size
    started = time.perf_counter()
    for _ in range(repetitions):
        process = await asyncio.create_subprocess_exec(
            "/bin/cat",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        stdout, _stderr = await process.communicate(payload)
        if process.returncode != 0 or stdout != payload:
            raise RuntimeError("subprocess pipe corrupted its payload")
    return repetitions * payload_size / (time.perf_counter() - started)


BENCHMARKS: dict[str, tuple[Benchmark, str]] = {
    "call_soon": (bench_call_soon, "callbacks/s"),
    "call_soon_args": (bench_call_soon_args, "callbacks/s"),
    "call_soon_threadsafe": (bench_call_soon_threadsafe, "callbacks/s"),
    "timers": (bench_timers, "timers/s"),
    "sleep_zero": (bench_sleep_zero, "iterations/s"),
    "echo_1kb": (bench_echo, "roundtrips/s"),
    "stream": (bench_stream, "bytes/s"),
    "getaddrinfo": (bench_getaddrinfo, "lookups/s"),
    "process_spawn": (bench_process_spawn, "processes/s"),
    "process_pipe": (bench_process_pipe, "bytes/s"),
}


def humanise(value: float, unit: str) -> str:
    if unit == "bytes/s":
        return f"{value / (1 << 20):>12,.0f} MiB/s"
    return f"{value:>15,.0f} {unit}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare zuvloop against asyncio and uvloop.")
    parser.add_argument("--repeat", type=int, default=3, help="runs per benchmark; the best is reported")
    parser.add_argument("--only", nargs="*", choices=sorted(BENCHMARKS), help="benchmarks to run")
    args = parser.parse_args()

    factories = loop_factories()
    baseline = "uvloop" if "uvloop" in factories else "asyncio"
    print(f"python {sys.version.split()[0]}   libuv {zuvloop.libuv_version()}\n")

    for name in args.only or list(BENCHMARKS):
        benchmark, unit = BENCHMARKS[name]
        print(name)
        # Interleave the rounds. Running every sample for one loop back to back
        # lets thermal drift and background load bias whichever went first.
        samples: dict[str, list[float]] = {label: [] for label in factories}
        for _ in range(args.repeat):
            for label, factory in factories.items():
                samples[label].append(run_on(factory, benchmark))

        results = {label: max(values) for label, values in samples.items()}
        for label, value in results.items():
            spread = statistics.pstdev(samples[label]) / value if len(samples[label]) > 1 else 0.0
            print(f"  {label:<8}{humanise(value, unit)}  (+/- {spread:.1%})")
        print(f"  {'':<8}{'zuvloop / ' + baseline:>15}  {results['zuvloop'] / results[baseline]:.2f}x\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
