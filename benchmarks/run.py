"""Absolute throughput per loop - the numbers behind the README table.

    uv run --group bench python benchmarks/run.py
    uv run --group bench python benchmarks/run.py --only call_soon timer_rounds

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
import math
import os
import socket
import statistics
import sys
import threading
import time
from collections.abc import Callable, Coroutine
from dataclasses import dataclass

import zuvloop

Factory = Callable[[], asyncio.AbstractEventLoop]
AsyncBenchmark = Callable[[], Coroutine[None, None, float]]
StoppedLoopBenchmark = Callable[[asyncio.AbstractEventLoop], float]


@dataclass(frozen=True)
class Benchmark:
    unit: str
    async_work: AsyncBenchmark | None = None
    stopped_loop_work: StoppedLoopBenchmark | None = None


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
        if benchmark.stopped_loop_work is not None:
            return benchmark.stopped_loop_work(loop)
        if benchmark.async_work is None:
            raise RuntimeError("benchmark has no workload")
        return loop.run_until_complete(benchmark.async_work())
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


async def bench_timer_schedule_cancel(iterations: int = 200_000) -> float:
    """Timer churn: schedule then cancel, never firing."""
    loop = asyncio.get_running_loop()
    started = time.perf_counter()
    for _ in range(iterations):
        loop.call_later(30, _noop).cancel()
    return iterations / (time.perf_counter() - started)


async def bench_timer_rounds(iterations: int = 10_000) -> float:
    """Run one zero-delay timer per loop turn until the chain completes."""
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    remaining = iterations

    def step() -> None:
        nonlocal remaining
        remaining -= 1
        if remaining == 0:
            done.set_result(None)
        else:
            loop.call_later(0, step)

    started = time.perf_counter()
    loop.call_later(0, step)
    await done
    return iterations / (time.perf_counter() - started)


def bench_timer_due_batch(loop: asyncio.AbstractEventLoop, iterations: int = 100_000) -> float:
    """Drain a prebuilt batch of due timers without timing allocation or deallocation."""
    done = loop.create_future()
    deadline = loop.time() + 0.5
    handles = [loop.call_at(deadline, _noop) for _ in range(iterations)]
    sentinel_deadline = deadline + 0.01
    handles.append(loop.call_at(sentinel_deadline, done.set_result, None))
    if not all(math.isfinite(handle.when()) for handle in handles):
        raise RuntimeError("timer returned a non-finite deadline")
    time.sleep(max(0.0, sentinel_deadline - loop.time()) + 0.001)
    started = time.perf_counter()
    loop.run_until_complete(done)
    elapsed = time.perf_counter() - started
    handles.clear()
    return iterations / elapsed


async def bench_sleep_zero(iterations: int = 30_000) -> float:
    """Loop iterations per second: one callback per turn, poll included."""
    started = time.perf_counter()
    for _ in range(iterations):
        await asyncio.sleep(0)
    return iterations / (time.perf_counter() - started)


async def bench_tasks(total: int = 50_000, batch_size: int = 5_000) -> float:
    """Task throughput in bounded batches, with one suspension per task."""

    async def tiny_task() -> None:
        await asyncio.sleep(0)

    if total < 0 or batch_size <= 0:
        raise ValueError("total must be non-negative and batch_size must be positive")

    started = time.perf_counter()
    completed = 0
    while completed < total:
        current_batch = min(batch_size, total - completed)
        await asyncio.gather(*(tiny_task() for _ in range(current_batch)))
        completed += current_batch
    return completed / (time.perf_counter() - started)


async def bench_ready_with_io(connection_count: int = 250, iterations: int = 20_000) -> float:
    """Ready-chain throughput while idle stream handles keep I/O active."""
    loop = asyncio.get_running_loop()
    accepted: list[asyncio.BaseTransport] = []

    class Hold(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            accepted.append(transport)

    server = await loop.create_server(Hold, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    pairs = await asyncio.gather(
        *(loop.create_connection(asyncio.Protocol, "127.0.0.1", port) for _ in range(connection_count))
    )
    clients = [transport for transport, _protocol in pairs]
    while len(accepted) < connection_count:
        await asyncio.sleep(0)

    started = time.perf_counter()
    for _ in range(iterations):
        await asyncio.sleep(0)
    elapsed = time.perf_counter() - started

    for transport in clients + accepted:
        transport.close()
    server.close()
    await server.wait_closed()
    for _ in range(3):
        await asyncio.sleep(0)
    return iterations / elapsed


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


BENCHMARKS: dict[str, Benchmark] = {
    "call_soon": Benchmark("callbacks/s", async_work=bench_call_soon),
    "call_soon_args": Benchmark("callbacks/s", async_work=bench_call_soon_args),
    "call_soon_threadsafe": Benchmark("callbacks/s", async_work=bench_call_soon_threadsafe),
    "timer_schedule_cancel": Benchmark("timers/s", async_work=bench_timer_schedule_cancel),
    "timer_rounds": Benchmark("timers/s", async_work=bench_timer_rounds),
    "timer_due_batch": Benchmark("timers/s", stopped_loop_work=bench_timer_due_batch),
    "sleep_zero": Benchmark("iterations/s", async_work=bench_sleep_zero),
    "tasks": Benchmark("tasks/s", async_work=bench_tasks),
    "ready_with_io": Benchmark("iterations/s", async_work=bench_ready_with_io),
    "echo_1kb": Benchmark("roundtrips/s", async_work=bench_echo),
    "stream": Benchmark("bytes/s", async_work=bench_stream),
    "getaddrinfo": Benchmark("lookups/s", async_work=bench_getaddrinfo),
    "process_spawn": Benchmark("processes/s", async_work=bench_process_spawn),
    "process_pipe": Benchmark("bytes/s", async_work=bench_process_pipe),
}


def humanise(value: float, unit: str) -> str:
    if unit == "bytes/s":
        return f"{value / (1 << 20):>12,.0f} MiB/s"
    return f"{value:>15,.0f} {unit}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare zuvloop against asyncio and uvloop.")
    parser.add_argument("--repeat", type=int, default=3, help="runs per benchmark; the median is reported")
    parser.add_argument("--only", nargs="*", choices=sorted(BENCHMARKS), help="benchmarks to run")
    args = parser.parse_args()
    if args.repeat < 1:
        parser.error("--repeat must be at least 1")

    factories = loop_factories()
    baseline = "uvloop" if "uvloop" in factories else "asyncio"
    print(f"python {sys.version.split()[0]}   libuv {zuvloop.libuv_version()}\n")

    for name in args.only or list(BENCHMARKS):
        benchmark = BENCHMARKS[name]
        print(name)
        # Interleave the rounds. Running every sample for one loop back to back
        # lets thermal drift and background load bias whichever went first.
        samples: dict[str, list[float]] = {label: [] for label in factories}
        for _ in range(args.repeat):
            for label, factory in factories.items():
                samples[label].append(run_on(factory, benchmark))

        results = {label: statistics.median(values) for label, values in samples.items()}
        for label, value in results.items():
            values = samples[label]
            spread = statistics.pstdev(values) / statistics.mean(values) if len(values) > 1 else 0.0
            print(f"  {label:<8}{humanise(value, benchmark.unit)}  (+/- {spread:.1%})")
        print(f"  {'':<8}{'zuvloop / ' + baseline:>15}  {results['zuvloop'] / results[baseline]:.2f}x\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
