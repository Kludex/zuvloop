"""Measure aiohttp's server and client under each event loop.

Only one side changes loop at a time; the other stays on stock asyncio so the
number reflects the side under test. Rounds are interleaved across loops.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import socket
import statistics
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from typing import Any

from aiohttp import ClientSession, web

import zuv

Factory = Callable[[], asyncio.AbstractEventLoop]

BODY = b"Hello, World!"


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


async def handle(_request: web.Request) -> web.Response:
    return web.Response(body=BODY, content_type="text/plain")


class ServerThread:
    """Runs an aiohttp server on a specific loop, in its own thread."""

    def __init__(self, factory: Factory, port: int) -> None:
        self._factory = factory
        self._port = port
        self._ready = threading.Event()
        self._stop: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self.loop_name = ""
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        async def main() -> None:
            self._loop = asyncio.get_running_loop()
            self.loop_name = type(self._loop).__module__.split(".")[0]
            self._stop = asyncio.Event()
            app = web.Application()
            app.router.add_get("/", handle)
            runner = web.AppRunner(app, access_log=None)
            await runner.setup()
            site = web.TCPSite(runner, "127.0.0.1", self._port)
            await site.start()
            self._ready.set()
            await self._stop.wait()
            await runner.cleanup()

        asyncio.run(main(), loop_factory=self._factory)

    def __enter__(self) -> ServerThread:
        self._thread.start()
        self._ready.wait(timeout=30)
        return self

    def __exit__(self, *exc: object) -> None:
        if self._loop is not None and self._stop is not None:
            self._loop.call_soon_threadsafe(self._stop.set)
        self._thread.join(timeout=15)


def measure_with_oha(port: int, duration: str, connections: int) -> float:
    result = subprocess.run(
        ["oha", "--no-tui", "--output-format", "json", "-z", duration, "-c", str(connections),
         f"http://127.0.0.1:{port}/"],
        capture_output=True,
        text=True,
        check=True,
    )
    return float(json.loads(result.stdout)["summary"]["requestsPerSec"])


async def client_load(port: int, requests: int, concurrency: int) -> float:
    """Drive a fixed server from aiohttp's client, on whichever loop is running."""
    async with ClientSession() as session:
        url = f"http://127.0.0.1:{port}/"

        async def worker(count: int) -> None:
            for _ in range(count):
                async with session.get(url) as response:
                    await response.read()

        share = requests // concurrency
        started = time.perf_counter()
        await asyncio.gather(*(worker(share) for _ in range(concurrency)))
        return (share * concurrency) / (time.perf_counter() - started)


def loop_factories() -> dict[str, Factory]:
    factories: dict[str, Factory] = {"asyncio": asyncio.new_event_loop, "zuv": zuv.new_event_loop}
    try:
        import uvloop
    except ImportError:
        return factories
    factories["uvloop"] = uvloop.new_event_loop
    return factories


def report(title: str, samples: dict[str, list[float]], unit: str) -> None:
    print(title)
    best = {name: max(values) for name, values in samples.items()}
    for name, value in best.items():
        spread = statistics.pstdev(samples[name]) / value if len(samples[name]) > 1 else 0.0
        print(f"  {name:<8}{value:>12,.0f} {unit}  (+/- {spread:.1%})")
    if "uvloop" in best:
        print(f"  {'':<8}{'zuv / uvloop':>12}  {best['zuv'] / best['uvloop']:.2f}x")
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark aiohttp across event loops.")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--duration", default="4s")
    parser.add_argument("--connections", type=int, default=64)
    parser.add_argument("--client-requests", type=int, default=20_000)
    args = parser.parse_args()

    factories = loop_factories()
    server_samples: dict[str, list[float]] = {name: [] for name in factories}
    client_samples: dict[str, list[float]] = {name: [] for name in factories}

    print(f"python {sys.version.split()[0]}  libuv {zuv.libuv_version()}  oha -c {args.connections} -z {args.duration}\n")

    for index in range(args.rounds):
        # Server under test, external load generator.
        for name, factory in factories.items():
            port = free_port()
            with ServerThread(factory, port):
                measure_with_oha(port, "1s", args.connections)
                server_samples[name].append(measure_with_oha(port, args.duration, args.connections))

        # Client under test, server pinned to stock asyncio for every run.
        port = free_port()
        with ServerThread(asyncio.new_event_loop, port):
            for name, factory in factories.items():
                asyncio.run(client_load(port, 2_000, 16), loop_factory=factory)  # warmup
                rate = asyncio.run(client_load(port, args.client_requests, 16), loop_factory=factory)
                client_samples[name].append(rate)
        print(f"round {index + 1}/{args.rounds} done", flush=True)

    print()
    report("aiohttp server", server_samples, "req/s")
    report("aiohttp client", client_samples, "req/s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
