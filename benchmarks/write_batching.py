"""Measure what a second `write()` per response costs each event loop.

ASGI and aiohttp both send a response as two writes - the header block, then the
body. This serves one fixed response both ways, with no HTTP server in between,
so the only variable is how many times `transport.write()` is called. A loop that
coalesces the two into one vectored syscall barely notices the difference; one
that issues each write separately pays for it on every response.
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
from collections.abc import Callable

import zuvloop

Factory = Callable[[], asyncio.AbstractEventLoop]

BODY = b"Hello, World!"
HEAD = b"HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: 13\r\n\r\n"


def make_protocol(split: bool) -> type[asyncio.Protocol]:
    class Responder(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport

        def data_received(self, data: bytes) -> None:
            for _ in range(data.count(b"\r\n\r\n")):
                if split:
                    self.transport.write(HEAD)  # type: ignore[attr-defined]
                    self.transport.write(BODY)  # type: ignore[attr-defined]
                else:
                    self.transport.write(HEAD + BODY)  # type: ignore[attr-defined]

    return Responder


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


class ServerThread:
    """Serves the responder on a specific loop, in a thread."""

    def __init__(self, factory: Factory, split: bool, port: int) -> None:
        self._factory = factory
        self._split = split
        self._port = port
        self._ready = threading.Event()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop: asyncio.Future[None] | None = None
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        async def main() -> None:
            loop = asyncio.get_running_loop()
            self._loop = loop
            self._stop = loop.create_future()
            server = await loop.create_server(make_protocol(self._split), "127.0.0.1", self._port)
            self._ready.set()
            await self._stop
            server.close()
            await server.wait_closed()

        asyncio.run(main(), loop_factory=self._factory)

    def __enter__(self) -> ServerThread:
        self._thread.start()
        self._ready.wait(10)
        return self

    def __exit__(self, *exc: object) -> None:
        loop, stop = self._loop, self._stop
        assert loop is not None and stop is not None

        def release() -> None:
            if not stop.done():
                stop.set_result(None)

        loop.call_soon_threadsafe(release)
        self._thread.join(timeout=10)


def measure(port: int, duration: str, connections: int) -> float:
    result = subprocess.run(
        [
            "oha",
            "--no-tui",
            "--output-format",
            "json",
            "-z",
            duration,
            "-c",
            str(connections),
            f"http://127.0.0.1:{port}/",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return float(json.loads(result.stdout)["summary"]["requestsPerSec"])


def loop_factories() -> dict[str, Factory]:
    factories: dict[str, Factory] = {"asyncio": asyncio.new_event_loop, "zuvloop": zuvloop.new_event_loop}
    try:
        import uvloop
    except ImportError:
        return factories
    factories["uvloop"] = uvloop.new_event_loop
    return factories


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure the cost of a split response write.")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--duration", default="5s")
    parser.add_argument("--connections", type=int, default=64)
    args = parser.parse_args()

    factories = loop_factories()
    variants = {"one write": False, "two writes": True}
    samples: dict[str, dict[str, list[float]]] = {v: {n: [] for n in factories} for v in variants}

    print(
        f"python {sys.version.split()[0]}  libuv {zuvloop.libuv_version()}"
        f"  oha -c {args.connections} -z {args.duration}\n"
    )
    for round_index in range(args.rounds):
        for variant, split in variants.items():
            for name, factory in factories.items():
                port = free_port()
                with ServerThread(factory, split, port):
                    measure(port, "1s", args.connections)  # warmup, discarded
                    samples[variant][name].append(measure(port, args.duration, args.connections))
        print(f"round {round_index + 1}/{args.rounds} done", flush=True)

    print()
    for variant in variants:
        print(variant)
        best = {name: max(values) for name, values in samples[variant].items()}
        for name, value in best.items():
            spread = statistics.pstdev(samples[variant][name]) / value if len(samples[variant][name]) > 1 else 0.0
            print(f"  {name:<8}{value:>12,.0f} req/s  (+/- {spread:.1%})")
        if "uvloop" in best:
            print(f"  {'':<8}{'zuvloop / uvloop':>12}  {best['zuvloop'] / best['uvloop']:.2f}x")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
