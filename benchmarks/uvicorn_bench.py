"""Serve a fixed ASGI app under each event loop and measure it with `oha`.

Only the loop factory changes between runs. Rounds are interleaved rather than
grouped per loop, so thermal drift and background load cannot quietly favour
whichever went first.
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

import uvicorn

import zuv

Factory = Callable[[], asyncio.AbstractEventLoop]

BODY_SIZES = {"plaintext": 13, "10kb": 10 * 1024}


def build_app(size: int) -> Any:
    body = b"x" * size if size != 13 else b"Hello, World!"
    headers = [(b"content-type", b"text/plain"), (b"content-length", str(len(body)).encode())]

    async def app(scope: Any, receive: Any, send: Any) -> None:
        await send({"type": "http.response.start", "status": 200, "headers": headers})
        await send({"type": "http.response.body", "body": body})

    return app


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


class ServerThread:
    """Runs uvicorn on a specific loop, in a thread, and reports the loop it used."""

    def __init__(self, factory: Factory, app: Any, port: int) -> None:
        self._factory = factory
        self._server = uvicorn.Server(
            uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error", access_log=False)
        )
        self.loop_name = ""
        self._thread = threading.Thread(target=self._run, daemon=True)

    def _run(self) -> None:
        async def main() -> None:
            self.loop_name = type(asyncio.get_running_loop()).__module__.split(".")[0]
            await self._server.serve()

        asyncio.run(main(), loop_factory=self._factory)

    def __enter__(self) -> ServerThread:
        self._thread.start()
        while not self._server.started:
            time.sleep(0.01)
        return self

    def __exit__(self, *exc: object) -> None:
        self._server.should_exit = True
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
    factories: dict[str, Factory] = {"asyncio": asyncio.new_event_loop, "zuv": zuv.new_event_loop}
    try:
        import uvloop
    except ImportError:
        return factories
    factories["uvloop"] = uvloop.new_event_loop
    return factories


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark uvicorn across event loops.")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--duration", default="5s")
    parser.add_argument("--connections", type=int, default=64)
    args = parser.parse_args()

    factories = loop_factories()
    samples: dict[str, dict[str, list[float]]] = {name: {k: [] for k in factories} for name in BODY_SIZES}

    print(
        f"python {sys.version.split()[0]}  libuv {zuv.libuv_version()}  oha -c {args.connections} -z {args.duration}\n"
    )
    for round_index in range(args.rounds):
        for body_name, size in BODY_SIZES.items():
            app = build_app(size)
            for loop_name, factory in factories.items():
                port = free_port()
                with ServerThread(factory, app, port) as server:
                    if server.loop_name.split(".")[0] not in (loop_name, "asyncio", "uvloop"):
                        raise SystemExit(f"expected {loop_name}, server ran on {server.loop_name}")
                    measure(port, "1s", args.connections)  # warmup, discarded
                    samples[body_name][loop_name].append(measure(port, args.duration, args.connections))
        print(f"round {round_index + 1}/{args.rounds} done", flush=True)

    print()
    for body_name in BODY_SIZES:
        print(body_name)
        best = {name: max(values) for name, values in samples[body_name].items()}
        for name, value in best.items():
            spread = statistics.pstdev(samples[body_name][name]) / value if len(samples[body_name][name]) > 1 else 0.0
            print(f"  {name:<8}{value:>12,.0f} req/s  (+/- {spread:.1%})")
        if "uvloop" in best:
            print(f"  {'':<8}{'zuv / uvloop':>12}  {best['zuv'] / best['uvloop']:.2f}x")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
