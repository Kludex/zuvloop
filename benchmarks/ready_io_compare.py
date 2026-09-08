"""Run with `uv run --group bench python -m benchmarks.ready_io_compare --loop zuvloop`."""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import socket
import statistics
import sys
import time
from collections.abc import Callable

import zuvloop
from benchmarks.ready_io import idle_connections, ready_chain, socket_readiness


def main() -> None:
    factories: dict[str, Callable[[], asyncio.AbstractEventLoop]] = {
        "asyncio": asyncio.new_event_loop,
        "zuvloop": zuvloop.new_event_loop,
    }
    try:
        import uvloop
    except ImportError:
        pass
    else:
        factories["uvloop"] = uvloop.new_event_loop
    parser = argparse.ArgumentParser(description="Report throughput and socket-read delay as JSON lines.")
    parser.add_argument("--loop", choices=factories, default="zuvloop")
    parser.add_argument("--rounds", type=int, default=9)
    args = parser.parse_args()
    if args.rounds < 1:
        parser.error("rounds must be positive")
    sys.stdout.write(json.dumps({"python": sys.version, "platform": platform.platform(), "loop": args.loop}) + "\n")

    loop = factories[args.loop]()
    try:
        for connections in (0, 1, 250):
            with idle_connections(loop, connections):
                for repetition in range(args.rounds + 2):
                    started = time.perf_counter_ns()
                    loop.run_until_complete(ready_chain())
                    elapsed = time.perf_counter_ns() - started
                    if repetition >= 2:
                        sys.stdout.write(
                            json.dumps(
                                {
                                    "case": "ready_chain",
                                    "connections": connections,
                                    "round": repetition - 2,
                                    "iterations_per_second": 10_000 * 1e9 / elapsed,
                                }
                            )
                            + "\n"
                        )
        if sys.platform == "win32" and args.loop == "asyncio":
            sys.stdout.write(
                json.dumps({"case": "socket_readiness", "skipped": "ProactorEventLoop does not support add_reader"})
                + "\n"
            )
            return
        reader, writer = socket.socketpair()
        with reader, writer:
            reader.setblocking(False)
            writer.setblocking(False)
            for callback_ns in (0, 100_000):
                for repetition in range(args.rounds + 2):
                    with socket_readiness(loop, reader, writer, callback_ns) as measure:
                        sample = measure()
                    if repetition >= 2:
                        sys.stdout.write(
                            json.dumps(
                                {
                                    "case": "socket_readiness",
                                    "callback_ns": callback_ns,
                                    "round": repetition - 2,
                                    "median_us": statistics.median(sample.latency_ns) / 1000,
                                    "p95_us": sorted(sample.latency_ns)[189] / 1000,
                                    "median_callbacks_before_read": statistics.median(sample.callbacks_before_read),
                                }
                            )
                            + "\n"
                        )
    finally:
        loop.close()


if __name__ == "__main__":
    main()
