"""Exercise repeated event-loop lifecycles and fail on resource growth."""

from __future__ import annotations

import argparse
import asyncio
import gc
import socket
import sys
import threading
import tracemalloc
from pathlib import Path

import zuvloop


def open_fds() -> int | None:
    directory = Path("/proc/self/fd")
    return len(tuple(directory.iterdir())) if directory.is_dir() else None


def current_rss_bytes() -> int | None:
    status = Path("/proc/self/status")
    if not status.exists():
        return None
    for line in status.read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1]) * 1024
    return None


async def exercise_loop() -> None:
    loop = asyncio.get_running_loop()
    ready: list[int] = []
    handles = [loop.call_soon(ready.append, index) for index in range(100)]
    for handle in handles[::3]:
        handle.cancel()
    await asyncio.sleep(0)
    if len(ready) != 66:
        raise RuntimeError(f"ready queue lost callbacks: {len(ready)}")

    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            payload = await reader.readexactly(64)
            writer.write(payload)
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(echo, "127.0.0.1", 0)
    try:
        address = server.sockets[0].getsockname()
        reader, writer = await asyncio.open_connection(address[0], address[1])
        writer.write(b"x" * 64)
        await writer.drain()
        if await reader.readexactly(64) != b"x" * 64:
            raise RuntimeError("stream payload changed")
        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()

    answers = await loop.getaddrinfo(
        "127.0.0.1",
        80,
        family=socket.AF_INET,
        type=socket.SOCK_STREAM,
        flags=socket.AI_NUMERICHOST | socket.AI_NUMERICSERV,
    )
    if not answers:
        raise RuntimeError("numeric DNS lookup returned no answers")

    delivered = loop.create_future()
    thread = threading.Thread(target=loop.call_soon_threadsafe, args=(delivered.set_result, None))
    thread.start()
    await delivered
    thread.join()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cycles", type=int, default=200)
    parser.add_argument("--warmup", type=int, default=20)
    parser.add_argument("--max-rss-growth", type=int, default=32 * 1024 * 1024)
    parser.add_argument("--max-python-growth", type=int, default=4 * 1024 * 1024)
    options = parser.parse_args()
    if options.cycles <= options.warmup:
        parser.error("cycles must be greater than warmup")

    tracemalloc.start()
    baseline_fds: int | None = None
    baseline_rss: int | None = None
    baseline_python: int | None = None

    for cycle in range(options.cycles):
        zuvloop.run(exercise_loop())
        gc.collect()
        if cycle + 1 == options.warmup:
            baseline_fds = open_fds()
            baseline_rss = current_rss_bytes()
            baseline_python = tracemalloc.get_traced_memory()[0]

    final_fds = open_fds()
    final_rss = current_rss_bytes()
    final_python = tracemalloc.get_traced_memory()[0]
    print(
        f"cycles={options.cycles} fds={baseline_fds}->{final_fds} "
        f"rss={baseline_rss}->{final_rss} python={baseline_python}->{final_python}"
    )

    if baseline_fds is not None and final_fds != baseline_fds:
        print("file descriptor count changed", file=sys.stderr)
        return 1
    if baseline_rss is not None and final_rss is not None and final_rss - baseline_rss > options.max_rss_growth:
        print("resident memory exceeded the growth limit", file=sys.stderr)
        return 1
    if baseline_python is not None and final_python - baseline_python > options.max_python_growth:
        print("Python allocations exceeded the growth limit", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
