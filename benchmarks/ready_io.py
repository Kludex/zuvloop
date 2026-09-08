"""Workloads for ready-chain throughput and socket readiness under callback load."""

from __future__ import annotations

import asyncio
import socket
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass


@contextmanager
def idle_connections(loop: asyncio.AbstractEventLoop, count: int) -> Iterator[None]:
    accepted: list[asyncio.BaseTransport] = []
    clients: list[asyncio.BaseTransport] = []
    server: asyncio.AbstractServer | None = None

    class Hold(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            accepted.append(transport)

    async def setup() -> None:
        nonlocal server
        server = await loop.create_server(Hold, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        for _ in range(count):
            transport, _protocol = await loop.create_connection(asyncio.Protocol, "127.0.0.1", port)
            clients.append(transport)
        while len(accepted) < count:
            await asyncio.sleep(0)

    try:
        if count:
            loop.run_until_complete(setup())
        yield
    finally:
        for transport in clients + accepted:
            transport.close()
        if server is not None:
            server.close()
            loop.run_until_complete(server.wait_closed())
        loop.run_until_complete(asyncio.sleep(0))


async def ready_chain(iterations: int = 10_000) -> None:
    for _ in range(iterations):
        await asyncio.sleep(0)


@dataclass(frozen=True)
class ReadinessSample:
    latency_ns: list[int]
    callbacks_before_read: list[int]


def socket_readiness(
    loop: asyncio.AbstractEventLoop, reader: socket.socket, writer: socket.socket, callback_ns: int, samples: int = 200
) -> ReadinessSample:
    pending = False
    finished = False
    sent_at = 0
    sent_count = 0
    count = 0
    handle: asyncio.Handle | None = None
    result = ReadinessSample([], [])

    def readable() -> None:
        nonlocal pending, finished
        reader.recv(1)
        result.latency_ns.append(time.perf_counter_ns() - sent_at)
        result.callbacks_before_read.append(count - sent_count)
        pending = False
        if len(result.latency_ns) == samples:
            finished = True
            loop.stop()

    def step() -> None:
        nonlocal pending, sent_at, sent_count, count, handle
        if finished:
            return
        count += 1
        if not pending:
            sent_at = time.perf_counter_ns()
            sent_count = count
            writer.send(b"x")
            pending = True
        if callback_ns:
            deadline = time.perf_counter_ns() + callback_ns
            while time.perf_counter_ns() < deadline:
                pass
        handle = loop.call_soon(step)

    loop.add_reader(reader.fileno(), readable)
    handle = loop.call_soon(step)
    try:
        loop.run_forever()
        return result
    finally:
        handle.cancel()
        loop.remove_reader(reader.fileno())
        loop.run_until_complete(asyncio.sleep(0))
