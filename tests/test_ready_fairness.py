from __future__ import annotations

import asyncio
import socket
import time

import pytest

import zuvloop


@pytest.mark.timeout(10)
def test_slow_ready_chain_yields_to_a_reader_registered_by_its_callback() -> None:
    loop: asyncio.AbstractEventLoop = zuvloop.new_event_loop()
    reader, writer = socket.socketpair()
    count = 0
    observed: list[int] = []
    received: list[bytes] = []
    handle: asyncio.Handle

    def readable() -> None:
        received.append(reader.recv(1))
        observed.append(count)
        loop.stop()

    def step() -> None:
        nonlocal count, handle
        count += 1
        if count == 1:
            loop.add_reader(reader.fileno(), readable)
            writer.send(b"x")
        time.sleep(0.001)
        handle = loop.call_soon(step)

    with reader, writer:
        reader.setblocking(False)
        writer.setblocking(False)
        handle = loop.call_soon(step)
        try:
            loop.run_forever()
        finally:
            handle.cancel()
            loop.remove_reader(reader.fileno())
            loop.close()
    assert received == [b"x"]
    assert len(observed) == 1
    assert observed[0] <= 3


@pytest.mark.timeout(10)
def test_stop_finishes_the_current_ready_batch_with_io_registered() -> None:
    loop = zuvloop.new_event_loop()
    reader, writer = socket.socketpair()
    seen: list[int] = []

    def callback(index: int) -> None:
        seen.append(index)
        time.sleep(0.001)
        loop.call_soon(seen.append, -1)

    with reader, writer:
        loop.add_reader(reader.fileno(), loop.stop)
        loop.call_soon(loop.stop)
        for index in range(20):
            loop.call_soon(callback, index)
        try:
            loop.run_forever()
            assert seen == list(range(20))
        finally:
            loop.remove_reader(reader.fileno())
            loop.close()


@pytest.mark.timeout(10)
@pytest.mark.parametrize("datagram", [False, True], ids=["stream", "datagram"])
def test_transport_close_completes_while_a_callback_chain_stays_ready(datagram: bool) -> None:
    loop = zuvloop.new_event_loop()
    closed = loop.create_future()
    iterations = 0

    class Protocol(asyncio.Protocol):
        def connection_lost(self, exc: Exception | None) -> None:
            closed.set_result(None)

    class DatagramProtocol(asyncio.DatagramProtocol):
        def connection_lost(self, exc: Exception | None) -> None:
            closed.set_result(None)

    async def work() -> None:
        nonlocal iterations
        transport: asyncio.BaseTransport
        if datagram:
            transport, _ = await loop.create_datagram_endpoint(DatagramProtocol, local_addr=("127.0.0.1", 0))
        else:
            reader, writer = socket.socketpair()
            with writer:
                transport, _ = await loop.create_connection(Protocol, sock=reader)
        transport.close()
        while not closed.done():
            iterations += 1
            await asyncio.sleep(0)
        await closed

    try:
        loop.run_until_complete(work())
    finally:
        loop.close()
    assert 0 < iterations <= 3
