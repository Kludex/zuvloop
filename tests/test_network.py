from __future__ import annotations

import asyncio
import gc
import os
import socket
import ssl
import struct
import tempfile
from collections.abc import Sequence
from pathlib import Path

import pytest

import zuvloop
from conftest import running_loop
from zuvloop._server import Server

pytestmark = pytest.mark.anyio

# What `getaddrinfo` hands back: family, kind, protocol, canonical name, address.
type AddrInfo = tuple[int, int, int, str, tuple[str, int] | tuple[str, int, int, int]]


class Echo(asyncio.Protocol):
    def __init__(self) -> None:
        self.transport: asyncio.Transport | None = None
        self.received = bytearray()
        self.closed: asyncio.Future[BaseException | None] | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]
        self.closed = running_loop().create_future()

    def data_received(self, data: bytes) -> None:
        self.received += data
        assert self.transport is not None
        self.transport.write(data)

    def connection_lost(self, exc: BaseException | None) -> None:
        assert self.closed is not None
        self.closed.set_result(exc)


class Sink(asyncio.Protocol):
    """Server protocol that only reads, so it never writes on the shared loop."""

    def __init__(self) -> None:
        self.received = bytearray()

    def data_received(self, data: bytes) -> None:
        self.received += data


class Collector(asyncio.Protocol):
    """Client protocol that resolves once the peer closes."""

    def __init__(self) -> None:
        self.transport: asyncio.Transport | None = None
        self.received = bytearray()
        self.done: asyncio.Future[bytes] | None = None
        self.eof = False

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]
        self.done = running_loop().create_future()

    def data_received(self, data: bytes) -> None:
        self.received += data

    def eof_received(self) -> bool:
        self.eof = True
        return False

    def connection_lost(self, exc: BaseException | None) -> None:
        assert self.done is not None
        self.done.set_result(bytes(self.received))


async def start_echo(backlog: int = 100) -> tuple[zuvloop.Server, int, list[Echo]]:
    protocols: list[Echo] = []

    def factory() -> Echo:
        protocol = Echo()
        protocols.append(protocol)
        return protocol

    loop = running_loop()
    server = await loop.create_server(factory, "127.0.0.1", 0, backlog=backlog)
    return server, server.sockets[0].getsockname()[1], protocols


async def test_server_sockets_are_protected_views() -> None:
    loop = running_loop()
    server = await loop.create_server(Echo, "127.0.0.1", 0, start_serving=False)
    exposed = server.sockets[0]

    assert not isinstance(exposed, socket.socket)
    assert not hasattr(exposed, "close")
    assert not hasattr(exposed, "detach")
    assert exposed.fileno() >= 0

    server.close()
    await server.wait_closed()
    assert exposed.fileno() == -1


async def test_server_close_tolerates_an_externally_closed_owned_socket() -> None:
    loop = running_loop()
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    server = await loop.create_server(Echo, sock=sock, start_serving=False)

    sock.close()
    server.close()
    await server.wait_closed()


async def test_streams_round_trip() -> None:
    server, port, _ = await start_echo()
    async with server:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"ping")
        await writer.drain()
        assert await reader.readexactly(4) == b"ping"
        writer.close()
        await writer.wait_closed()


async def test_large_payload_survives_partial_writes() -> None:
    server, port, _ = await start_echo()
    payload = os.urandom(1 << 20)
    async with server:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(payload)
        await writer.drain()
        assert await reader.readexactly(len(payload)) == payload
        writer.close()
        await writer.wait_closed()


async def test_writelines_uses_scatter_gather() -> None:
    server, port, _ = await start_echo()
    chunks = [b"a" * 10, b"b" * 10, b"c" * 10]
    async with server:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.writelines(chunks)
        await writer.drain()
        assert await reader.readexactly(30) == b"".join(chunks)
        writer.close()
        await writer.wait_closed()


async def test_writelines_with_many_chunks() -> None:
    server, port, _ = await start_echo()
    chunks = [bytes([index % 251]) for index in range(64)]
    async with server:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.writelines(chunks)
        await writer.drain()
        assert await reader.readexactly(64) == b"".join(chunks)
        writer.close()
        await writer.wait_closed()


async def test_empty_writes_are_ignored() -> None:
    server, port, _ = await start_echo()
    async with server:
        _reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"")
        writer.writelines([])
        await writer.drain()
        writer.close()
        await writer.wait_closed()


async def test_transport_exposes_addresses() -> None:
    server, port, _ = await start_echo()
    loop = running_loop()
    async with server:
        transport, protocol = await loop.create_connection(Collector, "127.0.0.1", port)
        assert transport.get_extra_info("peername")[1] == port
        assert transport.get_extra_info("sockname")[0] == "127.0.0.1"
        assert transport.get_extra_info("family") == socket.AF_INET
        assert transport.get_extra_info("missing", "fallback") == "fallback"
        assert transport.get_extra_info("missing") is None
        assert transport.can_write_eof() is True
        transport.close()
        assert protocol.done is not None
        await protocol.done


async def test_transport_is_an_asyncio_transport() -> None:
    server, port, _ = await start_echo()
    loop = running_loop()
    async with server:
        transport, protocol = await loop.create_connection(Collector, "127.0.0.1", port)
        assert isinstance(transport, asyncio.Transport)
        assert isinstance(transport, asyncio.ReadTransport)
        assert isinstance(transport, asyncio.WriteTransport)
        assert not hasattr(transport, "_extra")
        transport.close()
        assert protocol.done is not None
        await protocol.done


async def test_consecutive_writes_are_sent_together() -> None:
    server, port, protocols = await start_echo()
    loop = running_loop()
    async with server:
        transport, protocol = await loop.create_connection(Collector, "127.0.0.1", port)

        transport.write(b"head ")
        transport.write(b"body")
        # Both are accounted for while they wait, and go out as one write.
        assert transport.get_write_buffer_size() == 9

        await asyncio.sleep(0.05)
        assert transport.get_write_buffer_size() == 0
        assert bytes(protocols[0].received) == b"head body"

        transport.close()
        assert protocol.done is not None
        await protocol.done


async def test_a_write_batch_larger_than_the_buffer_still_arrives_in_order() -> None:
    server, port, protocols = await start_echo()
    loop = running_loop()
    chunks = [bytes([index % 251]) * 8 for index in range(10)]
    async with server:
        transport, protocol = await loop.create_connection(Collector, "127.0.0.1", port)
        for chunk in chunks:
            transport.write(chunk)

        await asyncio.sleep(0.05)
        assert bytes(protocols[0].received) == b"".join(chunks)

        transport.close()
        assert protocol.done is not None
        await protocol.done


async def test_unsent_writes_count_towards_the_high_water_mark() -> None:
    server, port, _ = await start_echo()
    loop = running_loop()
    async with server:
        transport, protocol = await loop.create_connection(Collector, "127.0.0.1", port)
        transport.set_write_buffer_limits(high=4, low=2)

        transport.write(b"over the mark")
        assert transport.get_write_buffer_size() == 13

        await asyncio.sleep(0.05)
        assert transport.get_write_buffer_size() == 0

        transport.close()
        assert protocol.done is not None
        await protocol.done


async def test_abort_drops_writes_that_have_not_been_sent() -> None:
    server, port, protocols = await start_echo()
    loop = running_loop()
    async with server:
        transport, protocol = await loop.create_connection(Collector, "127.0.0.1", port)
        transport.write(b"never sent")
        transport.abort()

        assert protocol.done is not None
        await protocol.done
        await asyncio.sleep(0.05)
        assert bytes(protocols[0].received) == b""


async def test_transport_reports_closing_state() -> None:
    server, port, _ = await start_echo()
    loop = running_loop()
    async with server:
        transport, protocol = await loop.create_connection(Collector, "127.0.0.1", port)
        assert transport.is_closing() is False
        transport.close()
        assert transport.is_closing() is True
        transport.close()
        assert protocol.done is not None
        await protocol.done


async def test_abort_drops_the_connection() -> None:
    server, port, _ = await start_echo()
    loop = running_loop()
    async with server:
        transport, protocol = await loop.create_connection(Collector, "127.0.0.1", port)
        transport.abort()
        assert protocol.done is not None
        await protocol.done


async def test_connection_lost_receives_the_socket_error() -> None:
    loop = running_loop()
    listener = socket.socket()
    listener.setblocking(False)
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    accepted = loop.create_task(loop.sock_accept(listener))

    class LostError(asyncio.Protocol):
        def __init__(self) -> None:
            self.done = loop.create_future()

        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            transport.write(b"make the peer reset unread data")  # type: ignore[attr-defined]

        def connection_lost(self, exc: BaseException | None) -> None:
            self.done.set_result(exc)

    try:
        transport, protocol = await loop.create_connection(LostError, *listener.getsockname())
        peer, _addr = await accepted
        peer.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        peer.close()

        exc = await asyncio.wait_for(protocol.done, 2)

        assert isinstance(exc, ConnectionResetError)
        assert transport.is_closing()
    finally:
        listener.close()


async def test_pause_and_resume_reading() -> None:
    server, port, _ = await start_echo()
    loop = running_loop()
    async with server:
        transport, protocol = await loop.create_connection(Collector, "127.0.0.1", port)
        assert transport.is_reading() is True
        transport.pause_reading()
        assert transport.is_reading() is False
        transport.write(b"hello")
        await asyncio.sleep(0.05)
        assert protocol.received == b""
        transport.resume_reading()
        assert transport.is_reading() is True
        await asyncio.sleep(0.05)
        assert protocol.received == b"hello"
        transport.close()
        assert protocol.done is not None
        await protocol.done


async def test_write_eof_is_seen_as_eof_by_the_peer() -> None:
    loop = running_loop()
    seen: list[bool] = []

    class EofWatcher(asyncio.Protocol):
        def __init__(self) -> None:
            self.done = loop.create_future()

        def eof_received(self) -> bool:
            seen.append(True)
            return False

        def connection_lost(self, exc: BaseException | None) -> None:
            self.done.set_result(None)

    server = await loop.create_server(EofWatcher, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        transport, protocol = await loop.create_connection(Collector, "127.0.0.1", port)
        transport.write_eof()
        transport.write_eof()
        await asyncio.sleep(0.1)
        transport.close()
        assert protocol.done is not None
        await protocol.done
    assert seen == [True]


async def test_write_eof_waits_for_buffered_writes() -> None:
    loop = running_loop()
    left, right = socket.socketpair()
    left.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4096)
    right.setblocking(False)
    payload = b"x" * (4 << 20)

    transport, protocol = await loop.connect_accepted_socket(Collector, left)
    try:
        transport.write(payload)
        assert transport.get_write_buffer_size() > 0
        transport.write_eof()

        received = bytearray()
        while chunk := await loop.sock_recv(right, 65536):
            received += chunk
        assert received == payload
    finally:
        transport.close()
        right.close()

    assert protocol.done is not None
    await protocol.done


async def test_writes_after_close_are_rejected() -> None:
    server, port, _ = await start_echo()
    loop = running_loop()
    async with server:
        transport, protocol = await loop.create_connection(Collector, "127.0.0.1", port)
        transport.close()
        with pytest.raises(RuntimeError, match="after write_eof"):
            transport.write(b"too late")
        assert protocol.done is not None
        await protocol.done


async def test_flow_control_pauses_the_protocol() -> None:
    loop = running_loop()
    server, port, protocols = await start_echo()
    async with server:
        transport, client = await loop.create_connection(Collector, "127.0.0.1", port)
        transport.set_write_buffer_limits(high=1024, low=256)
        assert transport.get_write_buffer_limits() == (256, 1024)
        transport.pause_reading()
        for _ in range(64):
            transport.write(b"x" * 65536)
        assert transport.get_write_buffer_size() > 0
        transport.abort()
        assert client.done is not None
        await client.done


async def test_a_drain_returns_when_the_high_water_mark_is_zero() -> None:
    """anyio writes this way: no buffer allowed at all, then wait for the drain.

    A write the socket accepts outright must not report itself as backed up, or
    the pause never lifts - there is no completion callback coming to lift it.
    """
    server, port, _ = await start_echo()
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    try:
        writer.transport.set_write_buffer_limits(0)
        writer.write(b"drain me")
        await asyncio.wait_for(writer.drain(), 2)
        assert await reader.readexactly(8) == b"drain me"
    finally:
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()


async def test_a_write_from_pause_writing_is_still_sent() -> None:
    """Flushing runs protocol code, which may write again while the flush is in
    progress. Those writes have to be picked up rather than left behind."""
    loop = running_loop()
    sinks: list[Sink] = []

    def sink_factory() -> Sink:
        sink = Sink()
        sinks.append(sink)
        return sink

    # The peer only reads. A peer that answered would write on this same loop,
    # and any write revives the flush list - which would hide the very thing
    # this test is here to catch.
    server = await loop.create_server(sink_factory, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    class WritesWhenPaused(Collector):
        def __init__(self) -> None:
            super().__init__()
            self.paused = False

        def pause_writing(self) -> None:
            self.paused = True
            assert self.transport is not None
            self.transport.write(b"written while paused")

    async def until_marker_arrives() -> None:
        # It lands mid-stream, since the write that filled the buffer is still
        # going out behind it.
        while b"written while paused" not in sinks[0].received:
            await asyncio.sleep(0.01)

    transport, client = await loop.create_connection(WritesWhenPaused, "127.0.0.1", port)
    try:
        transport.set_write_buffer_limits(high=1024, low=256)

        # One write, so the only thing that sends it is the end-of-turn flush.
        # The socket cannot take it all, which pauses the protocol from inside
        # that flush - and the write it makes then has nothing left to send it
        # unless the flush notices the batch refilled.
        transport.write(b"x" * (4 << 20))
        await asyncio.sleep(0)
        assert client.paused

        await asyncio.wait_for(until_marker_arrives(), 5)
    finally:
        transport.abort()
        assert client.done is not None
        await client.done
        server.close()
        await server.wait_closed()


async def test_pause_writing_cannot_overfill_a_reentrant_batch() -> None:
    """A full batch flush calls Python before the outer write claims its slot."""
    loop = running_loop()
    sinks: list[Sink] = []

    def sink_factory() -> Sink:
        sink = Sink()
        sinks.append(sink)
        return sink

    server = await loop.create_server(sink_factory, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]

    class RefillsBatch(Collector):
        def pause_writing(self) -> None:
            assert self.transport is not None
            for _ in range(4):
                self.transport.write(b"r")

    async def until_markers_arrive() -> None:
        while b"rrrr" not in sinks[0].received:
            await asyncio.sleep(0.01)

    transport, client = await loop.create_connection(RefillsBatch, "127.0.0.1", port)
    try:
        transport.set_write_buffer_limits(high=1024, low=256)
        for _ in range(4):
            transport.write(b"x" * (2 << 20))

        # The fifth write synchronously flushes the four-slot batch. Its pause
        # callback refills every slot before this outer call resumes.
        transport.write(b"fifth")
        assert transport.get_write_buffer_size() <= (8 << 20) + 9
        await asyncio.wait_for(until_markers_arrive(), 5)
    finally:
        transport.abort()
        assert client.done is not None
        await client.done
        server.close()
        await server.wait_closed()


async def test_write_buffer_limits_validate_their_arguments() -> None:
    server, port, _ = await start_echo()
    loop = running_loop()
    async with server:
        transport, protocol = await loop.create_connection(Collector, "127.0.0.1", port)
        transport.set_write_buffer_limits()
        assert transport.get_write_buffer_limits() == (16384, 65536)
        transport.set_write_buffer_limits(high=8192)
        assert transport.get_write_buffer_limits() == (2048, 8192)
        with pytest.raises(ValueError, match="high water mark"):
            transport.set_write_buffer_limits(high=10, low=100)
        with pytest.raises(ValueError, match="high water mark must be non-negative"):
            transport.set_write_buffer_limits(high=-1)
        with pytest.raises(ValueError, match="low water mark must be non-negative"):
            transport.set_write_buffer_limits(low=-1)
        with pytest.raises(TypeError, match="unexpected keyword"):
            transport.set_write_buffer_limits(medium=1)  # type: ignore[call-arg]
        with pytest.raises(TypeError, match="at most 2"):
            transport.set_write_buffer_limits(1, 2, 3)  # type: ignore[call-arg]
        transport.close()
        assert protocol.done is not None
        await protocol.done


async def test_protocol_can_be_replaced() -> None:
    server, port, _ = await start_echo()
    loop = running_loop()
    async with server:
        transport, protocol = await loop.create_connection(Collector, "127.0.0.1", port)
        assert transport.get_protocol() is protocol
        replacement = asyncio.Protocol()
        transport.set_protocol(replacement)
        assert transport.get_protocol() is replacement
        transport.close()
        await asyncio.sleep(0.05)


async def test_buffered_protocol_reads_into_its_own_buffer() -> None:
    loop = running_loop()
    done = loop.create_future()

    class Buffered(asyncio.BufferedProtocol):
        def __init__(self) -> None:
            self.buffer = bytearray(4096)
            self.received = bytearray()

        def get_buffer(self, sizehint: int) -> bytearray:
            return self.buffer

        def buffer_updated(self, nbytes: int) -> None:
            self.received += self.buffer[:nbytes]
            done.set_result(bytes(self.received))

    server, port, _ = await start_echo()
    async with server:
        transport, _protocol = await loop.create_connection(Buffered, "127.0.0.1", port)
        transport.write(b"hello")
        assert await done == b"hello"
        transport.close()
        await asyncio.sleep(0.05)


async def test_connection_refused_is_reported(closed_port: int) -> None:
    loop = running_loop()
    with pytest.raises(ConnectionRefusedError):
        await loop.create_connection(Collector, "127.0.0.1", closed_port)


async def test_create_connection_rejects_conflicting_arguments() -> None:
    loop = running_loop()
    with socket.socket() as sock:
        with pytest.raises(ValueError, match="at the same time"):
            await loop.create_connection(Collector, "127.0.0.1", 80, sock=sock)
    with pytest.raises(ValueError, match="no sock specified"):
        await loop.create_connection(Collector)
    with pytest.raises(ValueError, match="only meaningful with ssl"):
        await loop.create_connection(Collector, "127.0.0.1", 80, server_hostname="x")


async def test_create_connection_from_an_existing_socket() -> None:
    server, port, _ = await start_echo()
    loop = running_loop()
    async with server:
        sock = socket.socket()
        sock.setblocking(False)
        await loop.sock_connect(sock, ("127.0.0.1", port))
        transport, protocol = await loop.create_connection(Collector, sock=sock)
        transport.write(b"via sock")
        await asyncio.sleep(0.05)
        assert protocol.received == b"via sock"
        transport.close()
        assert protocol.done is not None
        await protocol.done


async def test_create_connection_rejects_a_datagram_socket() -> None:
    loop = running_loop()
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        with pytest.raises(ValueError, match="socket was expected"):
            await loop.create_connection(Collector, sock=sock)


async def test_create_connection_binds_a_local_address() -> None:
    server, port, _ = await start_echo()
    loop = running_loop()
    async with server:
        transport, protocol = await loop.create_connection(Collector, "127.0.0.1", port, local_addr=("127.0.0.1", 0))
        assert transport.get_extra_info("sockname")[0] == "127.0.0.1"
        transport.close()
        assert protocol.done is not None
        await protocol.done


async def test_create_connection_reports_every_address_failing(closed_port: int) -> None:
    loop = running_loop()
    with pytest.raises(OSError):
        await loop.create_connection(Collector, "localhost", closed_port)


async def test_create_server_rejects_conflicting_arguments() -> None:
    loop = running_loop()
    with socket.socket() as sock:
        with pytest.raises(ValueError, match="at the same time"):
            await loop.create_server(Echo, "127.0.0.1", 0, sock=sock)
    with pytest.raises(ValueError, match="Neither host/port nor sock"):
        await loop.create_server(Echo)


async def test_create_server_from_an_existing_socket() -> None:
    loop = running_loop()
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    server = await loop.create_server(Echo, sock=sock)
    async with server:
        assert server.is_serving() is True
        assert server.get_loop() is loop
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"sock server")
        assert await reader.readexactly(11) == b"sock server"
        writer.close()
        await writer.wait_closed()


async def test_server_can_start_serving_later() -> None:
    loop = running_loop()
    server = await loop.create_server(Echo, "127.0.0.1", 0, start_serving=False)
    port = server.sockets[0].getsockname()[1]
    assert server.is_serving() is False
    async with server:
        await server.start_serving()
        await server.start_serving()
        assert server.is_serving() is True
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"late")
        assert await reader.readexactly(4) == b"late"
        writer.close()
        await writer.wait_closed()


async def test_server_binds_several_hosts() -> None:
    loop = running_loop()
    server = await loop.create_server(Echo, ["127.0.0.1", "::1"], 0, reuse_address=True)
    async with server:
        assert len(server.sockets) >= 1


async def test_server_reuse_port() -> None:
    loop = running_loop()
    server = await loop.create_server(Echo, "127.0.0.1", 0, reuse_port=True)
    async with server:
        assert len(server.sockets) == 1


async def test_server_close_is_idempotent() -> None:
    server, _port, _ = await start_echo()
    server.close()
    server.close()
    await server.wait_closed()
    await server.wait_closed()
    assert server.sockets == ()


async def test_cancelled_wait_closed_does_not_block_the_others() -> None:
    server, _port, _ = await start_echo()
    cancelled = asyncio.ensure_future(server.wait_closed())
    waiting = asyncio.ensure_future(server.wait_closed())
    await asyncio.sleep(0)

    # The cancellation only resolves the future; `close()` reaches `_wakeup()`
    # before the task resumes to unregister itself.
    cancelled.cancel()
    server.close()

    await waiting
    with pytest.raises(asyncio.CancelledError):
        await cancelled


async def test_cancelled_server_creation_closes_listeners(monkeypatch: pytest.MonkeyPatch) -> None:
    loop = running_loop()
    started = loop.create_future()

    async def pause_during_start(server: Server) -> None:
        server._start_serving()
        started.set_result(None)
        await loop.create_future()

    monkeypatch.setattr(Server, "start_serving", pause_during_start)
    watchers = loop._metrics()["watchers"]
    creation = loop.create_task(loop.create_server(Echo, "127.0.0.1", 0))
    await started
    assert loop._metrics()["watchers"] > watchers

    creation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await creation

    assert loop._metrics()["watchers"] == watchers


async def test_server_wait_closed_waits_for_clients() -> None:
    server, port, _ = await start_echo()
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"open")
    assert await reader.readexactly(4) == b"open"
    server.close()
    closed = running_loop().create_task(server.wait_closed())
    await asyncio.sleep(0.02)
    assert not closed.done()
    writer.close()
    await writer.wait_closed()
    await asyncio.wait_for(closed, 2)


@pytest.mark.parametrize("method", ["close_clients", "abort_clients"])
async def test_server_can_close_clients_without_closing_listener(method: str) -> None:
    server, port, _ = await start_echo()
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        getattr(server, method)()
        assert await asyncio.wait_for(reader.read(), 2) == b""
        assert server.is_serving()

        second_reader, second_writer = await asyncio.open_connection("127.0.0.1", port)
        second_writer.write(b"still serving")
        assert await second_reader.readexactly(13) == b"still serving"
        second_writer.close()
        await second_writer.wait_closed()
        writer.close()
        await writer.wait_closed()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.parametrize("tls", [False, True])
async def test_server_factory_failure_does_not_prevent_wait_closed(tls: bool, server_context: ssl.SSLContext) -> None:
    loop = running_loop()
    errors: list[BaseException | None] = []
    loop.set_exception_handler(lambda _loop, context: errors.append(context.get("exception")))

    def broken_factory() -> asyncio.Protocol:
        raise RuntimeError("broken protocol factory")

    server = await loop.create_server(
        broken_factory,
        "127.0.0.1",
        0,
        ssl=server_context if tls else None,
    )
    client = socket.socket()
    client.setblocking(False)
    try:
        await loop.sock_connect(client, server.sockets[0].getsockname())
        await asyncio.sleep(0.05)
    finally:
        client.close()
        server.close()

    await asyncio.wait_for(server.wait_closed(), 2)
    assert errors
    assert isinstance(errors[0], RuntimeError)


async def test_failure_after_transport_adoption_detaches_server_once(monkeypatch: pytest.MonkeyPatch) -> None:
    loop = running_loop()
    errors: list[BaseException | None] = []
    loop.set_exception_handler(lambda _loop, context: errors.append(context.get("exception")))

    def fail_to_track(_server: Server, _transport: asyncio.Transport) -> None:
        raise RuntimeError("cannot track transport")

    monkeypatch.setattr(Server, "_attach", fail_to_track)
    server = await loop.create_server(Echo, "127.0.0.1", 0)
    client = socket.socket()
    client.setblocking(False)
    try:
        await loop.sock_connect(client, server.sockets[0].getsockname())
        await asyncio.sleep(0.05)
    finally:
        client.close()
        server.close()

    await asyncio.wait_for(server.wait_closed(), 2)
    assert server._active == 0
    assert any(isinstance(error, RuntimeError) for error in errors)


async def test_serve_forever_runs_until_cancelled() -> None:
    server, port, _ = await start_echo()
    loop = running_loop()
    task = loop.create_task(server.serve_forever())
    await asyncio.sleep(0.02)
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"serving")
    assert await reader.readexactly(7) == b"serving"
    writer.close()
    await writer.wait_closed()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_connect_accepted_socket() -> None:
    loop = running_loop()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.setblocking(False)
    port = listener.getsockname()[1]

    client = socket.socket()
    client.setblocking(False)
    connecting = loop.create_task(loop.sock_connect(client, ("127.0.0.1", port)))
    conn, _addr = await loop.sock_accept(listener)
    await connecting

    transport, protocol = await loop.connect_accepted_socket(Collector, conn)
    await loop.sock_sendall(client, b"accepted")
    await asyncio.sleep(0.05)
    assert protocol.received == b"accepted"
    transport.close()
    client.close()
    listener.close()


async def test_unix_sockets_round_trip() -> None:
    loop = running_loop()
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "zuvloop.sock"
        server = await loop.create_unix_server(Echo, path)
        async with server:
            reader, writer = await asyncio.open_unix_connection(str(path))
            writer.write(b"unix")
            assert await reader.readexactly(4) == b"unix"
            writer.close()
            await writer.wait_closed()
        assert not path.exists()


async def test_unix_server_cleanup_preserves_a_replacement_path() -> None:
    loop = running_loop()
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "zuvloop.sock"
        server = await loop.create_unix_server(Echo, path)
        path.unlink()
        path.write_text("replacement")

        server.close()
        await server.wait_closed()

        assert path.read_text() == "replacement"


async def test_unix_server_cleanup_tolerates_a_missing_path() -> None:
    loop = running_loop()
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "zuvloop.sock"
        server = await loop.create_unix_server(Echo, path)
        path.unlink()

        server.close()
        await server.wait_closed()


async def test_unix_server_cleanup_tolerates_a_path_unlinked_while_binding() -> None:
    loop = running_loop()
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "zuvloop.sock"

        def vanishing_stat(
            target: int | str | bytes | os.PathLike[str], *, dir_fd: int | None = None, follow_symlinks: bool = True
        ) -> os.stat_result:
            raise FileNotFoundError(target)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(os, "stat", vanishing_stat)
            server = await loop.create_unix_server(Echo, path)

        server.close()
        await server.wait_closed()
        assert path.exists()
        path.unlink()


async def test_unix_server_closes_its_listeners_when_serving_fails() -> None:
    loop = running_loop()
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "zuvloop.sock"

        async def refuse(self: Server) -> None:
            raise RuntimeError("refused")

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(Server, "start_serving", refuse)
            with pytest.raises(RuntimeError, match="refused"):
                await loop.create_unix_server(Echo, path)

        assert not path.exists()


async def test_unix_server_closes_its_listener_when_the_cleanup_stat_fails() -> None:
    loop = running_loop()
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "zuvloop.sock"

        def refuse_stat(
            target: int | str | bytes | os.PathLike[str], *, dir_fd: int | None = None, follow_symlinks: bool = True
        ) -> os.stat_result:
            raise BlockingIOError("stat refused")

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(os, "stat", refuse_stat)
            with pytest.raises(BlockingIOError, match="stat refused"):
                await loop.create_unix_server(Echo, path)

        # The socket is closed rather than left for the garbage collector to
        # notice, which would report it against whatever runs next.
        with pytest.raises(ConnectionRefusedError):
            await asyncio.open_unix_connection(str(path))


async def test_unix_server_rejects_an_unusable_path_before_opening_a_socket() -> None:
    loop = running_loop()
    with pytest.raises(TypeError):
        await loop.create_unix_server(Echo, object())  # type: ignore[arg-type]
    gc.collect()


async def test_unix_server_rejects_conflicting_arguments() -> None:
    loop = running_loop()
    with socket.socket(socket.AF_UNIX) as sock:
        with pytest.raises(ValueError, match="at the same time"):
            await loop.create_unix_server(Echo, "/tmp/x", sock=sock)
    with pytest.raises(ValueError, match="no sock specified"):
        await loop.create_unix_server(Echo)


async def test_unix_server_from_an_existing_socket() -> None:
    loop = running_loop()
    with tempfile.TemporaryDirectory() as directory:
        path = str(Path(directory) / "explicit.sock")
        sock = socket.socket(socket.AF_UNIX)
        sock.bind(path)
        server = await loop.create_unix_server(Echo, sock=sock)
        async with server:
            reader, writer = await asyncio.open_unix_connection(path)
            writer.write(b"explicit")
            assert await reader.readexactly(8) == b"explicit"
            writer.close()
            await writer.wait_closed()


async def test_unix_server_rejects_a_bound_path_in_use() -> None:
    loop = running_loop()
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "taken.sock"
        path.touch()
        with pytest.raises(OSError):
            await loop.create_unix_server(Echo, path)


async def test_unix_connection_rejects_conflicting_arguments() -> None:
    loop = running_loop()
    with socket.socket(socket.AF_UNIX) as sock:
        with pytest.raises(ValueError, match="at the same time"):
            await loop.create_unix_connection(Collector, "/tmp/x", sock=sock)
    with pytest.raises(ValueError, match="no path and sock"):
        await loop.create_unix_connection(Collector)
    with pytest.raises(ValueError, match="only meaningful with ssl"):
        await loop.create_unix_connection(Collector, "/tmp/x", server_hostname="host")


async def test_unix_connection_to_a_missing_path_fails() -> None:
    loop = running_loop()
    with tempfile.TemporaryDirectory() as directory:
        with pytest.raises(FileNotFoundError):
            await loop.create_unix_connection(Collector, Path(directory) / "absent.sock")


async def test_unix_connection_from_an_existing_socket() -> None:
    loop = running_loop()
    with tempfile.TemporaryDirectory() as directory:
        path = str(Path(directory) / "pair.sock")
        server = await loop.create_unix_server(Echo, path)
        async with server:
            sock = socket.socket(socket.AF_UNIX)
            sock.setblocking(False)
            await loop.sock_connect(sock, path)
            transport, protocol = await loop.create_unix_connection(Collector, sock=sock)
            transport.write(b"paired")
            await asyncio.sleep(0.05)
            assert protocol.received == b"paired"
            transport.close()
            assert protocol.done is not None
            await protocol.done


async def test_unix_connection_rejects_a_tcp_socket() -> None:
    loop = running_loop()
    with socket.socket() as sock:
        with pytest.raises(ValueError, match="socket was expected"):
            await loop.create_unix_connection(Collector, sock=sock)


async def test_server_without_address_reuse() -> None:
    loop = running_loop()
    server = await loop.create_server(Echo, "127.0.0.1", 0, reuse_address=False)
    async with server:
        assert len(server.sockets) == 1


async def test_binding_a_busy_port_releases_every_socket() -> None:
    loop = running_loop()
    taken = await loop.create_server(Echo, "127.0.0.1", 0, reuse_address=False)
    port = taken.sockets[0].getsockname()[1]
    async with taken:
        with pytest.raises(OSError):
            await loop.create_server(Echo, "127.0.0.1", port, reuse_address=False)


async def test_unix_server_can_start_serving_later() -> None:
    loop = running_loop()
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "later.sock"
        server = await loop.create_unix_server(Echo, path, start_serving=False)
        async with server:
            assert server.is_serving() is False
            await server.start_serving()
            reader, writer = await asyncio.open_unix_connection(str(path))
            writer.write(b"later")
            assert await reader.readexactly(5) == b"later"
            writer.close()
            await writer.wait_closed()


async def test_unix_server_reports_an_unusable_path() -> None:
    loop = running_loop()
    with pytest.raises(OSError):
        await loop.create_unix_server(Echo, "/nonexistent-directory/zuvloop.sock")


async def test_a_backlog_of_one_accepts_one_connection_per_wakeup() -> None:
    server, port, _ = await start_echo(backlog=1)
    async with server:
        writers = []
        for _ in range(3):
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(b"n")
            assert await reader.readexactly(1) == b"n"
            writers.append(writer)
        for writer in writers:
            writer.close()
            await writer.wait_closed()


async def test_wait_closed_tolerates_a_cancelled_waiter() -> None:
    loop = running_loop()
    server, port, _ = await start_echo()
    reader, writer = await asyncio.open_connection("127.0.0.1", port)
    writer.write(b"x")
    assert await reader.readexactly(1) == b"x"
    server.close()
    waiting = loop.create_task(server.wait_closed())
    await asyncio.sleep(0.02)
    waiting.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting
    assert server._waiters == []
    writer.close()
    await writer.wait_closed()
    await server.wait_closed()


async def test_tls_client_needs_a_server_hostname() -> None:
    loop = running_loop()
    sock = socket.socket()
    sock.setblocking(False)
    try:
        with pytest.raises(ValueError, match="must set server_hostname"):
            await loop.create_connection(Collector, sock=sock, ssl=True)
    finally:
        sock.close()


async def test_a_transport_can_wrap_an_unconnected_socket() -> None:
    loop = running_loop()
    sock = socket.socket()
    sock.setblocking(False)
    transport, _protocol = await loop.connect_accepted_socket(Collector, sock)
    assert transport.get_extra_info("peername") is None
    transport.close()
    await asyncio.sleep(0.02)


async def test_a_client_handshake_against_a_plain_server_fails(client_context: ssl.SSLContext) -> None:
    loop = running_loop()

    class NotTls(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            transport.write(b"definitely not a ServerHello")  # type: ignore[attr-defined]
            transport.close()

    server = await loop.create_server(NotTls, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        with pytest.raises(OSError):
            await loop.create_connection(Collector, "127.0.0.1", port, ssl=client_context, server_hostname="localhost")


async def test_tls_defaults_the_server_hostname_to_the_host(
    server_context: ssl.SSLContext, client_context: ssl.SSLContext
) -> None:
    loop = running_loop()
    server = await loop.create_server(Echo, "127.0.0.1", 0, ssl=server_context)
    port = server.sockets[0].getsockname()[1]
    async with server:
        transport, _protocol = await loop.create_connection(Collector, "localhost", port, ssl=client_context)
        transport.close()
        await asyncio.sleep(0.05)


async def test_transports_are_weak_referenceable() -> None:
    import weakref

    server, port, _ = await start_echo()
    loop = asyncio.get_running_loop()
    async with server:
        transport, protocol = await loop.create_connection(Collector, "127.0.0.1", port)
        assert weakref.ref(transport)() is transport
        transport.close()
        assert protocol.done is not None
        await protocol.done


async def test_contextvars_reach_protocol_callbacks() -> None:
    """asyncio delivers reads in the context captured when the transport was made."""
    import contextvars

    marker: contextvars.ContextVar[str] = contextvars.ContextVar("marker")
    marker.set("set-before-the-connection")
    loop = running_loop()
    seen: dict[str, str] = {}

    class Watcher(asyncio.Protocol):
        def __init__(self) -> None:
            self.done = loop.create_future()

        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            seen["connection_made"] = marker.get("MISSING")

        def data_received(self, data: bytes) -> None:
            seen["data_received"] = marker.get("MISSING")
            self.done.set_result(None)

        def connection_lost(self, exc: BaseException | None) -> None:
            seen["connection_lost"] = marker.get("MISSING")

    server, port, _ = await start_echo()
    async with server:
        transport, protocol = await loop.create_connection(Watcher, "127.0.0.1", port)
        transport.write(b"ping")
        await protocol.done
        transport.close()
        await asyncio.sleep(0.05)

    assert seen == {
        "connection_made": "set-before-the-connection",
        "data_received": "set-before-the-connection",
        "connection_lost": "set-before-the-connection",
    }


async def test_a_repeated_host_binds_one_socket() -> None:
    """Naming a host twice is not a request to bind it twice."""
    loop = running_loop()
    server = await loop.create_server(Echo, ["127.0.0.1", "127.0.0.1"], 0, start_serving=False)
    try:
        assert len(server.sockets) == 1
    finally:
        server.close()
        await server.wait_closed()


async def test_an_empty_host_binds_every_interface() -> None:
    """`host=""` means the null host, not a host literally named ""."""
    loop = running_loop()
    server = await loop.create_server(Echo, "", 0, start_serving=False)
    try:
        assert server.sockets
    finally:
        server.close()
        await server.wait_closed()


async def test_a_refused_local_addr_names_the_address() -> None:
    """asyncio puts the address in the message; the bare errno text does not."""
    loop = running_loop()
    taken = socket.socket()
    taken.bind(("127.0.0.1", 0))
    taken.listen(1)
    local = taken.getsockname()
    try:
        with pytest.raises(OSError) as caught:
            await loop.create_connection(Echo, *local, local_addr=local)
        assert repr(local) in str(caught.value)
    finally:
        taken.close()


async def test_an_ssl_handshake_timeout_without_ssl_is_rejected() -> None:
    loop = running_loop()
    left, right = socket.socketpair()
    try:
        with pytest.raises(ValueError, match="ssl_handshake_timeout is only meaningful with ssl"):
            await loop.connect_accepted_socket(Echo, left, ssl_handshake_timeout=5.0)
    finally:
        left.close()
        right.close()


async def test_an_ssl_shutdown_timeout_without_ssl_is_rejected() -> None:
    loop = running_loop()
    left, right = socket.socketpair()
    try:
        with pytest.raises(ValueError, match="ssl_shutdown_timeout is only meaningful with ssl"):
            await loop.connect_accepted_socket(Echo, left, ssl_shutdown_timeout=5.0)
    finally:
        left.close()
        right.close()


async def test_the_asyncio_server_hook_unregisters_and_closes() -> None:
    """`_stop_serving` is what `asyncio.base_events.Server.close` reaches for."""
    loop = running_loop()
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    sock.setblocking(False)
    loop.add_reader(sock.fileno(), lambda: None)

    loop._stop_serving(sock)

    assert sock.fileno() == -1


async def test_happy_eyeballs_beats_a_black_holed_address() -> None:
    """Without racing, an address on a route that drops costs a full timeout.

    The route that drops is arranged here rather than borrowed from the host:
    whether `192.0.2.1` answers, refuses or vanishes is a property of the
    machine's routing table, and a refusal would let a sequential connect pass.
    """
    loop = running_loop()
    server, port, _ = await start_echo()
    black_hole = ("192.0.2.1", port)
    resolved = [
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", black_hole),
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port)),
    ]

    original_getaddrinfo = loop.getaddrinfo
    original_sock_connect = loop.sock_connect

    async def getaddrinfo(
        host: str | bytes | None, port: str | bytes | int | None, **kwargs: int
    ) -> Sequence[AddrInfo]:
        # Only the made-up name is answered from the list. `sock_connect`
        # resolves the address it is handed, and answering that from the list
        # too would send every attempt to the same place.
        if host == "split":
            return resolved
        return await original_getaddrinfo(host, port, **kwargs)

    async def sock_connect(sock: socket.socket, address: tuple[str, int]) -> None:
        if address == black_hole:
            await asyncio.Event().wait()
        await original_sock_connect(sock, address)

    async with server:
        loop.getaddrinfo = getaddrinfo  # type: ignore[method-assign]
        loop.sock_connect = sock_connect  # type: ignore[method-assign]
        try:
            transport, _protocol = await asyncio.wait_for(
                loop.create_connection(Collector, "split", port, happy_eyeballs_delay=0.1), 5
            )
            assert transport.get_extra_info("peername")[0] == "127.0.0.1"
            transport.close()
        finally:
            loop.getaddrinfo = original_getaddrinfo  # type: ignore[method-assign]
            loop.sock_connect = original_sock_connect  # type: ignore[method-assign]


async def test_all_errors_reports_every_failure() -> None:
    loop = running_loop()
    with pytest.raises(ExceptionGroup) as caught:
        await loop.create_connection(Collector, "127.0.0.1", 1, all_errors=True)
    assert caught.value.exceptions
    assert all(isinstance(exc, OSError) for exc in caught.value.exceptions)


async def test_interleave_is_accepted_without_a_delay() -> None:
    server, port, _ = await start_echo()
    loop = running_loop()
    async with server:
        transport, _protocol = await loop.create_connection(Collector, "127.0.0.1", port, interleave=1)
        transport.close()


async def test_differing_failures_are_reported_together() -> None:
    """One complaint stands in for many identical ones; different ones do not."""
    loop = running_loop()
    original = loop.getaddrinfo

    async def getaddrinfo(
        host: str | bytes | None, port: str | bytes | int | None, **kwargs: int
    ) -> Sequence[AddrInfo]:
        if host == "two-ways-to-fail":
            # Both refuse at once, and the address is in the message, so the two
            # complaints differ - which is the case that cannot be folded.
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 1)),
                (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::1", 1, 0, 0)),
            ]
        return await original(host, port, **kwargs)

    loop.getaddrinfo = getaddrinfo  # type: ignore[method-assign]
    try:
        with pytest.raises(OSError, match="Multiple exceptions"):
            await loop.create_connection(Collector, "two-ways-to-fail", 1)
    finally:
        loop.getaddrinfo = original  # type: ignore[method-assign]


async def test_identical_failures_are_reported_once() -> None:
    """Two attempts that fail the same way are one complaint, not a list of two."""
    loop = running_loop()
    original_getaddrinfo = loop.getaddrinfo
    original_sock_connect = loop.sock_connect
    attempts = 0

    async def getaddrinfo(
        host: str | bytes | None, port: str | bytes | int | None, **kwargs: int
    ) -> Sequence[AddrInfo]:
        if host == "twice-the-same":
            info = (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 1))
            return [info, info]
        return await original_getaddrinfo(host, port, **kwargs)

    async def sock_connect(sock: socket.socket, address: tuple[str, int]) -> None:
        nonlocal attempts
        attempts += 1
        await original_sock_connect(sock, address)

    loop.getaddrinfo = getaddrinfo  # type: ignore[method-assign]
    loop.sock_connect = sock_connect  # type: ignore[method-assign]
    try:
        with pytest.raises(ConnectionRefusedError) as caught:
            await loop.create_connection(Collector, "twice-the-same", 1)
        # One complaint is the right answer only if both attempts really ran.
        assert attempts == 2
        assert "Multiple exceptions" not in str(caught.value)
    finally:
        loop.getaddrinfo = original_getaddrinfo  # type: ignore[method-assign]
        loop.sock_connect = original_sock_connect  # type: ignore[method-assign]


async def test_a_socket_that_cannot_be_created_is_still_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    """Creating the socket fails for real - EMFILE, a family the kernel refuses.

    An attempt that failed without recording anything leaves nothing to raise at
    the end, which surfaced as an `IndexError` rather than the actual problem.
    """
    loop = running_loop()
    real = socket.socket

    def refuse(*args: int, **kwargs: int) -> socket.socket:
        raise OSError(24, "Too many open files")

    monkeypatch.setattr(socket, "socket", refuse)
    with pytest.raises(OSError, match="Too many open files"):
        await loop.create_connection(Collector, "127.0.0.1", 1)

    monkeypatch.setattr(socket, "socket", real)
    assert socket.socket is real


async def test_every_failure_is_reported_when_a_socket_cannot_be_created(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`all_errors` needs something to group; an empty group is a ValueError."""
    loop = running_loop()

    def refuse(*args: int, **kwargs: int) -> socket.socket:
        raise OSError(24, "Too many open files")

    monkeypatch.setattr(socket, "socket", refuse)
    with pytest.raises(ExceptionGroup) as caught:
        await loop.create_connection(Collector, "127.0.0.1", 1, all_errors=True)
    assert [str(exc) for exc in caught.value.exceptions] == ["[Errno 24] Too many open files"]


@pytest.mark.parametrize("keep_alive", [None, False, True])
async def test_create_server_honours_keep_alive(keep_alive: bool | None) -> None:
    """asyncio sets SO_KEEPALIVE on each bound socket; this used to be a TypeError."""
    loop = running_loop()
    server = await loop.create_server(Echo, "127.0.0.1", 0, keep_alive=keep_alive)
    try:
        raw = socket.socket(fileno=os.dup(server.sockets[0].fileno()))
        with raw:
            enabled = raw.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE)
        assert bool(enabled) is bool(keep_alive)
    finally:
        server.close()
        await server.wait_closed()


async def test_binding_a_unix_path_twice_names_the_path() -> None:
    """The check read Linux's EADDRINUSE, so on any other platform it never fired."""
    loop = running_loop()
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "taken.sock"
        server = await loop.create_unix_server(Echo, path)
        async with server:
            with pytest.raises(OSError, match="already in use") as caught:
                await loop.create_unix_server(Echo, path)
            assert str(path) in str(caught.value)
