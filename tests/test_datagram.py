from __future__ import annotations

import asyncio
import socket
import tempfile
from pathlib import Path
from typing import cast

import pytest

from conftest import running_loop
from zuvloop import _connect, _zuvloop

pytestmark = pytest.mark.anyio

Address = tuple[str, int] | str


class Echo(asyncio.DatagramProtocol):
    """Server endpoint that answers every datagram to its sender."""

    def __init__(self) -> None:
        self.transport: asyncio.DatagramTransport | None = None
        self.received: list[tuple[bytes, Address]] = []

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]

    def datagram_received(self, data: bytes, addr: Address) -> None:
        self.received.append((data, addr))
        assert self.transport is not None
        self.transport.sendto(b"re:" + data, addr)


class Collector(asyncio.DatagramProtocol):
    """Client endpoint that resolves once a datagram arrives."""

    def __init__(self) -> None:
        self.transport: asyncio.DatagramTransport | None = None
        self.received: list[tuple[bytes, Address]] = []
        self.errors: list[BaseException] = []
        self.done: asyncio.Future[bytes] | None = None
        self.lost: asyncio.Future[BaseException | None] | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = transport  # type: ignore[assignment]
        loop = running_loop()
        self.done = loop.create_future()
        self.lost = loop.create_future()

    def datagram_received(self, data: bytes, addr: Address) -> None:
        self.received.append((data, addr))
        assert self.done is not None
        if not self.done.done():
            self.done.set_result(data)

    def error_received(self, exc: BaseException) -> None:
        self.errors.append(exc)

    def connection_lost(self, exc: BaseException | None) -> None:
        # A protocol installed by set_protocol() never saw connection_made.
        if self.lost is not None and not self.lost.done():
            self.lost.set_result(exc)


async def start_echo() -> tuple[asyncio.DatagramTransport, Echo, Address]:
    transport, protocol = await running_loop().create_datagram_endpoint(Echo, local_addr=("127.0.0.1", 0))
    return transport, protocol, transport.get_extra_info("sockname")


async def test_datagram_round_trip() -> None:
    server, _echo, address = await start_echo()
    client, protocol = await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    try:
        client.sendto(b"hello", address)
        assert protocol.done is not None
        assert await asyncio.wait_for(protocol.done, 2) == b"re:hello"
        assert protocol.received[0][1][1] == address[1]
    finally:
        client.close()
        server.close()


async def test_a_connected_endpoint_needs_no_address() -> None:
    server, _echo, address = await start_echo()
    client, protocol = await running_loop().create_datagram_endpoint(Collector, remote_addr=address)
    try:
        client.sendto(b"hello")
        assert protocol.done is not None
        assert await asyncio.wait_for(protocol.done, 2) == b"re:hello"
        assert client.get_extra_info("peername")[1] == address[1]
    finally:
        client.close()
        server.close()


async def test_the_transport_is_an_asyncio_datagram_transport() -> None:
    server, _echo, _address = await start_echo()
    try:
        assert isinstance(server, asyncio.DatagramTransport)
        assert isinstance(server, asyncio.BaseTransport)
        assert not hasattr(server, "_extra")
        assert server.get_extra_info("family") == socket.AF_INET
        assert server.get_extra_info("missing", "fallback") == "fallback"
        assert server.get_extra_info("missing") is None
    finally:
        server.close()


async def test_an_address_is_rejected_on_a_connected_endpoint() -> None:
    server, _echo, address = await start_echo()
    client, _protocol = await running_loop().create_datagram_endpoint(Collector, remote_addr=address)
    try:
        with pytest.raises(ValueError, match="connected"):
            client.sendto(b"hello", address)
    finally:
        client.close()
        server.close()


async def test_an_address_is_required_on_an_unconnected_endpoint() -> None:
    transport, _protocol = await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    try:
        with pytest.raises(ValueError, match="requires an address"):
            transport.sendto(b"hello")
    finally:
        transport.close()


async def test_close_reports_connection_lost() -> None:
    transport, protocol = await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    assert transport.is_closing() is False
    transport.close()
    assert transport.is_closing() is True
    transport.close()
    assert protocol.lost is not None
    assert await asyncio.wait_for(protocol.lost, 2) is None


async def test_abort_closes_immediately() -> None:
    transport, protocol = await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    transport.abort()
    assert protocol.lost is not None
    await asyncio.wait_for(protocol.lost, 2)


async def test_sending_after_close_is_rejected() -> None:
    server, _echo, address = await start_echo()
    transport, _protocol = await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    transport.close()
    try:
        with pytest.raises(RuntimeError, match="after close"):
            transport.sendto(b"hello", address)
    finally:
        server.close()


async def test_the_protocol_can_be_replaced() -> None:
    transport, protocol = await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    try:
        assert transport.get_protocol() is protocol
        replacement = Collector()
        transport.set_protocol(replacement)
        assert transport.get_protocol() is replacement
    finally:
        transport.close()


async def test_write_buffer_limits_are_reported() -> None:
    raw, _protocol = await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    transport = cast("_zuvloop.DatagramTransport", raw)
    try:
        assert transport.get_write_buffer_size() == 0
        transport.set_write_buffer_limits(high=4096, low=1024)
        assert transport.get_write_buffer_limits() == (1024, 4096)
        transport.set_write_buffer_limits()
        assert transport.get_write_buffer_limits() == (16384, 65536)
        transport.set_write_buffer_limits(low=256)
        assert transport.get_write_buffer_limits() == (256, 1024)
        with pytest.raises(ValueError, match="high water mark"):
            transport.set_write_buffer_limits(high=1, low=2)
        with pytest.raises(TypeError, match="unexpected keyword"):
            transport.set_write_buffer_limits(bogus=1)  # type: ignore[call-arg]
        with pytest.raises(TypeError, match="at most 2"):
            transport.set_write_buffer_limits(1, 2, 3)  # type: ignore[call-arg]
    finally:
        transport.close()


async def test_an_existing_socket_can_be_adopted() -> None:
    server, _echo, address = await start_echo()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    client, protocol = await running_loop().create_datagram_endpoint(Collector, sock=sock)
    try:
        client.sendto(b"adopted", address)
        assert protocol.done is not None
        assert await asyncio.wait_for(protocol.done, 2) == b"re:adopted"
    finally:
        client.close()
        server.close()


async def test_socket_modifiers_are_rejected_alongside_a_socket() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    with sock:
        with pytest.raises(ValueError, match="can not be used when sock is specified"):
            await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0), sock=sock)


async def test_a_stream_socket_is_rejected() -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with sock:
        with pytest.raises(ValueError):
            await running_loop().create_datagram_endpoint(Collector, sock=sock)


async def test_an_endpoint_without_any_address_needs_a_family() -> None:
    with pytest.raises(ValueError, match="unexpected address family"):
        await running_loop().create_datagram_endpoint(Collector)


async def test_an_unbound_endpoint_can_be_created_from_a_family() -> None:
    server, _echo, address = await start_echo()
    client, protocol = await running_loop().create_datagram_endpoint(Collector, family=socket.AF_INET)
    try:
        client.sendto(b"unbound", address)
        assert protocol.done is not None
        assert await asyncio.wait_for(protocol.done, 2) == b"re:unbound"
    finally:
        client.close()
        server.close()


async def test_broadcast_and_reuse_port_are_applied() -> None:
    transport, _protocol = await running_loop().create_datagram_endpoint(
        Collector, local_addr=("127.0.0.1", 0), allow_broadcast=True, reuse_port=True
    )
    try:
        sock = transport.get_extra_info("socket")
        assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST) != 0
        assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT) != 0
    finally:
        transport.close()


async def test_an_address_must_be_a_host_port_pair() -> None:
    with pytest.raises(TypeError, match="host, port"):
        await running_loop().create_datagram_endpoint(Collector, local_addr="not-a-pair")


async def test_a_send_failure_reaches_error_received() -> None:
    loop = running_loop()
    transport, protocol = await loop.create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    try:
        # Larger than any datagram a UDP socket can carry, so the send fails
        # rather than the delivery - which is what `error_received` is for.
        transport.sendto(b"x" * 100_000, ("127.0.0.1", 9))
        assert protocol.errors
        assert isinstance(protocol.errors[0], OSError)
    finally:
        transport.close()


async def test_ipv6_endpoints_round_trip() -> None:
    loop = running_loop()
    try:
        transport, _echo = await loop.create_datagram_endpoint(Echo, local_addr=("::1", 0))
    except OSError:  # pragma: no cover - host without IPv6 loopback
        pytest.skip("no IPv6 loopback")
    try:
        address = transport.get_extra_info("sockname")
        client, protocol = await loop.create_datagram_endpoint(Collector, local_addr=("::1", 0))
        try:
            client.sendto(b"v6", address)
            assert protocol.done is not None
            assert await asyncio.wait_for(protocol.done, 2) == b"re:v6"
        finally:
            client.close()
    finally:
        transport.close()


async def test_a_cancelled_setup_closes_the_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    # With the waiter never resolved, the setup stays parked exactly where a
    # cancellation has to unwind it.
    monkeypatch.setattr(_connect, "_set_result_unless_done", lambda future: None)
    protocols: list[Collector] = []

    def factory() -> Collector:
        protocol = Collector()
        protocols.append(protocol)
        return protocol

    loop = running_loop()
    task = asyncio.ensure_future(loop.create_datagram_endpoint(factory, local_addr=("127.0.0.1", 0)))
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert protocols[0].lost is not None
    await asyncio.wait_for(protocols[0].lost, 2)


async def test_a_socket_that_cannot_be_bound_is_closed() -> None:
    with pytest.raises(OSError):
        # TEST-NET-1: routable as an address, never assigned to an interface.
        await running_loop().create_datagram_endpoint(Collector, local_addr=("192.0.2.1", 0))


async def test_a_transport_that_fails_to_adopt_releases_the_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*args: object, **kwargs: object) -> _zuvloop.DatagramTransport:
        raise RuntimeError("refused")

    monkeypatch.setattr(type(running_loop()), "_make_datagram_transport", refuse)
    with pytest.raises(RuntimeError, match="refused"):
        await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))


async def test_an_empty_resolution_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    async def nothing(*args: object, **kwargs: object) -> list[tuple[int, int, int, str, tuple[str, int]]]:
        return []

    monkeypatch.setattr(type(running_loop()), "getaddrinfo", nothing)
    with pytest.raises(OSError, match="empty list"):
        await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))


async def test_reuse_port_is_rejected_where_it_does_not_exist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delattr(socket, "SO_REUSEPORT", raising=False)
    with pytest.raises(ValueError, match="reuse_port"):
        await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0), reuse_port=True)


async def test_unix_datagram_endpoints_round_trip() -> None:
    loop = running_loop()
    # pytest's tmp_path overruns the length a unix socket path may have.
    directory = tempfile.mkdtemp()
    server_path = str(Path(directory) / "s")
    client_path = str(Path(directory) / "c")
    server, _echo = await loop.create_datagram_endpoint(Echo, local_addr=server_path, family=socket.AF_UNIX)
    client, protocol = await loop.create_datagram_endpoint(Collector, local_addr=client_path, family=socket.AF_UNIX)
    try:
        client.sendto(b"unix", server_path)
        assert protocol.done is not None
        assert await asyncio.wait_for(protocol.done, 2) == b"re:unix"
    finally:
        client.close()
        server.close()


async def test_several_datagrams_arrive_in_turn() -> None:
    server, _echo, address = await start_echo()
    client, protocol = await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    try:
        client.sendto(b"one", address)
        client.sendto(b"two", address)

        async def until_both_arrive() -> None:
            while len(protocol.received) < 2:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(until_both_arrive(), 2)
        assert [data for data, _addr in protocol.received] == [b"re:one", b"re:two"]
    finally:
        client.close()
        server.close()


async def test_resolving_a_cancelled_waiter_is_a_no_op() -> None:
    waiter: asyncio.Future[None] = running_loop().create_future()
    waiter.cancel()
    _connect._set_result_unless_done(waiter)
    assert waiter.cancelled()
