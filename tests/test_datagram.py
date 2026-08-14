from __future__ import annotations

import asyncio
import contextlib
import os
import shutil
import socket
import sys
import tempfile
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import NoReturn

import pytest

from tests.conftest import running_loop
from zuvloop import _connect, _zuvloop

pytestmark = pytest.mark.anyio
requires_unix_sockets = pytest.mark.skipif(sys.platform == "win32", reason="Windows has no Unix sockets")

Address = tuple[str, int] | str | bytes | None


@contextlib.contextmanager
def unix_socket_dir() -> Iterator[Path]:
    """`tmp_path` overruns the length a unix socket path may have, and binding
    leaves the path behind - closing the socket does not unlink it."""
    directory = tempfile.mkdtemp()
    try:
        yield Path(directory)
    finally:
        shutil.rmtree(directory, ignore_errors=True)


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


async def start_echo() -> tuple[asyncio.DatagramTransport, Echo, tuple[str, int]]:
    transport, protocol = await running_loop().create_datagram_endpoint(Echo, local_addr=("127.0.0.1", 0))
    sockname = transport.get_extra_info("sockname")
    assert (
        isinstance(sockname, tuple)
        and len(sockname) == 2
        and isinstance(sockname[0], str)
        and isinstance(sockname[1], int)
    )
    return transport, protocol, sockname


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


async def test_a_connected_endpoint_accepts_the_address_it_is_connected_to() -> None:
    """asyncio allows `addr` when it names the peer already connected to."""
    server, _echo, address = await start_echo()
    client, protocol = await running_loop().create_datagram_endpoint(Collector, remote_addr=address)
    try:
        client.sendto(b"hello", address)
        assert protocol.done is not None
        assert await asyncio.wait_for(protocol.done, 2) == b"re:hello"
    finally:
        client.close()
        server.close()


async def test_a_connected_endpoint_rejects_a_different_address() -> None:
    server, _echo, address = await start_echo()
    client, _protocol = await running_loop().create_datagram_endpoint(Collector, remote_addr=address)
    try:
        host, port = address[0], address[1]
        assert isinstance(host, str) and isinstance(port, int)
        with pytest.raises(ValueError, match="connected"):
            client.sendto(b"hello", (host, port + 1))
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


@pytest.mark.parametrize("abort", [False, True])
async def test_shutdown_does_not_resume_or_report_cancelled_sends(abort: bool) -> None:
    loop = running_loop()
    with unix_socket_dir() as directory:
        receiver_path = str(directory / "receiver")
        sender_path = str(directory / "sender")
        receiver = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        receiver.bind(receiver_path)
        receiver.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 2048)

        sender = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        sender.bind(sender_path)
        sender.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)
        sender.setblocking(False)

        events: list[str] = []

        class FlowControl(asyncio.DatagramProtocol):
            def __init__(self) -> None:
                self.lost: asyncio.Future[None] = loop.create_future()

            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                events.append("made")

            def pause_writing(self) -> None:
                events.append("pause")

            def resume_writing(self) -> None:
                events.append("resume")  # pragma: no cover - the assertion is that this never runs

            def error_received(self, exc: BaseException) -> None:
                events.append(type(exc).__name__)  # pragma: no cover - the assertion is that this never runs

            def connection_lost(self, exc: BaseException | None) -> None:
                events.append("lost")
                self.lost.set_result(None)

        raw, protocol = await loop.create_datagram_endpoint(FlowControl, sock=sender)
        assert isinstance(raw, _zuvloop.DatagramTransport)
        transport = raw
        transport.set_write_buffer_limits(high=1, low=0)
        try:
            for _ in range(100):  # pragma: no branch - exhaustion fails via the assertion below
                transport.sendto(b"x" * 1024, receiver_path)
                if transport.get_write_buffer_size() != 0:
                    break
            assert transport.get_write_buffer_size() != 0
            assert events == ["made", "pause"]

            if abort:
                transport.abort()
            else:
                transport.close()
            receiver.close()
            await asyncio.wait_for(protocol.lost, 2)
            await asyncio.sleep(0)
            await asyncio.sleep(0)
            assert events == ["made", "pause", "lost"]
        finally:
            receiver.close()
            transport.abort()


async def test_sending_after_close_is_dropped() -> None:
    """asyncio drops it; a shutdown race should not raise out of user code."""
    loop = running_loop()
    server, echo, address = await start_echo()
    transport, _protocol = await loop.create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    live, live_protocol = await loop.create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    try:
        transport.close()
        transport.sendto(b"dropped", address)
        # Once the echo has answered this one, anything the closed endpoint sent
        # would have arrived.
        live.sendto(b"delivered", address)
        assert live_protocol.done is not None
        assert await asyncio.wait_for(live_protocol.done, 2) == b"re:delivered"
        assert [data for data, _addr in echo.received] == [b"delivered"]
    finally:
        live.close()
        server.close()


async def test_a_closing_endpoint_still_rejects_a_bad_address() -> None:
    """Only the state check softens - the argument errors asyncio raises remain."""
    transport, _protocol = await running_loop().create_datagram_endpoint(Collector, remote_addr=("127.0.0.1", 9))
    transport.close()
    with pytest.raises(ValueError, match="connected"):
        transport.sendto(b"hello", ("127.0.0.1", 1234))


async def test_a_closing_endpoint_still_rejects_a_payload_that_is_not_a_buffer() -> None:
    """asyncio checks the type before it decides to drop anything."""
    transport, _protocol = await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    transport.close()
    with pytest.raises(TypeError):
        transport.sendto(123, ("127.0.0.1", 1234))  # type: ignore[arg-type]


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
    assert isinstance(raw, _zuvloop.DatagramTransport)
    transport = raw
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
        with pytest.raises(ValueError, match="high water mark must be non-negative"):
            transport.set_write_buffer_limits(high=-1)
        with pytest.raises(ValueError, match="low water mark must be non-negative"):
            transport.set_write_buffer_limits(low=-1)
        with pytest.raises(OverflowError, match="high water mark is too large"):
            transport.set_write_buffer_limits(low=sys.maxsize)
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
        client.close()
        assert sock.fileno() == -1
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


async def test_broadcast_is_applied() -> None:
    transport, _protocol = await running_loop().create_datagram_endpoint(
        Collector, local_addr=("127.0.0.1", 0), allow_broadcast=True
    )
    try:
        sock = transport.get_extra_info("socket")
        assert sock.getsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST) != 0
    finally:
        transport.close()


@pytest.mark.skipif(sys.platform == "win32", reason="Windows has no SO_REUSEPORT")
async def test_reuse_port_is_applied() -> None:
    transport, _protocol = await running_loop().create_datagram_endpoint(
        Collector, local_addr=("127.0.0.1", 0), reuse_port=True
    )
    try:
        sock = transport.get_extra_info("socket")
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


async def test_a_connected_v6_endpoint_tells_the_scope_apart() -> None:
    """The scope is part of what names an IPv6 peer, so a different one is a
    different peer."""
    loop = running_loop()
    try:
        server, _echo = await loop.create_datagram_endpoint(Echo, local_addr=("::1", 0))
    except OSError:  # pragma: no cover - host without IPv6 loopback
        pytest.skip("no IPv6 loopback")
    try:
        host, port = server.get_extra_info("sockname")[:2]
        client, protocol = await loop.create_datagram_endpoint(
            Collector, remote_addr=(host, port), family=socket.AF_INET6
        )
        try:
            client.sendto(b"v6", (host, port, 0, 0))
            assert protocol.done is not None
            assert await asyncio.wait_for(protocol.done, 2) == b"re:v6"
            with pytest.raises(ValueError, match="connected"):
                client.sendto(b"v6", (host, port, 0, 1))
            with pytest.raises(ValueError, match="connected"):
                client.sendto(b"v6", (host, port, 1, 0))
        finally:
            client.close()
    finally:
        server.close()


@pytest.mark.parametrize(
    "address",
    [
        ("::1", 9, -1, 0),
        ("::1", 9, 1_048_576, 0),
        ("::1", 9, 0, -1),
        ("::1", 9, 0, 1 << 32),
    ],
)
async def test_ipv6_send_rejects_out_of_range_ancillary_fields(
    address: tuple[str, int, int, int],
) -> None:
    loop = running_loop()
    try:
        transport, _protocol = await loop.create_datagram_endpoint(Collector, local_addr=("::1", 0))
    except OSError:  # pragma: no cover - host without IPv6 loopback
        pytest.skip("no IPv6 loopback")
    try:
        with pytest.raises(OverflowError):
            transport.sendto(b"x", address)
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


async def test_a_failed_protocol_factory_closes_its_internal_datagram_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_socket = socket.socket
    created: list[socket.socket] = []

    def track(family: int = -1, type: int = -1, proto: int = -1, fileno: int | None = None) -> socket.socket:
        sock = real_socket(family, type, proto) if fileno is None else real_socket(family, type, proto, fileno)
        created.append(sock)
        return sock

    def fail() -> NoReturn:
        raise RuntimeError("factory failed")

    monkeypatch.setattr(socket, "socket", track)
    with pytest.raises(RuntimeError, match="factory failed"):
        await running_loop().create_datagram_endpoint(fail, local_addr=("127.0.0.1", 0))

    assert len(created) == 1
    assert created[0].fileno() == -1


async def test_a_transport_that_fails_to_adopt_releases_the_socket(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*args: object, **kwargs: object) -> _zuvloop.DatagramTransport:
        raise RuntimeError("refused")

    monkeypatch.setattr(type(running_loop()), "_make_datagram_transport", refuse)
    with pytest.raises(RuntimeError, match="refused"):
        await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))


async def test_a_transport_that_fails_to_adopt_its_socket_view_is_aborted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_socket = socket.socket
    created: list[socket.socket] = []

    class RefusingTransport:
        def __init__(self, fd: int) -> None:
            self.fd = fd
            self.aborted = False

        def _adopt_socket_view(self, _sock: socket.socket) -> NoReturn:
            raise RuntimeError("adoption failed")

        def abort(self) -> None:
            os.close(self.fd)
            self.aborted = True

    refusing: RefusingTransport | None = None

    def track(family: int = -1, type: int = -1, proto: int = -1, fileno: int | None = None) -> socket.socket:
        sock = real_socket(family, type, proto) if fileno is None else real_socket(family, type, proto, fileno)
        created.append(sock)
        return sock

    def make_refusing_transport(
        _loop: asyncio.AbstractEventLoop,
        fd: int,
        _family: int,
        _connected: bool,
        _protocol: asyncio.BaseProtocol,
        _extra: Mapping[str, object],
    ) -> RefusingTransport:
        nonlocal refusing
        refusing = RefusingTransport(fd)
        return refusing

    monkeypatch.setattr(socket, "socket", track)
    monkeypatch.setattr(type(running_loop()), "_make_datagram_transport", make_refusing_transport)
    with pytest.raises(RuntimeError, match="adoption failed"):
        await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))

    assert len(created) == 1
    assert created[0].fileno() == -1
    assert refusing is not None
    assert refusing.aborted
    with pytest.raises(OSError, match="Bad file descriptor"):
        os.fstat(refusing.fd)


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


@requires_unix_sockets
async def test_unix_datagram_endpoints_round_trip() -> None:
    loop = running_loop()
    with unix_socket_dir() as directory:
        server_path = str(directory / "s")
        client_path = str(directory / "c")
        server, _echo = await loop.create_datagram_endpoint(Echo, local_addr=server_path, family=socket.AF_UNIX)
        client, protocol = await loop.create_datagram_endpoint(Collector, local_addr=client_path, family=socket.AF_UNIX)
        try:
            client.sendto(b"unix", server_path)
            assert protocol.done is not None
            assert await asyncio.wait_for(protocol.done, 2) == b"re:unix"
        finally:
            client.close()
            server.close()


@pytest.mark.skipif(sys.platform != "linux", reason="abstract AF_UNIX names are a Linux feature")
async def test_abstract_unix_datagram_sender_address_is_preserved() -> None:  # pragma: no cover - Linux CI
    loop = running_loop()
    suffix = f"{os.getpid()}-{os.urandom(6).hex()}".encode()
    receiver_name = b"\0zuvloop-receiver-" + suffix + b"\0"
    sender_name = b"\0zuvloop-sender-" + suffix + b"\0"
    receiver, protocol = await loop.create_datagram_endpoint(Collector, local_addr=receiver_name, family=socket.AF_UNIX)
    sender, _sender_protocol = await loop.create_datagram_endpoint(
        Collector, local_addr=sender_name, family=socket.AF_UNIX
    )
    try:
        sender.sendto(b"abstract", receiver_name)
        assert protocol.done is not None
        assert await asyncio.wait_for(protocol.done, 2) == b"abstract"
        assert protocol.received == [(b"abstract", sender_name)]
    finally:
        sender.close()
        receiver.close()


@pytest.mark.skipif(sys.platform != "linux", reason="abstract AF_UNIX names are a Linux feature")
async def test_unnamed_unix_datagram_sender_has_no_address() -> None:  # pragma: no cover - Linux CI
    loop = running_loop()
    receiver_name = b"\0zuvloop-receiver-" + f"{os.getpid()}-{os.urandom(6).hex()}".encode()
    receiver, protocol = await loop.create_datagram_endpoint(Collector, local_addr=receiver_name, family=socket.AF_UNIX)
    sender, _sender_protocol = await loop.create_datagram_endpoint(Collector, family=socket.AF_UNIX)
    try:
        sender.sendto(b"unnamed", receiver_name)
        assert protocol.done is not None
        assert await asyncio.wait_for(protocol.done, 2) == b"unnamed"
        assert protocol.received == [(b"unnamed", None)]
    finally:
        sender.close()
        receiver.close()


async def test_a_connected_unix_endpoint_accepts_the_path_it_is_connected_to() -> None:
    """For AF_UNIX the path is what identifies the peer."""
    loop = running_loop()
    with unix_socket_dir() as directory:
        server_path = str(directory / "s")
        client_path = str(directory / "c")
        server, _echo = await loop.create_datagram_endpoint(Echo, local_addr=server_path, family=socket.AF_UNIX)
        client, protocol = await loop.create_datagram_endpoint(
            Collector, local_addr=client_path, remote_addr=server_path, family=socket.AF_UNIX
        )
        try:
            client.sendto(b"unix", server_path)
            assert protocol.done is not None
            assert await asyncio.wait_for(protocol.done, 2) == b"re:unix"
        finally:
            client.close()
            server.close()


async def test_a_connected_unix_endpoint_rejects_a_different_path() -> None:
    loop = running_loop()
    with unix_socket_dir() as directory:
        server_path = str(directory / "s")
        client_path = str(directory / "c")
        server, _echo = await loop.create_datagram_endpoint(Echo, local_addr=server_path, family=socket.AF_UNIX)
        client, _protocol = await loop.create_datagram_endpoint(
            Collector, local_addr=client_path, remote_addr=server_path, family=socket.AF_UNIX
        )
        try:
            with pytest.raises(ValueError, match="connected"):
                client.sendto(b"unix", str(directory / "other"))
        finally:
            client.close()
            server.close()


async def test_an_endpoint_from_a_connected_socket_accepts_its_peer() -> None:
    """A connected `sock` makes the endpoint connected, with no `remote_addr`."""
    server, _echo, address = await start_echo()
    host, port = address[0], address[1]
    assert isinstance(host, str) and isinstance(port, int)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    sock.connect((host, port))
    client, protocol = await running_loop().create_datagram_endpoint(Collector, sock=sock)
    try:
        client.sendto(b"hello", (host, port))
        assert protocol.done is not None
        assert await asyncio.wait_for(protocol.done, 2) == b"re:hello"
        with pytest.raises(ValueError, match="connected"):
            client.sendto(b"hello", (host, port + 1))
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


@pytest.mark.parametrize("port", [-1, 65536, 70000, 2**31, 2**63, 2**64])
async def test_an_out_of_range_port_is_rejected(port: int) -> None:
    """`uv_ip4_addr` stores `htons(port)`, so 70000 would address 4464 instead.

    The standard library refuses the address rather than sending it somewhere
    else, and `socket.sendto` raises `OverflowError` for it - for every port out
    of range, including the ones too wide for the C types it is converted through.
    """
    transport, _protocol = await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    try:
        with pytest.raises(OverflowError, match="0-65535"):
            transport.sendto(b"misdirected", ("127.0.0.1", port))
    finally:
        transport.close()


async def test_pausing_reading_stops_delivery_until_resumed() -> None:
    """asyncio implements both; uvloop has neither, and inheriting the stubs
    from `asyncio.Transport` made them raise here."""
    loop = running_loop()
    raw, protocol = await loop.create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    assert isinstance(raw, _zuvloop.DatagramTransport)
    receiver = raw
    address = receiver.get_extra_info("sockname")
    sender, _sender_protocol = await loop.create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    control, control_protocol = await loop.create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    control_address = control.get_extra_info("sockname")
    try:
        receiver.pause_reading()
        sender.sendto(b"while paused", address)
        # An endpoint that is not paused, sent to afterwards: once its datagram
        # arrives the loop has been round, so the receiver's silence is the
        # pause rather than a clock that had not run out.
        sender.sendto(b"unpaused", control_address)
        assert control_protocol.done is not None
        assert await asyncio.wait_for(control_protocol.done, 2) == b"unpaused"
        assert protocol.received == []

        receiver.resume_reading()
        assert protocol.done is not None
        assert await asyncio.wait_for(protocol.done, 2) == b"while paused"
    finally:
        control.close()
        sender.close()
        receiver.close()


async def test_pausing_reading_after_close_is_rejected() -> None:
    raw, _protocol = await running_loop().create_datagram_endpoint(Collector, local_addr=("127.0.0.1", 0))
    assert isinstance(raw, _zuvloop.DatagramTransport)
    transport = raw
    transport.close()
    with pytest.raises(RuntimeError, match="after close"):
        transport.pause_reading()
    with pytest.raises(RuntimeError, match="after close"):
        transport.resume_reading()
