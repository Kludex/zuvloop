from __future__ import annotations

import asyncio
import socket
import ssl

import pytest

from conftest import collect_contexts, running_loop
from zuvloop._server import Server

pytestmark = pytest.mark.anyio


async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    data = await reader.read(64)
    writer.write(data)
    await writer.drain()
    writer.close()


async def test_tls_round_trip(server_context: ssl.SSLContext, client_context: ssl.SSLContext) -> None:
    server = await asyncio.start_server(echo, "127.0.0.1", 0, ssl=server_context)
    port = server.sockets[0].getsockname()[1]
    async with server:
        reader, writer = await asyncio.open_connection(
            "127.0.0.1", port, ssl=client_context, server_hostname="localhost"
        )
        writer.write(b"encrypted")
        await writer.drain()
        assert await reader.readexactly(9) == b"encrypted"
        writer.close()
        await writer.wait_closed()


async def test_tls_exposes_the_peer_certificate(server_context: ssl.SSLContext, client_context: ssl.SSLContext) -> None:
    server = await asyncio.start_server(echo, "127.0.0.1", 0, ssl=server_context)
    port = server.sockets[0].getsockname()[1]
    async with server:
        _reader, writer = await asyncio.open_connection(
            "127.0.0.1", port, ssl=client_context, server_hostname="localhost"
        )
        assert writer.get_extra_info("peercert")["subject"]
        assert writer.get_extra_info("cipher") is not None
        writer.close()
        await writer.wait_closed()


async def test_tls_rejects_an_untrusted_certificate(server_context: ssl.SSLContext) -> None:
    loop = running_loop()
    # The server side sees the client's rejection as a reset; that is expected here.
    loop.set_exception_handler(lambda _loop, _context: None)
    server = await asyncio.start_server(echo, "127.0.0.1", 0, ssl=server_context)
    port = server.sockets[0].getsockname()[1]
    try:
        with pytest.raises(ssl.SSLCertVerificationError):
            await asyncio.open_connection("127.0.0.1", port, ssl=True, server_hostname="localhost")
        await asyncio.sleep(0.05)
    finally:
        server.close()
        loop.set_exception_handler(None)


async def test_cancelling_a_server_handshake_disarms_the_accepted_socket_before_close(
    server_context: ssl.SSLContext,
) -> None:
    loop = running_loop()

    class CloseTrackingSocket(socket.socket):
        close_calls = 0

        def close(self) -> None:  # pragma: no cover - the assertion is that this never runs
            self.close_calls += 1
            super().close()

    raw, peer = socket.socketpair()
    accepted = CloseTrackingSocket(fileno=raw.detach())
    accepted.setblocking(False)
    server = Server(loop, (), asyncio.Protocol, server_context, 1, None, None)
    server._active = 1

    try:
        handshake = loop.create_task(loop._accept_tls(accepted, asyncio.Protocol, server))
        await asyncio.sleep(0)
        handshake.cancel()
        with pytest.raises(asyncio.CancelledError):
            await handshake
        await asyncio.sleep(0)

        assert accepted.close_calls == 0
        assert accepted.fileno() == -1
        assert server._active == 0
    finally:
        peer.close()


async def test_a_failed_handshake_is_reported(server_context: ssl.SSLContext) -> None:
    loop = running_loop()
    reported = collect_contexts(loop)
    server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0, ssl=server_context)
    port = server.sockets[0].getsockname()[1]
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"this is not a TLS ClientHello")
        await writer.drain()
        await reader.read(1)
        writer.close()
        await asyncio.sleep(0.2)
        assert any("TLS handshake" in str(entry.get("message", "")) for entry in reported)
    finally:
        loop.set_exception_handler(None)
        server.close()


async def test_start_tls_upgrades_a_plain_connection(
    server_context: ssl.SSLContext, client_context: ssl.SSLContext
) -> None:
    loop = running_loop()
    upgraded = loop.create_future()

    class ServerSide(asyncio.Protocol):
        def __init__(self) -> None:
            self.transport: asyncio.Transport | None = None

        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport  # type: ignore[assignment]

        def data_received(self, data: bytes) -> None:
            if data == b"STARTTLS":
                loop.create_task(self._upgrade())
            else:
                assert self.transport is not None
                self.transport.write(data)

        async def _upgrade(self) -> None:
            assert self.transport is not None
            self.transport = await loop.start_tls(self.transport, self, server_context, server_side=True)
            upgraded.set_result(None)

    server = await loop.create_server(ServerSide, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"STARTTLS")
        await writer.drain()
        await asyncio.sleep(0.05)
        await writer.start_tls(client_context, server_hostname="localhost")
        await upgraded
        writer.write(b"secure now")
        await writer.drain()
        assert await reader.readexactly(10) == b"secure now"
        writer.close()
        await asyncio.sleep(0.05)


async def test_server_side_tls_needs_a_context() -> None:
    loop = running_loop()
    with pytest.raises(ValueError, match="needs an SSLContext"):
        await loop.create_server(asyncio.Protocol, "127.0.0.1", 0, ssl=True)


async def test_start_tls_closes_the_transport_when_the_handshake_fails(client_context: ssl.SSLContext) -> None:
    """The echo server replies with the ClientHello, which is not a ServerHello."""
    loop = running_loop()

    class Echo(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport

        def data_received(self, data: bytes) -> None:
            self.transport.write(data)  # type: ignore[attr-defined]

    server = await loop.create_server(Echo, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    async with server:
        protocol = asyncio.Protocol()
        transport, _ = await loop.create_connection(lambda: protocol, "127.0.0.1", port)
        with pytest.raises(OSError):
            await loop.start_tls(transport, protocol, client_context, server_hostname="localhost")
        assert transport.is_closing()
        await asyncio.sleep(0.05)
