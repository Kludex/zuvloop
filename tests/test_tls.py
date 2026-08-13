from __future__ import annotations

import asyncio
import contextlib
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


async def test_start_tls_consumes_a_buffered_client_hello(
    server_context: ssl.SSLContext, client_context: ssl.SSLContext
) -> None:
    loop = running_loop()
    proxy_line = b"PROXY TCP4 127.0.0.1 127.0.0.1 54321 443\r\n"
    target_done: asyncio.Future[int] = loop.create_future()

    async def handle_target(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            assert await reader.readline() == proxy_line
            buffer: bytearray = getattr(reader, "_buffer")
            async with asyncio.timeout(1):
                while not getattr(reader, "_paused"):  # pragma: no cover - TCP chunking varies by platform
                    await asyncio.sleep(0)
            buffered = len(buffer)
            assert buffered > 0
            await writer.start_tls(server_context)
            assert getattr(reader, "_paused") is False
            payload = await reader.readexactly(6)
            writer.write(payload)
            await writer.drain()
            target_done.set_result(buffered)
        except BaseException as exc:  # pragma: no cover - assertion diagnostic
            if not target_done.done():
                target_done.set_exception(exc)
        finally:
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()

    target = await asyncio.start_server(handle_target, "127.0.0.1", 0, limit=64)
    target_address = target.sockets[0].getsockname()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.setblocking(False)

    async def relay(source: socket.socket, destination: socket.socket) -> None:
        while data := await loop.sock_recv(source, 65_536):
            await loop.sock_sendall(destination, data)

    async def proxy_once() -> None:
        client, _address = await loop.sock_accept(listener)
        remote = socket.socket()
        client.setblocking(False)
        remote.setblocking(False)
        try:
            await loop.sock_connect(remote, target_address)
            client_hello = bytearray()
            while len(client_hello) < 5:
                client_hello += await loop.sock_recv(client, 5 - len(client_hello))
            record_size = 5 + int.from_bytes(client_hello[3:5])
            while len(client_hello) < record_size:
                client_hello += await loop.sock_recv(client, 65_536)
            await loop.sock_sendall(remote, proxy_line + client_hello)

            relays = [
                loop.create_task(relay(client, remote)),
                loop.create_task(relay(remote, client)),
            ]
            _done, pending = await asyncio.wait(relays, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            await asyncio.gather(*relays, return_exceptions=True)
        finally:
            client.close()
            remote.close()

    proxy_task = loop.create_task(proxy_once())
    writer: asyncio.StreamWriter | None = None
    try:
        reader, writer = await asyncio.open_connection(*listener.getsockname())
        await writer.start_tls(client_context, server_hostname="localhost")
        writer.write(b"secure")
        await writer.drain()
        assert await asyncio.wait_for(reader.readexactly(6), 5) == b"secure"
        assert await asyncio.wait_for(target_done, 5) > 0
    finally:
        if writer is not None:  # pragma: no branch - open failure is cleanup-only
            writer.close()
            with contextlib.suppress(OSError):
                await writer.wait_closed()
        listener.close()
        target.close()
        await target.wait_closed()
        if not proxy_task.done():  # pragma: no branch - completion timing is platform-dependent
            proxy_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await proxy_task


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
