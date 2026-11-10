from __future__ import annotations

import asyncio
import ssl

import pytest

from tests.conftest import running_loop

pytestmark = pytest.mark.anyio


class BufferedReader(asyncio.BufferedProtocol):
    def __init__(self) -> None:
        self.buffer = bytearray(4096)
        self.acquired = 0
        self.released = 0
        self.chunks: asyncio.Queue[bytes] = asyncio.Queue()
        self.connected: asyncio.Future[asyncio.Transport] = running_loop().create_future()
        self.closed: asyncio.Future[None] = running_loop().create_future()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        assert isinstance(transport, asyncio.Transport)
        self.connected.set_result(transport)

    def get_buffer(self, sizehint: int) -> memoryview:
        return memoryview(self)

    def __buffer__(self, flags: int) -> memoryview:
        assert self.acquired == self.released
        self.acquired += 1
        return memoryview(self.buffer)

    def __release_buffer__(self, view: memoryview) -> None:
        self.released += 1

    def buffer_updated(self, nbytes: int) -> None:
        self.chunks.put_nowait(bytes(self.buffer[:nbytes]))

    def connection_lost(self, exc: Exception | None) -> None:
        assert exc is None
        self.closed.set_result(None)

    async def receive(self, expected: bytes) -> None:
        received = bytearray()
        while len(received) < len(expected):
            received.extend(await self.chunks.get())
        assert received == expected
        assert self.acquired > 0
        assert self.acquired == self.released
        self.buffer.append(0)
        self.buffer.pop()


async def test_buffered_reads_release_views_before_protocol_replacement() -> None:
    loop = running_loop()
    server_protocol = BufferedReader()
    server = await loop.create_server(lambda: server_protocol, "127.0.0.1", 0)
    async with asyncio.timeout(10), server:
        client, protocol = await loop.create_connection(BufferedReader, *server.sockets[0].getsockname())
        peer = await server_protocol.connected
        try:
            for index in range(20):
                payload = bytes([index]) * 65_536
                peer.write(payload)
                await protocol.receive(payload)
                peer.write(payload)
                client.pause_reading()
                replacement = BufferedReader()
                client.set_protocol(replacement)
                replacement.connection_made(client)
                assert protocol.acquired == protocol.released
                protocol.buffer.clear()
                client.resume_reading()
                await replacement.receive(payload)
                protocol = replacement
        finally:
            client.close()
            peer.close()
            await asyncio.gather(protocol.closed, server_protocol.closed)


async def test_buffered_reads_release_views_before_tls_upgrade(
    server_context: ssl.SSLContext, client_context: ssl.SSLContext
) -> None:
    loop = running_loop()
    async with asyncio.timeout(20):
        for _ in range(10):
            server_protocol = BufferedReader()
            server = await loop.create_server(lambda: server_protocol, "127.0.0.1", 0)
            async with server:
                client, protocol = await loop.create_connection(BufferedReader, *server.sockets[0].getsockname())
                peer = await server_protocol.connected
                try:
                    payload = b"plain" * 16_384
                    peer.write(payload)
                    client.write(payload)
                    await asyncio.gather(protocol.receive(payload), server_protocol.receive(payload))
                    secured_peer, secured_client = await asyncio.gather(
                        loop.start_tls(peer, server_protocol, server_context, server_side=True),
                        loop.start_tls(client, protocol, client_context, server_hostname="localhost"),
                    )
                    assert secured_peer is not None and secured_client is not None
                    assert protocol.acquired == protocol.released
                    assert server_protocol.acquired == server_protocol.released
                    peer, client = secured_peer, secured_client
                    payload = b"encrypted" * 16_384
                    peer.write(payload)
                    client.write(payload)
                    await asyncio.gather(protocol.receive(payload), server_protocol.receive(payload))
                finally:
                    client.close()
                    peer.close()
                    await asyncio.gather(protocol.closed, server_protocol.closed)
