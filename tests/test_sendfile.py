from __future__ import annotations

import asyncio
import errno
import io
import os
import socket
import ssl
import tempfile
from asyncio.constants import _SendfileMode
from collections.abc import Iterator
from pathlib import Path
from typing import IO, cast

import pytest

from conftest import running_loop
from zuvloop import _zuvloop
from zuvloop._sendfile import _SendfileProtocol
from zuvloop._server import Server

pytestmark = pytest.mark.anyio

PAYLOAD = (bytes(range(256)) * (16 * 1024 + 64))[: 4 * 1024 * 1024]


@pytest.fixture
def payload_file(tmp_path: Path) -> Iterator[IO[bytes]]:
    path = tmp_path / "payload.bin"
    path.write_bytes(PAYLOAD)
    with path.open("rb") as file:
        yield file


@pytest.fixture
def stream_pair() -> Iterator[tuple[socket.socket, socket.socket]]:
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    with left, right:
        yield left, right


async def read_exactly(sock: socket.socket, expected: int) -> bytes:
    loop = running_loop()
    received = bytearray()
    while len(received) < expected:
        chunk = await loop.sock_recv(sock, 1 << 16)
        assert chunk, "peer closed before the expected bytes arrived"
        received += chunk
    return bytes(received)


class Recorder(asyncio.Protocol):
    """Client protocol that records the flow-control calls it receives."""

    def __init__(self) -> None:
        self.paused = 0
        self.resumed = 0
        self.lost: list[BaseException | None] = []

    def pause_writing(self) -> None:
        self.paused += 1

    def resume_writing(self) -> None:
        self.resumed += 1

    def connection_lost(self, exc: BaseException | None) -> None:
        self.lost.append(exc)


class Sink(asyncio.Protocol):
    """Server protocol that collects what arrives and resolves on close."""

    def __init__(self) -> None:
        self.transport: asyncio.Transport | None = None
        self.received = bytearray()
        self.closed: asyncio.Future[None] = running_loop().create_future()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.transport = cast("asyncio.Transport", transport)

    def data_received(self, data: bytes) -> None:
        self.received += data

    def connection_lost(self, exc: BaseException | None) -> None:
        self.closed.set_result(None)


class StalledSink(Sink):
    """Stops reading, so the sender's buffers stay full.

    The transport starts delivering reads right after `connection_made`, so the
    pause must be scheduled behind that rather than called directly. A paused
    connection never notices the peer vanishing, so tests abort it themselves.
    """

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        super().connection_made(transport)
        running_loop().call_soon(cast("asyncio.Transport", transport).pause_reading)


async def start_sink(protocol_type: type[Sink] = Sink) -> tuple[Server, int, list[Sink]]:
    sinks: list[Sink] = []

    def factory() -> Sink:
        sink = protocol_type()
        sinks.append(sink)
        return sink

    loop = running_loop()
    server = await loop.create_server(factory, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1], sinks


# -- sock_sendfile ----------------------------------------------------------


async def test_sock_sendfile_sends_the_whole_file(
    stream_pair: tuple[socket.socket, socket.socket], payload_file: IO[bytes]
) -> None:
    left, right = stream_pair
    loop = running_loop()
    reader = loop.create_task(read_exactly(right, len(PAYLOAD)))
    sent = await loop.sock_sendfile(left, payload_file)
    assert sent == len(PAYLOAD)
    assert payload_file.tell() == len(PAYLOAD)
    assert await reader == PAYLOAD


async def test_sock_sendfile_honours_offset_and_count(
    stream_pair: tuple[socket.socket, socket.socket], payload_file: IO[bytes]
) -> None:
    left, right = stream_pair
    loop = running_loop()
    reader = loop.create_task(read_exactly(right, 1000))
    sent = await loop.sock_sendfile(left, payload_file, offset=512, count=1000)
    assert sent == 1000
    assert payload_file.tell() == 1512
    assert await reader == PAYLOAD[512:1512]


async def test_sock_sendfile_stops_at_the_end_of_the_file(
    stream_pair: tuple[socket.socket, socket.socket], tmp_path: Path
) -> None:
    left, right = stream_pair
    loop = running_loop()
    path = tmp_path / "small.bin"
    path.write_bytes(PAYLOAD[:100])
    with path.open("rb") as file:
        reader = loop.create_task(read_exactly(right, 100))
        assert await loop.sock_sendfile(left, file, count=1000) == 100
        assert await reader == PAYLOAD[:100]
        assert await loop.sock_sendfile(left, file, offset=200) == 0


async def test_sock_sendfile_of_an_empty_file_sends_nothing(
    stream_pair: tuple[socket.socket, socket.socket], tmp_path: Path
) -> None:
    left, _right = stream_pair
    loop = running_loop()
    path = tmp_path / "empty.bin"
    path.write_bytes(b"")
    with path.open("rb") as file:
        assert await loop.sock_sendfile(left, file) == 0
        assert file.tell() == 0


async def test_sock_sendfile_falls_back_without_a_real_file(
    stream_pair: tuple[socket.socket, socket.socket],
) -> None:
    left, right = stream_pair
    loop = running_loop()
    file = io.BytesIO(PAYLOAD[:100_000])
    reader = loop.create_task(read_exactly(right, 99_000))
    sent = await loop.sock_sendfile(left, file, offset=1000)
    assert sent == 99_000
    assert file.tell() == 100_000
    assert await reader == PAYLOAD[1000:100_000]


async def test_sock_sendfile_without_fallback_requires_a_regular_file(
    stream_pair: tuple[socket.socket, socket.socket],
) -> None:
    left, _right = stream_pair
    loop = running_loop()
    with pytest.raises(asyncio.SendfileNotAvailableError):
        await loop.sock_sendfile(left, io.BytesIO(b"data"), fallback=False)


class PipeBackedFile:
    """Readable, with a descriptor `os.sendfile` rejects, and no `seek`."""

    def __init__(self, fd: int, data: bytes) -> None:
        self._fd = fd
        self._buffer = io.BytesIO(data)

    def fileno(self) -> int:
        return self._fd

    def read(self, size: int) -> bytes:
        return self._buffer.read(size)


async def test_sock_sendfile_falls_back_when_the_descriptor_is_not_a_file(
    stream_pair: tuple[socket.socket, socket.socket],
) -> None:
    left, right = stream_pair
    loop = running_loop()
    read_end, write_end = os.pipe()
    try:
        source = PipeBackedFile(read_end, b"0123456789")
        reader = loop.create_task(read_exactly(right, 10))
        sent = await loop.sock_sendfile(left, cast("IO[bytes]", source), count=10)
        assert sent == 10
        assert await reader == b"0123456789"
    finally:
        os.close(read_end)
        os.close(write_end)


class SeeklessFile:
    """Readable, but with no descriptor and no way to reposition."""

    def __init__(self, data: bytes) -> None:
        self._buffer = io.BytesIO(data)

    def read(self, size: int) -> bytes:
        return self._buffer.read(size)


async def test_sock_sendfile_treats_a_missing_descriptor_as_no_file(
    stream_pair: tuple[socket.socket, socket.socket],
) -> None:
    left, _right = stream_pair
    loop = running_loop()
    assert await loop.sock_sendfile(left, cast("IO[bytes]", SeeklessFile(b""))) == 0


async def test_sock_sendfile_validates_its_arguments(
    stream_pair: tuple[socket.socket, socket.socket], tmp_path: Path
) -> None:
    left, _right = stream_pair
    loop = running_loop()
    path = tmp_path / "text.txt"
    path.write_text("plain text")

    with socket.socket() as blocking:
        with pytest.raises(ValueError, match="non-blocking"):
            await loop.sock_sendfile(blocking, io.BytesIO())
    with path.open("r") as text:
        with pytest.raises(ValueError, match="binary mode"):
            await loop.sock_sendfile(left, cast("IO[bytes]", text))
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as datagram:
        datagram.setblocking(False)
        with pytest.raises(ValueError, match="SOCK_STREAM"):
            await loop.sock_sendfile(datagram, io.BytesIO())
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    with context.wrap_socket(socket.socket(), do_handshake_on_connect=False) as wrapped:
        wrapped.setblocking(False)
        with pytest.raises(TypeError, match="SSLSocket"):
            await loop.sock_sendfile(wrapped, io.BytesIO())
    with pytest.raises(ValueError, match="count"):
        await loop.sock_sendfile(left, io.BytesIO(), count=0)
    with pytest.raises(TypeError, match="count"):
        await loop.sock_sendfile(left, io.BytesIO(), count=1.5)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="offset"):
        await loop.sock_sendfile(left, io.BytesIO(), offset=-1)
    with pytest.raises(TypeError, match="offset"):
        await loop.sock_sendfile(left, io.BytesIO(), offset=1.5)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("code", "expected"),
    [(errno.ENOTCONN, ConnectionError), (errno.EIO, OSError)],
    ids=["ENOTCONN becomes ConnectionError", "other errors pass through"],
)
async def test_sock_sendfile_failing_after_a_partial_transfer(
    stream_pair: tuple[socket.socket, socket.socket],
    payload_file: IO[bytes],
    monkeypatch: pytest.MonkeyPatch,
    code: int,
    expected: type[OSError],
) -> None:
    left, right = stream_pair
    loop = running_loop()
    real_sendfile = os.sendfile
    calls = 0

    def flaky(out_fd: int, in_fd: int, offset: int, count: int) -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            return real_sendfile(out_fd, in_fd, offset, 1024)
        raise OSError(code, os.strerror(code))

    monkeypatch.setattr(os, "sendfile", flaky)
    reader = loop.create_task(read_exactly(right, 1024))
    with pytest.raises(expected) as excinfo:
        await loop.sock_sendfile(left, payload_file, fallback=False)
    assert not isinstance(excinfo.value, ConnectionError) or code == errno.ENOTCONN
    assert await reader == PAYLOAD[:1024]


# -- sendfile over transports ----------------------------------------------


async def test_sendfile_streams_a_file_over_a_transport(payload_file: IO[bytes]) -> None:
    loop = running_loop()
    server, port, sinks = await start_sink()
    protocol = Recorder()
    transport, _ = await loop.create_connection(lambda: protocol, "127.0.0.1", port)
    try:
        sent = await loop.sendfile(transport, payload_file)
        assert sent == len(PAYLOAD)
        assert transport.get_protocol() is protocol
        assert transport.is_reading()
        transport.write(b"tail")
    finally:
        transport.close()
        server.close()
        await server.wait_closed()
    await sinks[0].closed
    assert bytes(sinks[0].received) == PAYLOAD + b"tail"


async def test_sendfile_honours_offset_and_count_over_a_transport(payload_file: IO[bytes]) -> None:
    loop = running_loop()
    server, port, sinks = await start_sink()
    transport, _ = await loop.create_connection(asyncio.Protocol, "127.0.0.1", port)
    try:
        sent = await loop.sendfile(transport, payload_file, offset=4096, count=100_000)
        assert sent == 100_000
        assert payload_file.tell() == 104_096
    finally:
        transport.close()
        server.close()
        await server.wait_closed()
    await sinks[0].closed
    assert bytes(sinks[0].received) == PAYLOAD[4096:104_096]


async def test_sendfile_of_an_empty_file_over_a_transport(tmp_path: Path) -> None:
    loop = running_loop()
    server, port, sinks = await start_sink()
    transport, _ = await loop.create_connection(asyncio.Protocol, "127.0.0.1", port)
    path = tmp_path / "empty.bin"
    path.write_bytes(b"")
    try:
        with path.open("rb") as file:
            assert await loop.sendfile(transport, file) == 0
    finally:
        transport.close()
        server.close()
        await server.wait_closed()


async def test_sendfile_waits_for_buffered_writes_to_drain(payload_file: IO[bytes]) -> None:
    loop = running_loop()
    server, port, sinks = await start_sink(StalledSink)
    protocol = Recorder()
    transport, _ = await loop.create_connection(lambda: protocol, "127.0.0.1", port)
    # A stalled peer and a head far beyond loopback buffering, so the backlog
    # reliably outlives the sendfile call on every platform - Linux otherwise
    # absorbs multi-megabyte writes synchronously and nothing stays buffered.
    head = PAYLOAD * 8
    try:
        transport.write(head)
        # Writes are handed to libuv at the end of the iteration; only then can
        # the transport notice the backlog and pause the protocol.
        await asyncio.sleep(0)
        assert isinstance(transport, _zuvloop.Transport)
        assert transport._protocol_paused
        task = loop.create_task(loop.sendfile(transport, payload_file, count=100_000))
        await asyncio.sleep(0.05)
        assert not task.done()
        assert sinks[0].transport is not None
        sinks[0].transport.resume_reading()
        assert await task == 100_000
        assert protocol.resumed >= 1
    finally:
        transport.close()
        server.close()
        await server.wait_closed()
    await sinks[0].closed
    assert bytes(sinks[0].received) == head + PAYLOAD[:100_000]


async def test_sendfile_rejects_a_closing_transport(payload_file: IO[bytes]) -> None:
    loop = running_loop()
    server, port, _sinks = await start_sink()
    transport, _ = await loop.create_connection(asyncio.Protocol, "127.0.0.1", port)
    transport.close()
    try:
        with pytest.raises(RuntimeError, match="closing"):
            await loop.sendfile(transport, payload_file)
    finally:
        server.close()
        await server.wait_closed()


async def test_sendfile_falls_back_for_a_filelike_over_a_transport() -> None:
    loop = running_loop()
    server, port, sinks = await start_sink()
    transport, _ = await loop.create_connection(asyncio.Protocol, "127.0.0.1", port)
    try:
        with pytest.raises(asyncio.SendfileNotAvailableError):
            await loop.sendfile(transport, io.BytesIO(b"data"), fallback=False)
        sent = await loop.sendfile(transport, io.BytesIO(PAYLOAD[:200_000]), offset=1000)
        assert sent == 199_000
    finally:
        transport.close()
        server.close()
        await server.wait_closed()
    await sinks[0].closed
    assert bytes(sinks[0].received) == PAYLOAD[1000:200_000]


async def test_sendfile_over_a_unix_connection(payload_file: IO[bytes]) -> None:
    loop = running_loop()
    sinks: list[Sink] = []

    def factory() -> Sink:
        sink = Sink()
        sinks.append(sink)
        return sink

    with tempfile.TemporaryDirectory() as directory:
        path = os.path.join(directory, "sendfile.sock")
        server = await loop.create_unix_server(factory, path)
        transport, _ = await loop.create_unix_connection(asyncio.Protocol, path)
        try:
            sent = await loop.sendfile(transport, payload_file, count=500_000)
            assert sent == 500_000
        finally:
            transport.close()
            server.close()
            await server.wait_closed()
        await sinks[0].closed
        assert bytes(sinks[0].received) == PAYLOAD[:500_000]


async def test_sendfile_over_tls_uses_the_fallback(
    server_context: ssl.SSLContext, client_context: ssl.SSLContext, payload_file: IO[bytes]
) -> None:
    loop = running_loop()
    sinks: list[Sink] = []

    def factory() -> Sink:
        sink = Sink()
        sinks.append(sink)
        return sink

    server = await loop.create_server(factory, "127.0.0.1", 0, ssl=server_context)
    port = server.sockets[0].getsockname()[1]
    transport, _ = await loop.create_connection(
        asyncio.Protocol, "127.0.0.1", port, ssl=client_context, server_hostname="localhost"
    )
    try:
        with pytest.raises(RuntimeError, match="fallback is disabled"):
            await loop.sendfile(transport, payload_file, fallback=False)
        sent = await loop.sendfile(transport, payload_file, count=300_000)
        assert sent == 300_000
    finally:
        transport.close()
        server.close()
        await server.wait_closed()
    await sinks[0].closed
    assert bytes(sinks[0].received) == PAYLOAD[:300_000]


async def test_sendfile_writes_through_a_pipe_transport() -> None:
    loop = running_loop()
    read_end, write_end = os.pipe()
    read_file = os.fdopen(read_end, "rb", buffering=0)
    transport, _ = await loop.connect_write_pipe(asyncio.Protocol, os.fdopen(write_end, "wb", buffering=0))
    try:
        with pytest.raises(asyncio.SendfileNotAvailableError):
            await loop.sendfile(transport, io.BytesIO(PAYLOAD[:10_000]), fallback=False)
        sent = await loop.sendfile(transport, io.BytesIO(PAYLOAD[:10_000]))
        assert sent == 10_000
        transport.close()
        received = await loop.run_in_executor(None, read_file.read)
        assert received == PAYLOAD[:10_000]
    finally:
        transport.close()
        read_file.close()


async def test_sendfile_is_unsupported_for_datagram_transports(payload_file: IO[bytes]) -> None:
    loop = running_loop()
    transport, _ = await loop.create_datagram_endpoint(asyncio.DatagramProtocol, local_addr=("127.0.0.1", 0))
    try:
        with pytest.raises(RuntimeError, match="not supported"):
            await loop.sendfile(cast("asyncio.WriteTransport", transport), payload_file)
    finally:
        transport.close()


async def test_sendfile_surfaces_a_connection_lost_while_draining(payload_file: IO[bytes]) -> None:
    loop = running_loop()
    server, port, sinks = await start_sink(StalledSink)
    protocol = Recorder()
    transport, _ = await loop.create_connection(lambda: protocol, "127.0.0.1", port)
    try:
        # Far more than loopback buffering absorbs, so the backlog outlives the test.
        transport.write(PAYLOAD * 8)
        task = loop.create_task(loop.sendfile(transport, payload_file))
        await asyncio.sleep(0.05)
        assert isinstance(transport, _zuvloop.Transport)
        transport._force_close(RuntimeError("connection went away"))
        with pytest.raises(RuntimeError, match="went away"):
            await task
        await asyncio.sleep(0)
        assert len(protocol.lost) == 1
        assert isinstance(protocol.lost[0], RuntimeError)
    finally:
        for sink in sinks:
            assert sink.transport is not None
            sink.transport.abort()
        server.close()
        await server.wait_closed()


async def test_sendfile_reports_an_abort_while_draining_as_connection_error(payload_file: IO[bytes]) -> None:
    loop = running_loop()
    server, port, sinks = await start_sink(StalledSink)
    transport, _ = await loop.create_connection(asyncio.Protocol, "127.0.0.1", port)
    try:
        transport.write(PAYLOAD * 8)
        task = loop.create_task(loop.sendfile(transport, payload_file))
        await asyncio.sleep(0.05)
        transport.abort()
        with pytest.raises(ConnectionError, match="closed by peer"):
            await task
    finally:
        for sink in sinks:
            assert sink.transport is not None
            sink.transport.abort()
        server.close()
        await server.wait_closed()


async def test_sendfile_cancellation_restores_the_transport(payload_file: IO[bytes]) -> None:
    loop = running_loop()
    server, port, sinks = await start_sink(StalledSink)
    protocol = Recorder()
    transport, _ = await loop.create_connection(lambda: protocol, "127.0.0.1", port)
    try:
        transport.write(PAYLOAD * 8)
        task = loop.create_task(loop.sendfile(transport, payload_file))
        await asyncio.sleep(0.05)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert transport.get_protocol() is protocol
        assert transport.is_reading()
    finally:
        transport.abort()
        for sink in sinks:
            assert sink.transport is not None
            sink.transport.abort()
        server.close()
        await server.wait_closed()


# -- foreign transports and the stand-in protocol ---------------------------


class FakeTransport:
    """The slice of the transport interface the fallback path touches."""

    def __init__(self, close_after: int | None = None) -> None:
        self.written: list[bytes] = []
        self.protocol: asyncio.BaseProtocol = asyncio.Protocol()
        self.close_after = close_after

    def is_closing(self) -> bool:
        return self.close_after is not None and len(self.written) >= self.close_after

    def get_protocol(self) -> asyncio.BaseProtocol:
        return self.protocol

    def set_protocol(self, protocol: asyncio.BaseProtocol) -> None:
        self.protocol = protocol

    def is_reading(self) -> bool:
        return True

    def pause_reading(self) -> None:
        pass

    def resume_reading(self) -> None:
        pass

    def write(self, data: bytes) -> None:
        self.written.append(bytes(data))


class NativeFakeTransport(FakeTransport):
    _sendfile_compatible = _SendfileMode.TRY_NATIVE


class FallbackFakeTransport(FakeTransport):
    _sendfile_compatible = _SendfileMode.FALLBACK


class PausedFallbackFakeTransport(FallbackFakeTransport):
    _protocol_paused = True


async def test_sendfile_on_a_foreign_transport_without_support(payload_file: IO[bytes]) -> None:
    loop = running_loop()
    with pytest.raises(RuntimeError, match="not supported"):
        await loop.sendfile(cast("asyncio.WriteTransport", FakeTransport()), payload_file)


async def test_sendfile_on_a_foreign_transport_claiming_native_support() -> None:
    loop = running_loop()
    fake = NativeFakeTransport()
    with pytest.raises(asyncio.SendfileNotAvailableError):
        await loop.sendfile(cast("asyncio.WriteTransport", fake), io.BytesIO(b"data"), fallback=False)
    sent = await loop.sendfile(cast("asyncio.WriteTransport", fake), io.BytesIO(PAYLOAD[:50_000]))
    assert sent == 50_000
    assert b"".join(fake.written) == PAYLOAD[:50_000]
    unseekable = NativeFakeTransport()
    source = cast("IO[bytes]", SeeklessFile(PAYLOAD[:20_000]))
    sent = await loop.sendfile(cast("asyncio.WriteTransport", unseekable), source)
    assert sent == 20_000
    assert b"".join(unseekable.written) == PAYLOAD[:20_000]


async def test_sendfile_on_a_foreign_transport_with_fallback_disabled(payload_file: IO[bytes]) -> None:
    loop = running_loop()
    with pytest.raises(RuntimeError, match="fallback is disabled"):
        await loop.sendfile(cast("asyncio.WriteTransport", FallbackFakeTransport()), payload_file, fallback=False)


async def test_sendfile_leaves_a_still_paused_protocol_paused() -> None:
    """A transport over its high-water mark delivers its own resume later;
    replaying one at restore would invite writes into the backlog."""
    loop = running_loop()
    fake = PausedFallbackFakeTransport()
    recorder = Recorder()
    fake.protocol = recorder
    assert await loop.sendfile(cast("asyncio.WriteTransport", fake), io.BytesIO()) == 0
    assert fake.protocol is recorder
    assert recorder.resumed == 0


async def test_sendfile_stops_when_the_transport_closes_mid_transfer() -> None:
    loop = running_loop()
    fake = FallbackFakeTransport(close_after=1)
    with pytest.raises(ConnectionError, match="closed by peer"):
        await loop.sendfile(cast("asyncio.WriteTransport", fake), io.BytesIO(PAYLOAD[:100_000]))


async def test_sendfile_protocol_guards_its_invalid_states() -> None:
    loop = running_loop()
    fake = cast("asyncio.WriteTransport", FakeTransport())
    stand_in = _SendfileProtocol(loop, fake, paused=False)
    with pytest.raises(RuntimeError, match="established"):
        stand_in.connection_made(fake)
    with pytest.raises(RuntimeError, match="paused"):
        stand_in.data_received(b"data")
    with pytest.raises(RuntimeError, match="paused"):
        stand_in.eof_received()
    stand_in.pause_writing()
    stand_in.pause_writing()
    stand_in.resume_writing()
    stand_in.resume_writing()
    stand_in.connection_lost(None)
    stand_in.restore()


async def test_sendfile_protocol_wakes_its_waiter_on_connection_lost() -> None:
    loop = running_loop()
    fake = FakeTransport()
    stand_in = _SendfileProtocol(loop, cast("asyncio.WriteTransport", fake), paused=True)
    stand_in.connection_lost(RuntimeError("gone"))
    with pytest.raises(RuntimeError, match="gone"):
        await stand_in.drain()
    stand_in.connection_lost(RuntimeError("late duplicate"))
    stand_in.restore()
    assert fake.protocol is not stand_in
