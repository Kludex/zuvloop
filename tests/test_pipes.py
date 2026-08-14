from __future__ import annotations

import asyncio
import os
import socket
from pathlib import Path
from typing import IO

import pytest

import zuvloop
from tests.conftest import running_loop

pytestmark = pytest.mark.anyio


class Reader(asyncio.Protocol):
    """Read-pipe protocol that resolves once the writer goes away."""

    def __init__(self) -> None:
        self.data = bytearray()
        self.done: asyncio.Future[None] | None = None

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        self.done = running_loop().create_future()

    def data_received(self, data: bytes) -> None:
        self.data += data

    def eof_received(self) -> bool:
        self._finish()
        return False

    def connection_lost(self, exc: BaseException | None) -> None:
        self._finish()

    def _finish(self) -> None:
        if self.done is not None and not self.done.done():
            self.done.set_result(None)


async def make_pipe() -> tuple[IO[bytes], IO[bytes]]:
    read_fd, write_fd = os.pipe()
    return open(read_fd, "rb", 0), open(write_fd, "wb", 0)


async def test_a_pipe_carries_data_between_transports() -> None:
    loop = running_loop()
    reader_file, writer_file = await make_pipe()
    read_transport, reader = await loop.connect_read_pipe(Reader, reader_file)
    write_transport, _writer = await loop.connect_write_pipe(asyncio.BaseProtocol, writer_file)
    try:
        write_transport.write(b"through the pipe")
        write_transport.close()
        assert reader.done is not None
        await asyncio.wait_for(reader.done, 2)
        assert bytes(reader.data) == b"through the pipe"
    finally:
        read_transport.close()


async def test_pipe_transports_report_their_pipe() -> None:
    loop = running_loop()
    reader_file, writer_file = await make_pipe()
    read_transport, _reader = await loop.connect_read_pipe(Reader, reader_file)
    write_transport, _writer = await loop.connect_write_pipe(asyncio.BaseProtocol, writer_file)
    try:
        assert read_transport.get_extra_info("pipe") is reader_file
        assert write_transport.get_extra_info("pipe") is writer_file
        assert isinstance(read_transport, asyncio.ReadTransport)
        assert isinstance(write_transport, asyncio.WriteTransport)
    finally:
        write_transport.close()
        read_transport.close()


async def test_closing_a_transport_closes_its_pipe() -> None:
    loop = running_loop()
    reader_file, writer_file = await make_pipe()
    read_transport, _reader = await loop.connect_read_pipe(Reader, reader_file)
    write_transport, _writer = await loop.connect_write_pipe(asyncio.BaseProtocol, writer_file)

    read_transport.close()
    write_transport.close()

    async with asyncio.timeout(2):
        while not (reader_file.closed and writer_file.closed):  # pragma: no cover - close may be synchronous
            await asyncio.sleep(0.01)


async def test_a_regular_file_is_rejected() -> None:
    loop = running_loop()
    with open(__file__, "rb") as handle:
        with pytest.raises(ValueError, match="pipes, sockets and character devices"):
            await loop.connect_read_pipe(Reader, handle)


async def test_a_character_device_is_accepted() -> None:
    loop = running_loop()
    with open(os.devnull, "wb", 0) as handle:
        transport, _protocol = await loop.connect_write_pipe(asyncio.BaseProtocol, handle)
        try:
            transport.write(b"discarded")
        finally:
            transport.close()


async def test_a_socket_pair_is_accepted() -> None:
    loop = running_loop()
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    read_transport, reader = await loop.connect_read_pipe(Reader, left)
    try:
        right.sendall(b"over a socket")

        async def until_delivered() -> None:
            while not reader.data:
                await asyncio.sleep(0.01)

        await asyncio.wait_for(until_delivered(), 2)
        assert bytes(reader.data) == b"over a socket"
    finally:
        read_transport.close()
        right.close()


async def test_a_write_pipe_never_reads_from_its_descriptor() -> None:
    loop = running_loop()
    reader_file, writer_file = await make_pipe()
    read_transport, reader = await loop.connect_read_pipe(Reader, reader_file)
    write_transport, _writer = await loop.connect_write_pipe(asyncio.BaseProtocol, writer_file)
    try:
        # A write-only descriptor cannot be read; starting a read on one would
        # fail the transport rather than simply deliver nothing.
        assert write_transport.is_closing() is False
        write_transport.write(b"still alive")
        await asyncio.sleep(0.05)
        assert bytes(reader.data) == b"still alive"
    finally:
        write_transport.close()
        read_transport.close()


async def test_a_large_write_drains_through_the_pipe() -> None:
    loop = running_loop()
    reader_file, writer_file = await make_pipe()
    read_transport, reader = await loop.connect_read_pipe(Reader, reader_file)
    write_transport, _writer = await loop.connect_write_pipe(asyncio.BaseProtocol, writer_file)
    payload = os.urandom(1 << 20)
    try:
        write_transport.write(payload)
        write_transport.close()
        assert reader.done is not None
        await asyncio.wait_for(reader.done, 5)
        assert bytes(reader.data) == payload
    finally:
        read_transport.close()


async def test_a_pipe_that_cannot_be_adopted_releases_its_descriptor(monkeypatch: pytest.MonkeyPatch) -> None:
    loop = running_loop()
    reader_file, writer_file = await make_pipe()

    def refuse(*args: object, **kwargs: object) -> zuvloop.Transport:
        raise RuntimeError("refused")

    monkeypatch.setattr(type(loop), "_make_transport", refuse)
    try:
        with pytest.raises(RuntimeError, match="refused"):
            await loop.connect_read_pipe(Reader, reader_file)
    finally:
        reader_file.close()
        writer_file.close()


async def test_a_cancelled_pipe_setup_closes_the_transport() -> None:
    loop = running_loop()
    reader_file, writer_file = await make_pipe()
    task = asyncio.ensure_future(loop.connect_read_pipe(Reader, reader_file))
    # One turn is enough to reach the waiter: everything before it is synchronous,
    # and the handle that resolves it only runs on the turn after this one.
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    await asyncio.sleep(0.05)
    assert reader_file.closed
    writer_file.close()


@pytest.mark.anyio(None)
def test_a_write_from_a_stopped_loop_reaches_the_pipe(loop: zuvloop.EventLoop) -> None:
    """Writes batch across a callback, but a stopped loop has no turn left to flush.

    CPython's `test_bidirectional_pty` writes here and then blocks reading the
    peer, which never returns if the batch is still waiting on an iteration.
    """
    read_fd, write_fd = os.pipe()
    os.set_blocking(read_fd, False)
    reader_file, writer_file = open(read_fd, "rb", 0), open(write_fd, "wb", 0)
    transport, _protocol = loop.run_until_complete(loop.connect_write_pipe(asyncio.BaseProtocol, writer_file))
    try:
        transport.write(b"before the next turn")
        assert os.read(read_fd, 64) == b"before the next turn"
    finally:
        transport.close()
        reader_file.close()


async def test_a_write_pipe_notices_the_reader_going_away() -> None:
    loop = running_loop()
    reader_file, writer_file = await make_pipe()
    lost = loop.create_future()

    class Writer(asyncio.BaseProtocol):
        def connection_lost(self, exc: BaseException | None) -> None:
            lost.set_result(exc)

    transport, _writer = await loop.connect_write_pipe(Writer, writer_file)
    try:
        reader_file.close()
        assert await asyncio.wait_for(lost, 2) is None
    finally:
        transport.close()


async def test_unread_data_does_not_close_a_named_fifo(tmp_path: Path) -> None:
    loop = running_loop()
    path = tmp_path / "named-fifo"
    os.mkfifo(path)
    read_fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK)
    try:
        with open(path, "wb", buffering=0) as writer_file:
            transport, _writer = await loop.connect_write_pipe(asyncio.BaseProtocol, writer_file)
            try:
                transport.write(b"unread")
                for _ in range(10):
                    await asyncio.sleep(0)
                assert not transport.is_closing()
                assert os.read(read_fd, 64) == b"unread"
            finally:
                transport.close()
    finally:
        os.close(read_fd)


async def test_an_undelivered_write_ends_the_write_pipe_with_a_broken_pipe() -> None:
    loop = running_loop()
    reader_file, writer_file = await make_pipe()
    lost = loop.create_future()

    class Writer(asyncio.BaseProtocol):
        def connection_lost(self, exc: BaseException | None) -> None:
            lost.set_result(exc)

    transport, _writer = await loop.connect_write_pipe(Writer, writer_file)
    try:
        # More than the pipe can hold, so the tail is still queued at the hangup.
        transport.write(os.urandom(4 << 20))
        reader_file.close()
        assert isinstance(await asyncio.wait_for(lost, 2), BrokenPipeError)
    finally:
        transport.close()
