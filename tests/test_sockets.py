from __future__ import annotations

import asyncio
import socket
import sys

import pytest

from conftest import running_loop

pytestmark = pytest.mark.anyio
requires_unix_sockets = pytest.mark.skipif(sys.platform == "win32", reason="Windows has no Unix sockets")


async def test_sock_recv_and_sendall() -> None:
    loop = running_loop()
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    try:
        await loop.sock_sendall(left, b"payload")
        assert await loop.sock_recv(right, 7) == b"payload"
    finally:
        left.close()
        right.close()


async def test_sock_sendall_splits_large_payloads() -> None:
    loop = running_loop()
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    payload = b"z" * (1 << 20)
    try:
        sending = loop.create_task(loop.sock_sendall(left, payload))
        received = bytearray()
        while len(received) < len(payload):
            received += await loop.sock_recv(right, 65536)
        await sending
        assert bytes(received) == payload
    finally:
        left.close()
        right.close()


async def test_sock_recv_waits_for_data() -> None:
    loop = running_loop()
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    try:
        pending = loop.create_task(loop.sock_recv(right, 16))
        await asyncio.sleep(0.02)
        assert not pending.done()
        await loop.sock_sendall(left, b"late")
        assert await pending == b"late"
    finally:
        left.close()
        right.close()


async def test_sock_recv_into() -> None:
    loop = running_loop()
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    try:
        await loop.sock_sendall(left, b"buffered")
        buffer = bytearray(16)
        count = await loop.sock_recv_into(right, buffer)
        assert buffer[:count] == b"buffered"
    finally:
        left.close()
        right.close()


async def test_a_retried_operation_reports_errors() -> None:
    """The peer disappears while the send is parked waiting for writability."""
    loop = running_loop()
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    sending = loop.create_task(loop.sock_sendall(left, b"x" * (8 << 20)))
    await asyncio.sleep(0.02)
    right.close()
    with pytest.raises(OSError):
        await sending
    left.close()


async def test_sock_accept_and_connect() -> None:
    loop = running_loop()
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.setblocking(False)
    port = listener.getsockname()[1]

    client = socket.socket()
    client.setblocking(False)
    try:
        connecting = loop.create_task(loop.sock_connect(client, ("127.0.0.1", port)))
        conn, address = await loop.sock_accept(listener)
        await connecting
        assert address[0] == "127.0.0.1"
        await loop.sock_sendall(client, b"handshake")
        assert await loop.sock_recv(conn, 9) == b"handshake"
        conn.close()
    finally:
        client.close()
        listener.close()


async def test_sock_connect_reports_refusal(closed_port: int) -> None:
    loop = running_loop()
    sock = socket.socket()
    sock.setblocking(False)
    try:
        with pytest.raises(OSError):
            await loop.sock_connect(sock, ("127.0.0.1", closed_port))
    finally:
        sock.close()


@requires_unix_sockets
async def test_sock_connect_to_a_unix_path_needs_no_resolution() -> None:
    import tempfile
    from pathlib import Path

    loop = running_loop()
    with tempfile.TemporaryDirectory() as directory:
        path = str(Path(directory) / "direct.sock")
        listener = socket.socket(socket.AF_UNIX)
        listener.bind(path)
        listener.listen(1)
        listener.setblocking(False)
        client = socket.socket(socket.AF_UNIX)
        client.setblocking(False)
        try:
            await loop.sock_connect(client, path)
            conn, _address = await loop.sock_accept(listener)
            conn.close()
        finally:
            client.close()
            listener.close()


async def test_sock_operations_reject_blocking_sockets() -> None:
    loop = running_loop()
    with socket.socket() as sock:
        with pytest.raises(ValueError, match="must be non-blocking"):
            await loop.sock_recv(sock, 1)
        with pytest.raises(ValueError, match="must be non-blocking"):
            await loop.sock_connect(sock, ("127.0.0.1", 1))


async def test_sock_sendto_and_recvfrom() -> None:
    loop = running_loop()
    left = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    right = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    left.bind(("127.0.0.1", 0))
    right.bind(("127.0.0.1", 0))
    left.setblocking(False)
    right.setblocking(False)
    try:
        await loop.sock_sendto(left, b"datagram", right.getsockname())
        data, address = await loop.sock_recvfrom(right, 32)
        assert data == b"datagram"
        assert address == left.getsockname()

        await loop.sock_sendto(left, b"into", right.getsockname())
        buffer = bytearray(32)
        count, _address = await loop.sock_recvfrom_into(right, buffer)
        assert buffer[:count] == b"into"
    finally:
        left.close()
        right.close()


async def test_readers_and_writers_can_be_replaced() -> None:
    loop = running_loop()
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    seen: list[str] = []
    try:
        loop.add_reader(right.fileno(), seen.append, "first")
        loop.add_reader(right.fileno(), seen.append, "second")
        left.send(b"!")
        await asyncio.sleep(0.05)
        # Watching is level-triggered, so the surviving callback fires until the
        # data is read; only the replacement should ever have run.
        assert seen and set(seen) == {"second"}
        assert loop.remove_reader(right.fileno()) is True
        assert loop.remove_reader(right.fileno()) is False
    finally:
        left.close()
        right.close()


async def test_writers_fire_when_writable() -> None:
    loop = running_loop()
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    done = loop.create_future()
    try:
        loop.add_writer(left.fileno(), done.set_result, "writable")
        assert await done == "writable"
        assert loop.remove_writer(left.fileno()) is True
        assert loop.remove_writer(left.fileno()) is False
    finally:
        left.close()
        right.close()


async def test_readers_and_writers_share_a_descriptor() -> None:
    loop = running_loop()
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    reads: list[str] = []
    writes: list[str] = []
    try:
        loop.add_reader(left.fileno(), reads.append, "readable")
        loop.add_writer(left.fileno(), writes.append, "writable")
        right.send(b"data")
        await asyncio.sleep(0.05)
        assert reads and writes
        assert loop.remove_writer(left.fileno()) is True
        await asyncio.sleep(0.02)
        assert loop.remove_reader(left.fileno()) is True
    finally:
        left.close()
        right.close()


async def test_add_reader_validates_its_arguments() -> None:
    loop = running_loop()
    with pytest.raises(TypeError, match="descriptor and a callback"):
        loop.add_reader(0)  # type: ignore[call-arg]
    with pytest.raises(TypeError, match="descriptor and a callback"):
        loop.add_writer(0)  # type: ignore[call-arg]


async def test_add_reader_rejects_an_invalid_descriptor() -> None:
    loop = running_loop()
    with pytest.raises(ValueError, match="negative"):
        loop.add_reader(-1, print)
    with pytest.raises(OSError):
        loop.add_reader(9999, print)


async def test_remove_reader_for_an_unwatched_descriptor() -> None:
    loop = running_loop()
    assert loop.remove_reader(0) is False
    assert loop.remove_writer(0) is False


async def test_sendfile_is_not_implemented() -> None:
    loop = running_loop()
    with socket.socket() as sock:
        with pytest.raises(NotImplementedError):
            await loop.sock_sendfile(sock, None)
    with pytest.raises(NotImplementedError):
        await loop.sendfile(None, None)


async def test_a_partially_drained_descriptor_reports_ready_again() -> None:
    """Reading one byte at a time leaves the descriptor readable between wakeups."""
    loop = running_loop()
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    try:
        pending = loop.create_task(loop.sock_recv_into(right, bytearray(1)))
        await asyncio.sleep(0.02)
        await loop.sock_sendall(left, b"many bytes waiting")
        assert await pending == 1
        assert await loop.sock_recv(right, 32) == b"any bytes waiting"
    finally:
        left.close()
        right.close()


async def test_a_cancelled_read_leaves_the_next_one_watching() -> None:
    """Cleanup runs a turn late, by which point the descriptor has a new owner.

    Only one watcher exists per direction, so unregistering blindly would strand
    the operation that replaced it - CPython's `test_sock_client_racing` covers
    the same ground.
    """
    loop = running_loop()
    left, right = socket.socketpair()
    left.setblocking(False)
    try:
        first = asyncio.ensure_future(loop.sock_recv(left, 16))
        await asyncio.sleep(0)
        # Supersedes the first registration: the descriptor holds one reader.
        second = asyncio.ensure_future(loop.sock_recv(left, 16))
        await asyncio.sleep(0)

        right.sendall(b"delivered")
        assert await asyncio.wait_for(second, 2) == b"delivered"

        # The first now unwinds onto a descriptor it no longer owns.
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first

        right.sendall(b"and again")
        assert await asyncio.wait_for(loop.sock_recv(left, 16), 2) == b"and again"
    finally:
        left.close()
        right.close()


async def test_replacing_a_watcher_survives_a_reentrant_finaliser() -> None:
    """The slot has to name the new watcher before the old one is released.

    Releasing a watcher runs its arguments' finalisers, and a finaliser is free to
    call `remove_reader` for the same descriptor. If it finds the slot still
    naming the handle being destroyed, it releases it a second time and closes a
    poll handle the replacement is about to be armed on.
    """
    loop = running_loop()
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    fd = right.fileno()
    reentered: list[bool] = []

    class Reentrant:
        def __del__(self) -> None:
            reentered.append(True)
            loop.remove_reader(fd)

    try:
        loop.add_reader(fd, lambda _held: None, Reentrant())
        loop.add_reader(fd, lambda: None)
        assert reentered == [True]

        # The descriptor has to remain watchable: an orphaned poll handle makes
        # the next registration fail with EEXIST.
        seen: list[str] = []
        loop.remove_reader(fd)
        loop.add_reader(fd, seen.append, "after")
        left.send(b"!")
        await asyncio.sleep(0.05)
        # That it fires at all is the point: an orphaned poll handle would have
        # made the registration above fail. Level-triggered, so it fires until
        # the data is read.
        assert seen and set(seen) == {"after"}
        assert loop.remove_reader(fd) is True
    finally:
        # An assertion above leaves the watcher armed, and closing a descriptor
        # libuv is still polling is its own kind of trouble.
        loop.remove_reader(fd)
        left.close()
        right.close()
