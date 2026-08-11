from __future__ import annotations

import asyncio
import errno
import os
import socket
import ssl
from asyncio.constants import _SendfileMode
from collections.abc import Callable
from typing import IO, cast

from . import _zuvloop
from ._sockets import SocketOperations, _attempt, _check_non_blocking

# CPython's fallback read sizes: large reads straight to a socket, smaller ones
# through a transport so flow control gets a say between chunks.
_SOCKET_CHUNK = 256 * 1024
_TRANSPORT_CHUNK = 16 * 1024


class SendfileOperations(SocketOperations):
    """`sendfile()` and `sock_sendfile()`: `os.sendfile` straight to the
    descriptor when the target is a plain socket, chunked writes when it is
    behind TLS or not a regular file."""

    async def sock_sendfile(  # type: ignore[override]  # typeshed allows fallback=None
        self, sock: socket.socket, file: IO[bytes], offset: int = 0, count: int | None = None, *, fallback: bool = True
    ) -> int:
        _check_non_blocking(sock)
        _check_sendfile_params(sock, file, offset, count)
        try:
            return await self._sendfile_to_fd(sock.fileno(), file, offset, count)
        except asyncio.SendfileNotAvailableError:
            if not fallback:
                raise
        return await self._sock_sendfile_fallback(sock, file, offset, count)

    async def sendfile(
        self,
        transport: asyncio.WriteTransport,
        file: IO[bytes],
        offset: int = 0,
        count: int | None = None,
        *,
        fallback: bool = True,
    ) -> int:
        if transport.is_closing():
            raise RuntimeError("Transport is closing")
        _check_file_params(file, offset, count)
        if isinstance(transport, _zuvloop.Transport):
            sock = transport.get_extra_info("socket")
            if sock is not None:
                try:
                    return await self._transport_sendfile_native(transport, sock, file, offset, count)
                except asyncio.SendfileNotAvailableError:
                    if not fallback:
                        raise
            elif not fallback:
                raise asyncio.SendfileNotAvailableError(f"native sendfile is not supported for transport {transport!r}")
            return await self._sendfile_fallback(transport, file, offset, count)
        mode = getattr(transport, "_sendfile_compatible", _SendfileMode.UNSUPPORTED)
        if mode == _SendfileMode.UNSUPPORTED:
            raise RuntimeError(f"sendfile is not supported for transport {transport!r}")
        if not fallback and mode != _SendfileMode.TRY_NATIVE:
            raise RuntimeError(f"fallback is disabled and native sendfile is not supported for transport {transport!r}")
        if not fallback:
            raise asyncio.SendfileNotAvailableError(f"native sendfile is not supported for transport {transport!r}")
        return await self._sendfile_fallback(transport, file, offset, count)

    async def _transport_sendfile_native(
        self, transport: _zuvloop.Transport, sock: socket.socket, file: IO[bytes], offset: int, count: int | None
    ) -> int:
        # Reject non-regular files before disturbing the transport at all.
        fileno = _regular_fileno(file)
        waiter = _SendfileProtocol(self, transport, transport._protocol_paused)
        low, high = transport.get_write_buffer_limits()
        # With both marks at zero, `resume_writing` fires exactly when libuv has
        # handed the last buffered byte to the socket - the point after which
        # writing to the descriptor directly cannot reorder data.
        transport.set_write_buffer_limits(high=0, low=0)
        try:
            if transport.get_write_buffer_size() > 0:
                await waiter.drain()
            # libuv allows one handle per descriptor, so readiness is polled
            # through a duplicate; both name the same socket.
            fd = os.dup(sock.fileno())
            try:
                return await self._sendfile_to_fd(fd, file, offset, count, fileno)
            finally:
                os.close(fd)
        finally:
            transport.set_write_buffer_limits(high=high, low=low)
            waiter.restore()

    async def _sendfile_to_fd(
        self, fd: int, file: IO[bytes], offset: int, count: int | None, fileno: int | None = None
    ) -> int:
        if fileno is None:
            fileno = _regular_fileno(file)
        blocksize = count if count is not None else os.fstat(fileno).st_size
        total_sent = 0
        try:
            while blocksize > 0:
                try:
                    sent = await self._retry_until_writable(fd, os.sendfile, fd, fileno, offset, blocksize)
                except OSError as exc:
                    if total_sent == 0:
                        # The main reason to get here is `file` not being a
                        # regular mmap-able file; the fallback's plain writes
                        # will either work or report the real failure.
                        raise asyncio.SendfileNotAvailableError("os.sendfile call failed") from exc
                    if exc.errno == errno.ENOTCONN and not isinstance(exc, ConnectionError):
                        # A disconnect surfaces as ENOTCONN on some platforms;
                        # every caller should see it as the same failure.
                        raise ConnectionError("socket is not connected", errno.ENOTCONN) from exc
                    raise
                if sent == 0:
                    break
                offset += sent
                total_sent += sent
                if count is not None:
                    blocksize = count - total_sent
            return total_sent
        finally:
            if total_sent > 0:
                os.lseek(fileno, offset, os.SEEK_SET)

    async def _retry_until_writable(self, fd: int, op: Callable[..., int], *args: int) -> int:
        future = self.create_future()
        if _attempt(future, op, args):
            return cast("int", await future)
        token = self._watch(fd, True, _attempt, future, op, args)
        try:
            return cast("int", await future)
        finally:
            self._unwatch(fd, True, token)

    async def _sock_sendfile_fallback(
        self, sock: socket.socket, file: IO[bytes], offset: int, count: int | None
    ) -> int:
        if offset:
            file.seek(offset)
        blocksize = min(count, _SOCKET_CHUNK) if count is not None else _SOCKET_CHUNK
        total_sent = 0
        try:
            while blocksize > 0:
                data = await self.run_in_executor(None, file.read, blocksize)
                if not data:
                    break
                await self.sock_sendall(sock, data)
                total_sent += len(data)
                if count is not None:
                    blocksize = min(count - total_sent, _SOCKET_CHUNK)
            return total_sent
        finally:
            if total_sent > 0 and hasattr(file, "seek"):
                file.seek(offset + total_sent)

    async def _sendfile_fallback(
        self, transport: asyncio.WriteTransport, file: IO[bytes], offset: int, count: int | None
    ) -> int:
        if offset:
            file.seek(offset)
        blocksize = min(count, _TRANSPORT_CHUNK) if count is not None else _TRANSPORT_CHUNK
        total_sent = 0
        waiter = _SendfileProtocol(self, transport, bool(getattr(transport, "_protocol_paused", False)))
        try:
            while blocksize > 0:
                data = await self.run_in_executor(None, file.read, blocksize)
                if not data:
                    break
                await waiter.drain()
                transport.write(data)
                total_sent += len(data)
                if count is not None:
                    blocksize = min(count - total_sent, _TRANSPORT_CHUNK)
            return total_sent
        finally:
            if total_sent > 0 and hasattr(file, "seek"):
                file.seek(offset + total_sent)
            waiter.restore()


class _SendfileProtocol(asyncio.Protocol):
    """Stands in for the transport's protocol while `sendfile()` owns the stream.

    Reading pauses for the duration; writability is answered through the flow
    control callbacks the transport already makes. `restore()` reinstates the
    real protocol and replays the flow-control state it last saw.
    """

    def __init__(self, loop: SendfileOperations, transport: asyncio.WriteTransport, paused: bool) -> None:
        self._loop = loop
        self._transport = cast("asyncio.Transport", transport)
        self._protocol = self._transport.get_protocol()
        self._should_resume_reading = self._transport.is_reading()
        self._should_resume_writing = paused
        self._waiter: asyncio.Future[None] | None = loop.create_future() if paused else None
        self._transport.pause_reading()
        self._transport.set_protocol(self)

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        raise RuntimeError("Invalid state: connection should have been established already")

    def connection_lost(self, exc: Exception | None) -> None:
        if self._waiter is not None and not self._waiter.done():
            self._waiter.set_exception(exc if exc is not None else ConnectionError("Connection is closed by peer"))
        self._protocol.connection_lost(exc)

    def data_received(self, data: bytes) -> None:
        raise RuntimeError("Invalid state: reading should be paused")

    def eof_received(self) -> bool:
        raise RuntimeError("Invalid state: reading should be paused")

    def pause_writing(self) -> None:
        if self._waiter is None:
            self._waiter = self._loop.create_future()

    def resume_writing(self) -> None:
        if self._waiter is not None:
            self._waiter.set_result(None)
            self._waiter = None

    async def drain(self) -> None:
        if self._transport.is_closing():
            raise ConnectionError("Connection closed by peer")
        if self._waiter is not None:
            await self._waiter

    def restore(self) -> None:
        self._transport.set_protocol(self._protocol)
        if self._should_resume_reading:
            self._transport.resume_reading()
        if self._waiter is not None:
            if not self._waiter.done():
                self._waiter.cancel()
            elif not self._waiter.cancelled():
                # A failure connection_lost() recorded that nobody awaited
                # would otherwise be reported as an unretrieved exception.
                self._waiter.exception()
            self._waiter = None
        # A transport still over its high-water mark delivers its own
        # resume_writing() once it drains; replaying one now would be an
        # invitation to write into the backlog.
        if self._should_resume_writing and not getattr(self._transport, "_protocol_paused", False):
            self._protocol.resume_writing()


def _check_sendfile_params(sock: socket.socket, file: IO[bytes], offset: int, count: int | None) -> None:
    if isinstance(sock, ssl.SSLSocket):
        raise TypeError("Socket cannot be of type SSLSocket")
    if sock.type != socket.SOCK_STREAM:
        raise ValueError("only SOCK_STREAM type sockets are supported")
    _check_file_params(file, offset, count)


def _check_file_params(file: IO[bytes], offset: int, count: int | None) -> None:
    if "b" not in getattr(file, "mode", "b"):
        raise ValueError("file should be opened in binary mode")
    if count is not None:
        if not isinstance(count, int):
            raise TypeError(f"count must be a positive integer (got {count!r})")
        if count <= 0:
            raise ValueError(f"count must be a positive integer (got {count!r})")
    if not isinstance(offset, int):
        raise TypeError(f"offset must be a non-negative integer (got {offset!r})")
    if offset < 0:
        raise ValueError(f"offset must be a non-negative integer (got {offset!r})")


def _regular_fileno(file: IO[bytes]) -> int:
    """The descriptor `os.sendfile` can read, or `SendfileNotAvailableError`."""
    try:
        fileno = file.fileno()
        os.fstat(fileno)
    except (AttributeError, OSError) as exc:
        raise asyncio.SendfileNotAvailableError("not a regular file") from exc
    return fileno
