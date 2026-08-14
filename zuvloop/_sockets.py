from __future__ import annotations

import asyncio
import socket
from collections.abc import Buffer, Callable

from ._base import LoopBase


class SocketOperations(LoopBase):
    """`sock_*` helpers, layered on the native reader/writer registrations."""

    def _watch(self, fd: int, write: bool, callback: Callable[[], object]) -> object:
        """Register a reader or writer, tagged so its owner can recognise it later."""
        token = object()
        self._sock_watchers[fd, write] = token
        (self.add_writer if write else self.add_reader)(fd, callback)
        return token

    def _unwatch(self, fd: int, write: bool, token: object) -> None:
        """Unregister only if the watcher on `fd` is still the one `token` installed.

        A cancelled operation runs its cleanup a turn late, by which time the next
        operation on the same descriptor has replaced the registration - only one
        watcher exists per direction. Removing blindly would strand that one.
        """
        if self._sock_watchers.get((fd, write)) is not token:
            return
        del self._sock_watchers[fd, write]
        (self.remove_writer if write else self.remove_reader)(fd)

    async def _retry_until_ready[T](self, sock: socket.socket, op: Callable[[], T], *, write: bool = False) -> T:
        """Run `op`, retrying it each time `sock` reports itself ready."""
        self._check_non_blocking(sock)
        return await self._retry_ready(sock.fileno(), write, op)

    async def _retry_ready[T](self, fd: int, write: bool, op: Callable[[], T]) -> T:
        future: asyncio.Future[T] = self.create_future()
        if _attempt(future, op):
            return await future
        token = self._watch(fd, write, lambda: _attempt(future, op))
        try:
            return await future
        finally:
            self._unwatch(fd, write, token)

    def _check_non_blocking(self, sock: socket.socket) -> None:
        if sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")

    async def sock_recv(self, sock: socket.socket, nbytes: int) -> bytes:
        return await self._retry_until_ready(sock, lambda: sock.recv(nbytes))

    async def sock_recv_into(self, sock: socket.socket, buf: Buffer) -> int:
        return await self._retry_until_ready(sock, lambda: sock.recv_into(buf))

    async def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> tuple[bytes, _SocketAddress]:
        return await self._retry_until_ready(sock, lambda: sock.recvfrom(bufsize))

    async def sock_recvfrom_into(self, sock: socket.socket, buf: Buffer, nbytes: int = 0) -> tuple[int, _SocketAddress]:
        return await self._retry_until_ready(sock, lambda: sock.recvfrom_into(buf, nbytes))

    async def sock_sendto(self, sock: socket.socket, data: Buffer, address: _SocketAddress) -> int:
        # Resolve `sendto` on every attempt so test doubles and instrumentors can
        # replace it while the descriptor is waiting to become writable.
        return await self._retry_until_ready(sock, lambda: sock.sendto(data, address), write=True)

    async def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, _SocketAddress]:
        conn, address = await self._retry_until_ready(sock, sock.accept)
        conn.setblocking(False)
        return conn, address

    async def sock_sendall(self, sock: socket.socket, data: Buffer) -> None:
        view = memoryview(data).cast("B")
        sent = 0
        while sent < len(view):
            sent += await self._retry_until_ready(sock, lambda: sock.send(view[sent:]), write=True)

    async def sock_connect(self, sock: socket.socket, address: _SocketAddress) -> None:
        self._check_non_blocking(sock)
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            if not isinstance(address, tuple) or len(address) < 2:
                raise TypeError("an internet socket address must be a tuple")
            host, port = address[:2]
            if not isinstance(host, (str, bytes)) or not isinstance(port, (str, bytes, int)):
                raise TypeError("an internet socket address must contain a host and port")
            resolved = await self.getaddrinfo(host, port, family=sock.family, type=sock.type, proto=sock.proto)
            address = resolved[0][4]
        try:
            sock.connect(address)
            return
        except BlockingIOError, InterruptedError:
            pass

        future = self.create_future()
        fd = sock.fileno()

        def check() -> None:
            if future.done():  # pragma: no cover - guards a wakeup the tests cannot force
                return
            err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err:
                future.set_exception(OSError(err, f"Connect call failed {address!r}"))
            else:
                future.set_result(None)

        token = self._watch(fd, True, check)
        try:
            await future
        finally:
            self._unwatch(fd, True, token)


def _attempt[T](future: asyncio.Future[T], op: Callable[[], T]) -> bool:
    """Settle `future` from one attempt at `op`; False means "not ready yet"."""
    if future.done():  # pragma: no cover - guards a wakeup the tests cannot force
        # Watching is level-triggered, so a partially drained descriptor could
        # report ready again before the awaiting coroutine resumes.
        return True
    try:
        result = op()
    except BlockingIOError, InterruptedError:
        return False
    except OSError as exc:
        future.set_exception(exc)
    else:
        future.set_result(result)
    return True


type _SocketAddress = tuple[str | bytes | int, ...] | str | Buffer | int
