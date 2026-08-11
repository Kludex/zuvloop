from __future__ import annotations

import asyncio
import socket
from collections.abc import Callable
from typing import Any

from ._base import LoopBase


class SocketOperations(LoopBase):
    """`sock_*` helpers, layered on the native reader/writer registrations."""

    def _watch(self, fd: int, write: bool, callback: Callable[..., Any], *args: Any) -> object:
        """Register a reader or writer, tagged so its owner can recognise it later."""
        token = object()
        self._sock_watchers[fd, write] = token
        (self.add_writer if write else self.add_reader)(fd, callback, *args)
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

    async def _retry_until_ready(
        self, sock: socket.socket, op: Callable[..., Any], *args: Any, write: bool = False
    ) -> Any:
        """Run `op`, retrying it each time `sock` reports itself ready."""
        _check_non_blocking(sock)
        future = self.create_future()
        if _attempt(future, op, args):
            return await future

        fd = sock.fileno()
        token = self._watch(fd, write, _attempt, future, op, args)
        try:
            return await future
        finally:
            self._unwatch(fd, write, token)

    async def sock_recv(self, sock: socket.socket, nbytes: int) -> bytes:
        return await self._retry_until_ready(sock, sock.recv, nbytes)  # type: ignore[no-any-return]

    async def sock_recv_into(self, sock: socket.socket, buf: Any) -> int:
        return await self._retry_until_ready(sock, sock.recv_into, buf)  # type: ignore[no-any-return]

    async def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> tuple[bytes, Any]:
        return await self._retry_until_ready(sock, sock.recvfrom, bufsize)  # type: ignore[no-any-return]

    async def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> tuple[int, Any]:
        return await self._retry_until_ready(sock, sock.recvfrom_into, buf, nbytes)  # type: ignore[no-any-return]

    async def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> int:
        return await self._retry_until_ready(sock, _sendto, sock, data, address, write=True)  # type: ignore[no-any-return]

    async def sock_accept(self, sock: socket.socket) -> tuple[socket.socket, Any]:
        conn, address = await self._retry_until_ready(sock, sock.accept)
        conn.setblocking(False)
        return conn, address

    async def sock_sendall(self, sock: socket.socket, data: Any) -> None:
        view = memoryview(data).cast("B")
        sent = 0
        while sent < len(view):
            sent += await self._retry_until_ready(sock, _send_chunk, sock, view, sent, write=True)

    async def sock_connect(self, sock: socket.socket, address: Any) -> None:
        _check_non_blocking(sock)
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            resolved = await self.getaddrinfo(*address[:2], family=sock.family, type=sock.type, proto=sock.proto)
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
                future.set_exception(OSError(err, f"Connect call failed {address}"))
            else:
                future.set_result(None)

        token = self._watch(fd, True, check)
        try:
            await future
        finally:
            self._unwatch(fd, True, token)


def _attempt(future: asyncio.Future[Any], op: Callable[..., Any], args: tuple[Any, ...]) -> bool:
    """Settle `future` from one attempt at `op`; False means "not ready yet"."""
    if future.done():  # pragma: no cover - guards a wakeup the tests cannot force
        # Watching is level-triggered, so a partially drained descriptor could
        # report ready again before the awaiting coroutine resumes.
        return True
    try:
        result = op(*args)
    except BlockingIOError, InterruptedError:
        return False
    except OSError as exc:
        future.set_exception(exc)
    else:
        future.set_result(result)
    return True


def _send_chunk(sock: socket.socket, view: memoryview, sent: int) -> int:
    return sock.send(view[sent:])


def _sendto(sock: socket.socket, data: Any, address: Any) -> int:
    """Resolved per attempt, not bound once: callers rebind `sendto` between retries."""
    return sock.sendto(data, address)


def _check_non_blocking(sock: socket.socket) -> None:
    if sock.gettimeout() != 0:
        raise ValueError("the socket must be non-blocking")
