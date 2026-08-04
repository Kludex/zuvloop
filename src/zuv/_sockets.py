from __future__ import annotations

import socket
from collections.abc import Callable
from typing import Any

from ._base import LoopBase


class SocketOperations(LoopBase):
    """`sock_*` helpers, layered on the native reader/writer registrations."""

    async def _retry_until_ready(
        self, sock: socket.socket, op: Callable[..., Any], *args: Any, write: bool = False
    ) -> Any:
        _check_non_blocking(sock)
        try:
            return op(*args)
        except (BlockingIOError, InterruptedError):
            pass

        future = self.create_future()
        fd = sock.fileno()

        def retry() -> None:
            if future.done():
                return
            try:
                result = op(*args)
            except (BlockingIOError, InterruptedError):
                return
            except (SystemExit, KeyboardInterrupt):
                raise
            except BaseException as exc:
                future.set_exception(exc)
            else:
                future.set_result(result)

        if write:
            self.add_writer(fd, retry)
        else:
            self.add_reader(fd, retry)
        try:
            return await future
        finally:
            if write:
                self.remove_writer(fd)
            else:
                self.remove_reader(fd)

    async def sock_recv(self, sock: socket.socket, nbytes: int) -> bytes:
        return await self._retry_until_ready(sock, sock.recv, nbytes)  # type: ignore[no-any-return]

    async def sock_recv_into(self, sock: socket.socket, buf: Any) -> int:
        return await self._retry_until_ready(sock, sock.recv_into, buf)  # type: ignore[no-any-return]

    async def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> tuple[bytes, Any]:
        return await self._retry_until_ready(sock, sock.recvfrom, bufsize)  # type: ignore[no-any-return]

    async def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> tuple[int, Any]:
        return await self._retry_until_ready(sock, sock.recvfrom_into, buf, nbytes)  # type: ignore[no-any-return]

    async def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> int:
        return await self._retry_until_ready(sock, sock.sendto, data, address, write=True)  # type: ignore[no-any-return]

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
        except (BlockingIOError, InterruptedError):
            pass

        future = self.create_future()
        fd = sock.fileno()

        def check() -> None:
            if future.done():
                return
            err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err:
                future.set_exception(OSError(err, f"Connect call failed {address}"))
            else:
                future.set_result(None)

        self.add_writer(fd, check)
        try:
            await future
        finally:
            self.remove_writer(fd)

    async def sock_sendfile(
        self, sock: socket.socket, file: Any, offset: int = 0, count: int | None = None, *, fallback: bool = True
    ) -> int:
        raise NotImplementedError("zuv does not implement sock_sendfile(); read and sock_sendall() instead")

    async def sendfile(
        self,
        transport: Any,
        file: Any,
        offset: int = 0,
        count: int | None = None,
        *,
        fallback: bool = True,
    ) -> int:
        raise NotImplementedError("zuv does not implement sendfile(); write the file contents instead")


def _send_chunk(sock: socket.socket, view: memoryview, sent: int) -> int:
    return sock.send(view[sent:])


def _check_non_blocking(sock: socket.socket) -> None:
    if sock.gettimeout() != 0:
        raise ValueError("the socket must be non-blocking")
