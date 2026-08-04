from __future__ import annotations

import asyncio
import os
import socket
from collections.abc import Callable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ._connect import ConnectionOperations


class Server(asyncio.AbstractServer):
    """Accepts connections on one or more listening sockets.

    Accept runs in Python - it happens once per connection, unlike the data
    path, which never leaves the extension.
    """

    def __init__(
        self,
        loop: ConnectionOperations,
        sockets: Sequence[socket.socket],
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        ssl: Any,
        backlog: int,
        ssl_handshake_timeout: float | None,
        ssl_shutdown_timeout: float | None,
    ) -> None:
        self._loop = loop
        self._sockets: list[socket.socket] | None = list(sockets)
        self._protocol_factory = protocol_factory
        self._ssl = ssl
        self._backlog = backlog
        self._ssl_handshake_timeout = ssl_handshake_timeout
        self._ssl_shutdown_timeout = ssl_shutdown_timeout
        self._active = 0
        self._serving = False
        self._waiters: list[asyncio.Future[None]] = []
        self._cleanup_path: str | None = None

    @property
    def sockets(self) -> tuple[socket.socket, ...]:
        return tuple(self._sockets or ())

    def get_loop(self) -> ConnectionOperations:
        return self._loop

    def is_serving(self) -> bool:
        return self._serving

    def _start_serving(self) -> None:
        if self._serving or self._sockets is None:
            return
        self._serving = True
        for sock in self._sockets:
            sock.listen(self._backlog)
            self._loop.add_reader(sock.fileno(), self._accept, sock)

    async def start_serving(self) -> None:
        self._start_serving()
        # Let the accept callbacks register before returning, matching asyncio.
        await asyncio.sleep(0)

    def _accept(self, sock: socket.socket) -> None:
        for _ in range(self._backlog):
            try:
                conn, _addr = sock.accept()
            except BlockingIOError, InterruptedError:
                return
            except OSError as exc:  # pragma: no cover - needs descriptor exhaustion
                self._loop.call_exception_handler(
                    {"message": "Error accepting a connection", "exception": exc, "socket": sock}
                )
                return
            conn.setblocking(False)
            self._active += 1
            try:
                self._loop._accept_connection(conn, self._protocol_factory, self)
            except BaseException:
                conn.close()
                self._detach()
                raise

    def _detach(self) -> None:
        self._active -= 1
        if self._active == 0 and self._sockets is None:
            self._wakeup()

    def close(self) -> None:
        sockets = self._sockets
        if sockets is None:
            return
        self._sockets = None
        self._serving = False
        for sock in sockets:
            self._loop.remove_reader(sock.fileno())
            sock.close()
        if self._cleanup_path is not None:
            os.unlink(self._cleanup_path)
            self._cleanup_path = None
        if self._active == 0:
            self._wakeup()

    def close_clients(self) -> None:  # pragma: no cover - parity with asyncio 3.13+
        self.close()

    def abort_clients(self) -> None:  # pragma: no cover - parity with asyncio 3.13+
        self.close()

    def _wakeup(self) -> None:
        waiters, self._waiters = self._waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    async def wait_closed(self) -> None:
        if self._sockets is None and self._active == 0:
            return
        waiter = self._loop.create_future()
        self._waiters.append(waiter)
        await waiter

    async def serve_forever(self) -> None:
        self._start_serving()
        try:
            await self._loop.create_future()
        finally:
            self.close()

    async def __aenter__(self) -> Server:
        return self

    async def __aexit__(  # type: ignore[override]  # typeshed types the arguments as object
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()
        await self.wait_closed()
