from __future__ import annotations

import asyncio
import os
import socket
from asyncio import trsock
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
        self._transports: set[asyncio.Transport] = set()
        self._serving = False
        self._waiters: list[asyncio.Future[None]] | None = []
        self._serving_forever: asyncio.Future[None] | None = None
        self._cleanup_path: str | None = None
        self._cleanup_identity: tuple[int, int] | None = None

    @property
    def sockets(self) -> tuple[trsock.TransportSocket, ...]:
        return tuple(trsock.TransportSocket(sock) for sock in self._sockets or ())

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
                # Before transport adoption, the accepted socket and active
                # count are still ours. Afterwards the native close callback
                # owns both, including failure cleanup.
                if conn.fileno() != -1:
                    conn.close()
                    self._detach()
                raise

    def _attach(self, transport: asyncio.Transport) -> None:
        self._transports.add(transport)

    def _detach(self, transport: asyncio.Transport | None = None) -> None:
        if transport is not None:
            self._transports.discard(transport)
        self._active -= 1
        if self._active == 0 and self._sockets is None:
            self._wakeup()

    def close(self) -> None:
        # Whoever is inside `serve_forever` is waiting on this and has no other
        # way to learn the server has gone.
        serving_forever = self._serving_forever
        if serving_forever is not None and not serving_forever.done():
            self._serving_forever = None
            serving_forever.cancel()

        sockets = self._sockets
        if sockets is None:
            return
        self._sockets = None
        self._serving = False
        for sock in sockets:
            fd = sock.fileno()
            if fd != -1:
                self._loop.remove_reader(fd)
            sock.close()
        cleanup_path = self._cleanup_path
        cleanup_identity = self._cleanup_identity
        self._cleanup_path = None
        self._cleanup_identity = None
        if cleanup_path is not None and cleanup_identity is not None:
            try:
                current = os.stat(cleanup_path)
                if (current.st_dev, current.st_ino) == cleanup_identity:
                    os.unlink(cleanup_path)
            except FileNotFoundError:
                pass
        if self._active == 0:
            self._wakeup()

    def close_clients(self) -> None:
        for transport in tuple(self._transports):
            transport.close()

    def abort_clients(self) -> None:
        for transport in tuple(self._transports):
            transport.abort()

    def _wakeup(self) -> None:
        # `None` rather than an empty list, as asyncio does: it is how the two of
        # them say "closed, and every connection gone" to a later `wait_closed`.
        waiters, self._waiters = self._waiters, None
        for waiter in waiters or ():
            if not waiter.done():
                waiter.set_result(None)

    async def wait_closed(self) -> None:
        if self._waiters is None or (self._sockets is None and self._active == 0):
            return
        waiter = self._loop.create_future()
        self._waiters.append(waiter)
        try:
            await waiter
        finally:
            # A cancelled waiter is dropped rather than left for `_wakeup` to
            # skip, so waiting and giving up repeatedly cannot grow the list.
            # Once `_wakeup` has run there is no list to drop it from.
            if self._waiters is not None:
                self._waiters.remove(waiter)

    async def serve_forever(self) -> None:
        if self._serving_forever is not None:
            raise RuntimeError(f"server {self!r} is already being awaited on serve_forever()")
        if self._sockets is None:
            raise RuntimeError(f"server {self!r} is closed")

        self._start_serving()
        self._serving_forever = self._loop.create_future()
        try:
            await self._serving_forever
        except asyncio.CancelledError:
            try:
                self.close()
                self.close_clients()
                await self.wait_closed()
            finally:
                raise
        finally:
            self._serving_forever = None

    async def __aenter__(self) -> Server:
        return self

    async def __aexit__(  # type: ignore[override]  # typeshed types the arguments as object
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()
        await self.wait_closed()
