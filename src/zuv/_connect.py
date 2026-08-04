from __future__ import annotations

import asyncio
import os
import socket
import ssl as ssl_module
from asyncio import sslproto
from collections.abc import Callable, Sequence
from typing import Any

from . import _zuv
from ._server import Server
from ._sockets import SocketOperations

_SSLArg = ssl_module.SSLContext | bool | None


class ConnectionOperations(SocketOperations):
    """Connection and server setup.

    Sockets are created and bound here; once connected the descriptor is handed
    to libuv, which owns every subsequent read and write.
    """

    async def getaddrinfo(
        self,
        host: str | bytes | None,
        port: str | bytes | int | None,
        *,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> Sequence[tuple[int, int, int, str, tuple[Any, ...]]]:
        return await self._getaddrinfo(host, port, family, type, proto, flags)

    async def getnameinfo(self, sockaddr: tuple[Any, ...], flags: int = 0) -> tuple[str, str]:
        return await self._getnameinfo(sockaddr, flags)

    # -- clients -----------------------------------------------------------

    async def create_connection(
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        host: str | None = None,
        port: int | str | None = None,
        *,
        ssl: _SSLArg = None,
        family: int = 0,
        proto: int = 0,
        flags: int = 0,
        sock: socket.socket | None = None,
        local_addr: tuple[str, int] | None = None,
        server_hostname: str | None = None,
        ssl_handshake_timeout: float | None = None,
        ssl_shutdown_timeout: float | None = None,
    ) -> tuple[asyncio.Transport, Any]:
        server_hostname = _check_ssl_args(ssl, server_hostname, host)
        if host is not None or port is not None:
            if sock is not None:
                raise ValueError("host/port and sock can not be specified at the same time")
            sock = await self._connect_tcp(host, port, family, proto, flags, local_addr)
        elif sock is None:
            raise ValueError("host and port was not specified and no sock specified")
        else:
            _check_socket(sock, socket.SOCK_STREAM)
            sock.setblocking(False)
        return await self._wrap_socket(
            sock, protocol_factory, ssl, server_hostname, False, ssl_handshake_timeout, ssl_shutdown_timeout, None
        )

    async def create_unix_connection(
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        path: str | os.PathLike[str] | None = None,
        *,
        ssl: _SSLArg = None,
        sock: socket.socket | None = None,
        server_hostname: str | None = None,
        ssl_handshake_timeout: float | None = None,
        ssl_shutdown_timeout: float | None = None,
    ) -> tuple[asyncio.Transport, Any]:
        if ssl is None and server_hostname is not None:
            raise ValueError("server_hostname is only meaningful with ssl")
        if path is not None:
            if sock is not None:
                raise ValueError("path and sock can not be specified at the same time")
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.setblocking(False)
            try:
                await self.sock_connect(sock, os.fspath(path))
            except BaseException:
                sock.close()
                raise
        elif sock is None:
            raise ValueError("no path and sock were specified")
        else:
            _check_socket(sock, socket.SOCK_STREAM, family=socket.AF_UNIX)
            sock.setblocking(False)
        return await self._wrap_socket(
            sock, protocol_factory, ssl, server_hostname, False, ssl_handshake_timeout, ssl_shutdown_timeout, None
        )

    async def connect_accepted_socket(
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        sock: socket.socket,
        *,
        ssl: _SSLArg = None,
        ssl_handshake_timeout: float | None = None,
        ssl_shutdown_timeout: float | None = None,
    ) -> tuple[asyncio.Transport, Any]:
        _check_socket(sock, socket.SOCK_STREAM)
        sock.setblocking(False)
        return await self._wrap_socket(
            sock, protocol_factory, ssl, None, True, ssl_handshake_timeout, ssl_shutdown_timeout, None
        )

    async def _connect_tcp(
        self,
        host: str | None,
        port: int | str | None,
        family: int,
        proto: int,
        flags: int,
        local_addr: tuple[str, int] | None,
    ) -> socket.socket:
        infos = await self.getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM, proto=proto, flags=flags)
        if not infos:
            raise OSError(f"getaddrinfo({host!r}, {port!r}) returned no addresses")
        errors: list[OSError] = []
        for af, kind, pr, _canon, address in infos:
            sock = socket.socket(af, kind, pr)
            try:
                sock.setblocking(False)
                if local_addr is not None:
                    sock.bind(local_addr)
                await self.sock_connect(sock, address)
            except OSError as exc:
                sock.close()
                errors.append(exc)
            else:
                return sock
        raise errors[0]

    # -- servers -----------------------------------------------------------

    async def create_server(
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        host: str | Sequence[str] | None = None,
        port: int | None = None,
        *,
        family: int = socket.AF_UNSPEC,
        flags: int = socket.AI_PASSIVE,
        sock: socket.socket | None = None,
        backlog: int = 100,
        ssl: _SSLArg = None,
        reuse_address: bool | None = None,
        reuse_port: bool | None = None,
        ssl_handshake_timeout: float | None = None,
        ssl_shutdown_timeout: float | None = None,
        start_serving: bool = True,
    ) -> Server:
        if host is not None or port is not None:
            if sock is not None:
                raise ValueError("host/port and sock can not be specified at the same time")
            sockets = await self._bind_tcp(host, port, family, flags, reuse_address, reuse_port)
        elif sock is None:
            raise ValueError("Neither host/port nor sock were specified")
        else:
            _check_socket(sock, socket.SOCK_STREAM)
            sock.setblocking(False)
            sockets = [sock]
        server = Server(self, sockets, protocol_factory, ssl, backlog, ssl_handshake_timeout, ssl_shutdown_timeout)
        if start_serving:
            await server.start_serving()
        return server

    async def create_unix_server(
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        path: str | os.PathLike[str] | None = None,
        *,
        sock: socket.socket | None = None,
        backlog: int = 100,
        ssl: _SSLArg = None,
        ssl_handshake_timeout: float | None = None,
        ssl_shutdown_timeout: float | None = None,
        start_serving: bool = True,
        cleanup_socket: bool = True,
    ) -> Server:
        if path is not None:
            if sock is not None:
                raise ValueError("path and sock can not be specified at the same time")
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            path = os.fspath(path)
            try:
                sock.bind(path)
            except OSError as exc:
                sock.close()
                if exc.errno == 98 or isinstance(exc, FileExistsError):  # pragma: no cover - platform dependent
                    raise OSError(exc.errno, f"Address {path!r} is already in use") from None
                raise
            sock.setblocking(False)
        elif sock is None:
            raise ValueError("path was not specified, and no sock specified")
        else:
            _check_socket(sock, socket.SOCK_STREAM, family=socket.AF_UNIX)
            sock.setblocking(False)
        server = Server(self, [sock], protocol_factory, ssl, backlog, ssl_handshake_timeout, ssl_shutdown_timeout)
        if cleanup_socket and path is not None:
            server._cleanup_path = str(path)
        if start_serving:
            await server.start_serving()
        return server

    async def _bind_tcp(
        self,
        host: str | Sequence[str] | None,
        port: int | None,
        family: int,
        flags: int,
        reuse_address: bool | None,
        reuse_port: bool | None,
    ) -> list[socket.socket]:
        hosts: Sequence[str | None]
        if host is None or isinstance(host, str):
            hosts = [host]
        else:
            hosts = list(host)
        if reuse_address is None:
            reuse_address = os.name == "posix"

        sockets: list[socket.socket] = []
        try:
            for entry in hosts:
                infos = await self.getaddrinfo(entry, port, family=family, type=socket.SOCK_STREAM, flags=flags)
                for af, kind, proto, _canon, address in infos:
                    sock = socket.socket(af, kind, proto)
                    sockets.append(sock)
                    if reuse_address:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    if reuse_port:
                        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                    if af == socket.AF_INET6 and hasattr(socket, "IPPROTO_IPV6"):
                        sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, True)
                    sock.setblocking(False)
                    sock.bind(address)
        except BaseException:
            for sock in sockets:
                sock.close()
            raise
        return sockets

    def _accept_connection(
        self, conn: socket.socket, protocol_factory: Callable[[], asyncio.BaseProtocol], server: Server
    ) -> None:
        if server._ssl is None:
            protocol = protocol_factory()
            self._attach_transport(conn, protocol, None, server)
            return
        self.create_task(self._accept_tls(conn, protocol_factory, server))

    async def _accept_tls(
        self, conn: socket.socket, protocol_factory: Callable[[], asyncio.BaseProtocol], server: Server
    ) -> None:
        try:
            await self._wrap_socket(
                conn,
                protocol_factory,
                server._ssl,
                None,
                True,
                server._ssl_handshake_timeout,
                server._ssl_shutdown_timeout,
                server,
            )
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self.call_exception_handler({"message": "Error completing a TLS handshake", "exception": exc})
            server._detach()

    # -- transport construction -------------------------------------------

    def _attach_transport(
        self,
        sock: socket.socket,
        protocol: asyncio.BaseProtocol,
        waiter: asyncio.Future[None] | None,
        server: Server | None,
    ) -> _zuv.Transport:
        extra = {
            "sockname": _safe_addr(sock.getsockname),
            "peername": _safe_addr(sock.getpeername),
            "family": sock.family,
            "type": sock.type,
            "proto": sock.proto,
        }
        kind = _zuv.KIND_PIPE if sock.family == socket.AF_UNIX else _zuv.KIND_TCP
        buffered = isinstance(protocol, asyncio.BufferedProtocol)
        fd = sock.detach()
        try:
            return self._make_transport(fd, kind, protocol, waiter, extra, server, buffered)
        except BaseException:
            os.close(fd)
            raise

    async def _wrap_socket(
        self,
        sock: socket.socket,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        ssl: _SSLArg,
        server_hostname: str | None,
        server_side: bool,
        ssl_handshake_timeout: float | None,
        ssl_shutdown_timeout: float | None,
        server: Server | None,
    ) -> tuple[asyncio.Transport, Any]:
        protocol = protocol_factory()
        waiter = self.create_future()
        if ssl:
            context = _resolve_context(ssl, server_side)
            driver: asyncio.BaseProtocol = sslproto.SSLProtocol(
                self,
                protocol,
                context,
                waiter,
                server_side,
                server_hostname,
                ssl_handshake_timeout=ssl_handshake_timeout,
                ssl_shutdown_timeout=ssl_shutdown_timeout,
            )
            transport = self._attach_transport(sock, driver, None, server)
        else:
            driver = protocol
            transport = self._attach_transport(sock, driver, waiter, server)
        try:
            await waiter
        except BaseException:
            transport.close()
            raise
        if ssl:
            return driver._app_transport, protocol  # type: ignore[attr-defined,no-any-return]
        return transport, protocol

    async def start_tls(
        self,
        transport: asyncio.WriteTransport,
        protocol: asyncio.BaseProtocol,
        sslcontext: ssl_module.SSLContext,
        *,
        server_side: bool = False,
        server_hostname: str | None = None,
        ssl_handshake_timeout: float | None = None,
        ssl_shutdown_timeout: float | None = None,
    ) -> asyncio.Transport:
        waiter = self.create_future()
        ssl_protocol = sslproto.SSLProtocol(
            self,
            protocol,
            sslcontext,
            waiter,
            server_side,
            server_hostname,
            call_connection_made=False,
            ssl_handshake_timeout=ssl_handshake_timeout,
            ssl_shutdown_timeout=ssl_shutdown_timeout,
        )
        transport.pause_reading()
        transport.set_protocol(ssl_protocol)
        self.call_soon(ssl_protocol.connection_made, transport)
        self.call_soon(transport.resume_reading)
        try:
            await waiter
        except BaseException:
            transport.close()
            raise
        return ssl_protocol._app_transport  # type: ignore[no-any-return]

    # -- unsupported -------------------------------------------------------

    async def create_datagram_endpoint(self, *args: Any, **kwargs: Any) -> tuple[asyncio.DatagramTransport, Any]:
        raise NotImplementedError("zuv does not implement datagram endpoints yet")

    async def subprocess_exec(self, *args: Any, **kwargs: Any) -> tuple[asyncio.SubprocessTransport, Any]:
        raise NotImplementedError("zuv does not implement subprocesses yet")

    async def subprocess_shell(self, *args: Any, **kwargs: Any) -> tuple[asyncio.SubprocessTransport, Any]:
        raise NotImplementedError("zuv does not implement subprocesses yet")

    async def connect_read_pipe(self, *args: Any, **kwargs: Any) -> tuple[asyncio.ReadTransport, Any]:
        raise NotImplementedError("zuv does not implement pipe transports yet")

    async def connect_write_pipe(self, *args: Any, **kwargs: Any) -> tuple[asyncio.WriteTransport, Any]:
        raise NotImplementedError("zuv does not implement pipe transports yet")


def _check_ssl_args(ssl: _SSLArg, server_hostname: str | None, host: str | None) -> str | None:
    if server_hostname is not None and not ssl:
        raise ValueError("server_hostname is only meaningful with ssl")
    if ssl and server_hostname is None:
        if not host:
            raise ValueError("You must set server_hostname when using ssl without a host")
        return host
    return server_hostname


def _resolve_context(ssl: _SSLArg, server_side: bool) -> ssl_module.SSLContext:
    if isinstance(ssl, ssl_module.SSLContext):
        return ssl
    if server_side:
        raise ValueError("Server side ssl needs an SSLContext, not a bool")
    return ssl_module.create_default_context()


def _check_socket(sock: socket.socket, kind: int, family: int | None = None) -> None:
    if sock.type != kind:
        raise ValueError(f"A {kind!r} socket was expected, got {sock!r}")
    if family is not None and sock.family != family:
        raise ValueError(f"A {family!r} socket was expected, got {sock!r}")


def _safe_addr(getter: Callable[[], Any]) -> Any:
    try:
        return getter()
    except OSError:
        return None
