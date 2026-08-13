from __future__ import annotations

import asyncio
import errno
import os
import signal
import socket
import ssl as ssl_module
import stat
import subprocess
from asyncio import base_subprocess, sslproto, staggered, trsock
from asyncio.base_events import _interleave_addrinfos  # type: ignore[attr-defined]  # private, not in typeshed
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, cast

from . import _zuvloop
from ._process import Popen
from ._sendfile import SendfileOperations
from ._server import Server

# What `getaddrinfo` hands back: family, kind, protocol, canonical name, address.
type _AddrInfo = tuple[int, int, int, str, tuple[str, int] | tuple[str, int, int, int]]
type _DatagramAddress = tuple[str, int] | str | bytes
_SSLArg = ssl_module.SSLContext | bool | None


class ConnectionOperations(SendfileOperations):
    """Connection and server setup.

    Sockets are created and bound here; once connected the descriptor is handed
    to libuv, which owns every subsequent read and write.
    """

    async def getaddrinfo(  # type: ignore[override]  # returns the same tuples, typed loosely
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
        happy_eyeballs_delay: float | None = None,
        interleave: int | None = None,
        all_errors: bool = False,
    ) -> tuple[asyncio.Transport, Any]:
        server_hostname = _check_ssl_args(ssl, server_hostname, host)
        _check_ssl_timeouts(ssl, ssl_handshake_timeout, ssl_shutdown_timeout)
        if happy_eyeballs_delay is not None and interleave is None:
            # Racing addresses that are all one family races nothing.
            interleave = 1
        if host is not None or port is not None:
            if sock is not None:
                raise ValueError("host/port and sock can not be specified at the same time")
            sock = await self._connect_tcp(
                host, port, family, proto, flags, local_addr, happy_eyeballs_delay, interleave, all_errors
            )
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
        _check_ssl_timeouts(ssl, ssl_handshake_timeout, ssl_shutdown_timeout)
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
        _check_ssl_timeouts(ssl, ssl_handshake_timeout, ssl_shutdown_timeout)
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
        happy_eyeballs_delay: float | None = None,
        interleave: int | None = None,
        all_errors: bool = False,
    ) -> socket.socket:
        infos = await self.getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM, proto=proto, flags=flags)
        if not infos:  # pragma: no cover - libuv reports an error rather than an empty list
            raise OSError(f"getaddrinfo({host!r}, {port!r}) returned no addresses")
        if interleave:
            infos = _interleave_addrinfos(infos, interleave)

        # One slot per address, filled in the order they were tried.
        errors: list[list[OSError]] = []
        winner: socket.socket | None = None
        if happy_eyeballs_delay is None:
            for info in infos:
                try:
                    winner = await self._connect_one(errors, info, local_addr)
                    break
                except OSError:
                    continue
        else:
            # Each attempt starts `happy_eyeballs_delay` after the last, and the
            # first to connect wins - so an address on a broken route costs that
            # delay rather than a full connect timeout. RFC 8305.
            winner = (
                await staggered.staggered_race(
                    (self._attempt(errors, info, local_addr) for info in infos),
                    happy_eyeballs_delay,
                    loop=self,
                )
            )[0]

        if winner is not None:
            return winner
        flattened = [exc for slot in errors for exc in slot]
        if all_errors:
            raise ExceptionGroup("create_connection failed", flattened)
        if len(flattened) == 1:
            raise flattened[0]
        if not flattened:  # pragma: no cover - every attempt records its failure
            raise OSError("Multiple exceptions: (no error was recorded)")
        # All the same complaint reads better as one of them than as a list.
        model = str(flattened[0])
        if all(str(exc) == model for exc in flattened):
            raise flattened[0]
        raise OSError(f"Multiple exceptions: {', '.join(str(exc) for exc in flattened)}")

    def _attempt(
        self,
        errors: list[list[OSError]],
        info: _AddrInfo,
        local_addr: tuple[str, int] | None,
    ) -> Callable[[], Awaitable[socket.socket]]:
        """One attempt, bound to its address, for the race to start when it likes."""

        async def run() -> socket.socket:
            return await self._connect_one(errors, info, local_addr)

        return run

    async def _connect_one(
        self,
        errors: list[list[OSError]],
        info: _AddrInfo,
        local_addr: tuple[str, int] | None,
    ) -> socket.socket:
        # The slot is taken before anything is awaited, so the failures come back
        # in the order the addresses were tried rather than the order they lost -
        # which under a race is whatever the network decided that time.
        mine: list[OSError] = []
        errors.append(mine)
        af, kind, proto, _canon, address = info
        try:
            # This raises for real - EMFILE, an address family the kernel will
            # not give - and an attempt that failed without recording anything
            # leaves nothing to report at the end.
            sock = socket.socket(af, kind, proto)
        except OSError as exc:
            mine.append(exc)
            raise

        try:
            sock.setblocking(False)
            if local_addr is not None:
                try:
                    sock.bind(local_addr)
                except OSError as exc:
                    # Which address was refused is the useful half of the report.
                    message = f"error while attempting to bind on address {local_addr!r}: {str(exc).lower()}"
                    raise OSError(exc.errno, message) from None
            await self.sock_connect(sock, address)
        except OSError as exc:
            sock.close()
            mine.append(exc)
            raise
        except BaseException:
            sock.close()
            raise
        return sock

    # -- servers -----------------------------------------------------------

    async def create_server(  # type: ignore[override]  # typeshed overloads host/port/sock
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
        keep_alive: bool | None = None,
        ssl_handshake_timeout: float | None = None,
        ssl_shutdown_timeout: float | None = None,
        start_serving: bool = True,
    ) -> Server:
        _check_server_ssl(ssl)
        _check_ssl_timeouts(ssl, ssl_handshake_timeout, ssl_shutdown_timeout)
        if host is not None or port is not None:
            if sock is not None:
                raise ValueError("host/port and sock can not be specified at the same time")
            sockets = await self._bind_tcp(host, port, family, flags, reuse_address, reuse_port, keep_alive)
        elif sock is None:
            raise ValueError("Neither host/port nor sock were specified")
        else:
            _check_socket(sock, socket.SOCK_STREAM)
            sock.setblocking(False)
            sockets = [sock]
        server = Server(self, sockets, protocol_factory, ssl, backlog, ssl_handshake_timeout, ssl_shutdown_timeout)
        if start_serving:
            try:
                await server.start_serving()
            except BaseException:
                server.close()
                raise
        return server

    async def create_unix_server(  # type: ignore[override]  # cleanup_socket is missing from typeshed
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
        _check_server_ssl(ssl)
        _check_ssl_timeouts(ssl, ssl_handshake_timeout, ssl_shutdown_timeout)
        if path is not None:
            if sock is not None:
                raise ValueError("path and sock can not be specified at the same time")
            path = os.fspath(path)
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.bind(path)
            except OSError as exc:
                sock.close()
                # 98 is Linux's EADDRINUSE and 48 is this platform's, so the
                # number has to come from the platform rather than the source.
                if exc.errno == errno.EADDRINUSE or isinstance(exc, FileExistsError):
                    raise OSError(exc.errno, f"Address {path!r} is already in use") from None
                raise
            sock.setblocking(False)
        elif sock is None:
            raise ValueError("path was not specified, and no sock specified")
        else:
            _check_socket(sock, socket.SOCK_STREAM, family=socket.AF_UNIX)
            sock.setblocking(False)
        server = Server(self, [sock], protocol_factory, ssl, backlog, ssl_handshake_timeout, ssl_shutdown_timeout)
        # From here the server owns a bound socket that only it can close, so
        # nothing may escape without closing it - not even `os.stat` failing.
        try:
            if cleanup_socket and path is not None and not path.startswith("\0"):
                try:
                    bound = os.stat(path)
                except FileNotFoundError:
                    pass
                else:
                    server._cleanup_path = path
                    server._cleanup_identity = (bound.st_dev, bound.st_ino)
            if start_serving:
                await server.start_serving()
        except BaseException:
            server.close()
            raise
        return server

    async def _bind_tcp(
        self,
        host: str | Sequence[str] | None,
        port: int | None,
        family: int,
        flags: int,
        reuse_address: bool | None,
        reuse_port: bool | None,
        keep_alive: bool | None,
    ) -> list[socket.socket]:
        hosts: Sequence[str | None]
        if host is None or isinstance(host, str):
            # An empty host means every interface, which is what a null host
            # resolves to; passing it through would ask for the host named "".
            hosts = [host or None]
        else:
            hosts = [entry or None for entry in host]
        if reuse_address is None:
            reuse_address = os.name == "posix"

        resolved: list[tuple[int, int, int, str, Any]] = []
        for entry in hosts:
            resolved += await self.getaddrinfo(entry, port, family=family, type=socket.SOCK_STREAM, flags=flags)

        sockets: list[socket.socket] = []
        try:
            # Hosts that resolve to the same address collapse to one socket -
            # repeating a host in the list is not a request to bind it twice.
            for af, kind, proto, _canon, address in dict.fromkeys(resolved):
                sock = socket.socket(af, kind, proto)
                sockets.append(sock)
                if reuse_address:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if reuse_port:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                if keep_alive:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
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
        except BaseException as exc:
            # Before descriptor adoption, the socket and server count are
            # still ours to release. After adoption, _wrap_socket closes the
            # transport and its close callback detaches from the server.
            if conn.fileno() != -1:
                conn.close()
                server._detach()
            if isinstance(exc, OSError):
                # SSLError and TimeoutError are both OSError subclasses.
                self.call_exception_handler({"message": "Error completing a TLS handshake", "exception": exc})
                return
            raise

    # -- transport construction -------------------------------------------

    def _attach_transport(
        self,
        sock: socket.socket,
        protocol: asyncio.BaseProtocol,
        waiter: asyncio.Future[None] | None,
        server: Server | None,
    ) -> _zuvloop.Transport:
        extra = {
            "sockname": _safe_addr(sock.getsockname),
            "peername": _safe_addr(sock.getpeername),
            "family": sock.family,
            "type": sock.type,
            "proto": sock.proto,
        }
        kind = _zuvloop.KIND_PIPE if sock.family == socket.AF_UNIX else _zuvloop.KIND_TCP
        fd = sock.fileno()

        # anyio - and so httpx, Starlette and FastAPI - reaches for the raw socket
        # through get_extra_info("socket"). asyncio exposes the caller's own object
        # here and leaves it armed for the life of the transport, which callers that
        # passed `sock=` read back afterwards; a separate view detached from `sock`
        # would hand them a closed socket instead.
        extra["socket"] = trsock.TransportSocket(sock)
        transport = self._make_transport(fd, kind, protocol, waiter, extra, server)

        # libuv owns the descriptor from here. Disarm the socket on any later
        # failure and close the native transport, so ownership cannot be split.
        try:
            transport._adopt_socket_view(sock)
            if server is not None:
                server._attach(transport)
        except BaseException:
            sock.detach()
            transport.abort()
            raise
        return transport

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
                ssl_handshake_timeout=ssl_handshake_timeout,  # type: ignore[arg-type]  # typeshed says int
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
            return cast("asyncio.Transport", driver._app_transport), protocol  # type: ignore[attr-defined]
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
            ssl_handshake_timeout=ssl_handshake_timeout,  # type: ignore[arg-type]  # typeshed says int
            ssl_shutdown_timeout=ssl_shutdown_timeout,
        )
        stream = cast("asyncio.Transport", transport)
        stream.pause_reading()
        stream.set_protocol(ssl_protocol)
        self.call_soon(ssl_protocol.connection_made, stream)
        self.call_soon(stream.resume_reading)
        try:
            await waiter
        except BaseException:
            stream.close()
            raise
        return cast("asyncio.Transport", ssl_protocol._app_transport)

    # -- unsupported -------------------------------------------------------

    async def create_datagram_endpoint(  # type: ignore[override]  # typeshed omits reuse_port
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        local_addr: _DatagramAddress | None = None,
        remote_addr: _DatagramAddress | None = None,
        *,
        family: int = 0,
        proto: int = 0,
        flags: int = 0,
        reuse_port: bool | None = None,
        allow_broadcast: bool | None = None,
        sock: socket.socket | None = None,
    ) -> tuple[asyncio.DatagramTransport, Any]:
        if sock is not None:
            if local_addr is not None or remote_addr is not None or family or proto or flags or reuse_port:
                raise ValueError("socket modifier keyword arguments can not be used when sock is specified")
            _check_socket(sock, socket.SOCK_DGRAM)
            sock.setblocking(False)
            connected = _safe_addr(sock.getpeername) is not None
        else:
            sock, connected = await self._bind_datagram(
                local_addr, remote_addr, family, proto, flags, reuse_port, allow_broadcast
            )

        protocol = protocol_factory()
        waiter = self.create_future()
        try:
            transport = self._attach_datagram(sock, protocol, connected)
        except BaseException:
            # Until libuv adopts the descriptor, the socket is still ours.
            sock.close()
            raise
        transport._start_receiving()
        self.call_soon(protocol.connection_made, transport)
        self.call_soon(_set_result_unless_done, waiter)
        try:
            await waiter
        except BaseException:
            transport.close()
            raise
        return transport, protocol

    async def _bind_datagram(
        self,
        local_addr: _DatagramAddress | None,
        remote_addr: _DatagramAddress | None,
        family: int,
        proto: int,
        flags: int,
        reuse_port: bool | None,
        allow_broadcast: bool | None,
    ) -> tuple[socket.socket, bool]:
        if local_addr is None and remote_addr is None:
            if not family:
                raise ValueError("unexpected address family")
            resolved_local: Any = None
            resolved_remote: Any = None
        else:
            resolved_local = await self._resolve_datagram(local_addr, family, proto, flags | socket.AI_PASSIVE)
            resolved_remote = await self._resolve_datagram(remote_addr, family, proto, flags)
            probe = resolved_local or resolved_remote
            assert probe is not None
            family, proto = probe[0], probe[2]

        sock = socket.socket(family, socket.SOCK_DGRAM, proto)
        try:
            if reuse_port:
                _set_reuse_port(sock)
            if allow_broadcast:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.setblocking(False)
            if resolved_local is not None:
                sock.bind(resolved_local[4])
            if resolved_remote is not None:
                await self.sock_connect(sock, resolved_remote[4])
        except BaseException:
            sock.close()
            raise
        return sock, resolved_remote is not None

    async def _resolve_datagram(self, address: _DatagramAddress | None, family: int, proto: int, flags: int) -> Any:
        if address is None:
            return None
        if family == socket.AF_UNIX:
            return (family, socket.SOCK_DGRAM, proto, "", address)
        if not isinstance(address, tuple) or len(address) < 2:
            raise TypeError("string or tuple of (host, port) expected")
        infos = await self.getaddrinfo(
            address[0], address[1], family=family, type=socket.SOCK_DGRAM, proto=proto, flags=flags
        )
        if not infos:
            raise OSError("getaddrinfo() returned empty list")
        return infos[0]

    def _attach_datagram(
        self, sock: socket.socket, protocol: asyncio.BaseProtocol, connected: bool
    ) -> _zuvloop.DatagramTransport:
        extra: dict[str, Any] = {
            "sockname": _safe_addr(sock.getsockname),
            "peername": _safe_addr(sock.getpeername),
            "family": sock.family,
            "type": sock.type,
            "proto": sock.proto,
        }
        fd = sock.fileno()

        # As on the stream path: the caller's own socket is what asyncio exposes,
        # and `create_datagram_endpoint(sock=...)` callers go on using it.
        extra["socket"] = trsock.TransportSocket(sock)
        transport = self._make_datagram_transport(fd, sock.family, connected, protocol, extra)
        transport._adopt_socket_view(sock)
        return transport

    async def subprocess_shell(
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        cmd: str | bytes,
        *,
        stdin: Any = subprocess.PIPE,
        stdout: Any = subprocess.PIPE,
        stderr: Any = subprocess.PIPE,
        universal_newlines: bool = False,
        shell: bool = True,
        bufsize: int = 0,
        encoding: str | None = None,
        errors: str | None = None,
        text: bool | None = None,
        **kwargs: Any,
    ) -> tuple[asyncio.SubprocessTransport, Any]:
        if not isinstance(cmd, (bytes, str)):
            raise ValueError("cmd must be a string")
        _check_subprocess_text(universal_newlines, shell, bufsize, encoding, errors, text, expect_shell=True)
        protocol = protocol_factory()
        transport = await self._make_subprocess_transport(protocol, cmd, True, stdin, stdout, stderr, bufsize, **kwargs)
        return transport, protocol

    async def subprocess_exec(
        self,
        protocol_factory: Callable[[], asyncio.BaseProtocol],
        program: Any,
        *args: Any,
        stdin: Any = subprocess.PIPE,
        stdout: Any = subprocess.PIPE,
        stderr: Any = subprocess.PIPE,
        universal_newlines: bool = False,
        shell: bool = False,
        bufsize: int = 0,
        encoding: str | None = None,
        errors: str | None = None,
        text: bool | None = None,
        **kwargs: Any,
    ) -> tuple[asyncio.SubprocessTransport, Any]:
        _check_subprocess_text(universal_newlines, shell, bufsize, encoding, errors, text, expect_shell=False)
        popen_args = (program, *args)
        for arg in popen_args:
            if not isinstance(arg, (str, bytes)):
                raise TypeError(f"program arguments must be a bytes or text string, not {type(arg).__name__}")
        protocol = protocol_factory()
        transport = await self._make_subprocess_transport(
            protocol, popen_args, False, stdin, stdout, stderr, bufsize, **kwargs
        )
        return transport, protocol

    async def _make_subprocess_transport(
        self,
        protocol: asyncio.BaseProtocol,
        args: Any,
        shell: bool,
        stdin: Any,
        stdout: Any,
        stderr: Any,
        bufsize: int,
        extra: Any = None,
        **kwargs: Any,
    ) -> asyncio.SubprocessTransport:
        # asyncio's own transport drives the process; it needs `connect_read_pipe`
        # and `connect_write_pipe` from the loop, and those are native here. A
        # process is spawned once, so the readable implementation is worth more
        # than owning the fork.
        waiter = self.create_future()
        transport = _SubprocessTransport(
            self,
            cast("asyncio.SubprocessProtocol", protocol),
            args,
            shell,
            stdin,
            stdout,
            stderr,
            bufsize,
            waiter=waiter,
            extra=extra,
            **kwargs,
        )
        try:
            await waiter
        except SystemExit, KeyboardInterrupt:  # pragma: no cover - not raised by a spawn
            raise
        except BaseException:
            transport.close()
            await transport._wait()
            raise
        return cast("asyncio.SubprocessTransport", transport)

    async def connect_read_pipe(
        self, protocol_factory: Callable[[], asyncio.BaseProtocol], pipe: Any
    ) -> tuple[asyncio.ReadTransport, Any]:
        return await self._connect_pipe(protocol_factory, pipe, _zuvloop.KIND_PIPE)

    async def connect_write_pipe(
        self, protocol_factory: Callable[[], asyncio.BaseProtocol], pipe: Any
    ) -> tuple[asyncio.WriteTransport, Any]:
        return await self._connect_pipe(protocol_factory, pipe, _zuvloop.KIND_PIPE_WRITE)

    async def _connect_pipe(
        self, protocol_factory: Callable[[], asyncio.BaseProtocol], pipe: Any, kind: int
    ) -> tuple[_zuvloop.Transport, Any]:
        fd = pipe.fileno()
        mode = os.fstat(fd).st_mode
        if not (stat.S_ISFIFO(mode) or stat.S_ISSOCK(mode) or stat.S_ISCHR(mode)):
            raise ValueError("Pipe transport is only for pipes, sockets and character devices")
        os.set_blocking(fd, False)

        protocol = protocol_factory()
        waiter = self.create_future()
        extra = {"pipe": pipe}
        # The transport owns the descriptor from here; `pipe` keeps a duplicate
        # so that closing the transport closes exactly one of them.
        duplicate = os.dup(fd)
        try:
            transport = self._make_transport(duplicate, kind, protocol, waiter, extra, None)
        except BaseException:
            os.close(duplicate)
            raise
        transport._adopt_pipe(pipe)
        try:
            # A socket write pipe learns of the hangup by reading; `uv_pipe_open`
            # clears the readable flag on an `O_WRONLY` FIFO, so that one needs a
            # poll of its own. Character devices report no hangup at all.
            if kind == _zuvloop.KIND_PIPE_WRITE and stat.S_ISFIFO(mode):
                watch = _HangupWatch(self, pipe, fd, transport)
                transport._adopt_pipe(watch)
                watch.arm()
            await waiter
        except BaseException:
            transport.close()
            raise
        return transport, protocol


class _HangupWatch:
    """Reports the peer closing the read end of a write pipe.

    The poll goes on the descriptor the transport kept rather than the duplicate
    libuv adopted, so the two never contend for one watcher. The transport adopts
    this object in place of the pipe, which puts `close()` on every teardown path
    the transport already has - dropping the poll before the pipe it watches.
    """

    __slots__ = ("_fd", "_loop", "_pipe", "_transport")

    def __init__(self, loop: ConnectionOperations, pipe: Any, fd: int, transport: _zuvloop.Transport) -> None:
        self._loop = loop
        self._pipe = pipe
        self._fd = fd
        self._transport = transport

    def arm(self) -> None:
        self._loop.add_reader(self._fd, self._hangup)

    def close(self) -> None:
        self._loop.remove_reader(self._fd)
        self._pipe.close()

    def _hangup(self) -> None:
        # Dropping the watch here, rather than leaving it to close(), also marks
        # this callback cancelled should the loop already have it queued.
        self._loop.remove_reader(self._fd)
        buffered = self._transport.get_write_buffer_size()
        self._transport._force_close(BrokenPipeError() if buffered else None)


def _check_ssl_timeouts(ssl: _SSLArg, handshake: float | None, shutdown: float | None) -> None:
    if handshake is not None and not ssl:
        raise ValueError("ssl_handshake_timeout is only meaningful with ssl")
    if shutdown is not None and not ssl:
        raise ValueError("ssl_shutdown_timeout is only meaningful with ssl")


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
    return ssl_module.create_default_context()


def _check_server_ssl(ssl: _SSLArg) -> None:
    if ssl and not isinstance(ssl, ssl_module.SSLContext):
        raise ValueError("Server side ssl needs an SSLContext, not a bool")


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


def _set_reuse_port(sock: socket.socket) -> None:
    if not hasattr(socket, "SO_REUSEPORT"):
        raise ValueError("reuse_port not supported by socket module")
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)


def _set_result_unless_done(future: asyncio.Future[None]) -> None:
    """Resolves the setup waiter, unless cancellation got there first."""
    if not future.done():
        future.set_result(None)


def _check_subprocess_text(
    universal_newlines: bool,
    shell: bool,
    bufsize: int,
    encoding: str | None,
    errors: str | None,
    text: bool | None,
    *,
    expect_shell: bool,
) -> None:
    """Rejects the text-mode arguments a subprocess transport cannot honour."""
    if universal_newlines:
        raise ValueError("universal_newlines must be False")
    if shell is not expect_shell:
        raise ValueError(f"shell must be {expect_shell}")
    if bufsize != 0:
        raise ValueError("bufsize must be 0")
    if text:
        raise ValueError("text must be False")
    if encoding is not None:
        raise ValueError("encoding must be None")
    if errors is not None:
        raise ValueError("errors must be None")


class _SubprocessTransport(base_subprocess.BaseSubprocessTransport):
    """asyncio's subprocess transport, spawning through libuv.

    Everything it does with the child - the pipes, the protocol callbacks, the
    exit bookkeeping - is asyncio's. Only the spawn is replaced, and `Popen`
    reports the exit itself, so no child watcher is needed.
    """

    def send_signal(self, signal_number: int) -> None:
        # asyncio signals the pid directly, which can race a reaped pid onto a
        # new process. libuv holds the handle, so it signals the child it spawned
        # or nothing at all. An exited child is a no-op, as asyncio has it.
        self._check_proc()
        assert self._proc is not None
        try:
            self._proc.send_signal(signal_number)
        except ProcessLookupError:  # pragma: no cover - pid exit race
            pass

    def terminate(self) -> None:
        self.send_signal(signal.SIGTERM)

    def kill(self) -> None:
        self.send_signal(signal.SIGKILL)

    def _start(
        self,
        args: Any,
        shell: bool,
        stdin: Any,
        stdout: Any,
        stderr: Any,
        bufsize: int,
        **kwargs: Any,
    ) -> None:
        argv = ["/bin/sh", "-c", args] if shell else [os.fsdecode(arg) for arg in args]
        self._proc = Popen(  # type: ignore[assignment]  # a Popen-shaped object, not a Popen
            cast("ConnectionOperations", self._loop),
            argv,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            on_exit=self._process_exited,
            **kwargs,
        )
