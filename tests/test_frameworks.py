"""aiohttp and uvicorn driven end to end on the loop.

The rest of the suite exercises the transport layer directly. A framework
reaches it through its own protocol, its own flow control and its own shutdown,
which is a different set of paths - and four defects that only Linux could see
have shipped, so what matters is that these run on every platform a wheel is
published for rather than on whichever machine the author happens to use.
"""

from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

import zuvloop

try:
    import aiohttp
    import uvicorn
    from aiohttp import web
except ModuleNotFoundError as exc:  # pragma: no cover - both come from the bench group
    # Only the packages themselves being absent is a reason to skip. Anything
    # broken underneath them - a transitive import, an ABI mismatch - is the
    # breakage these tests exist to notice, so it is left to propagate.
    if exc.name not in {"aiohttp", "uvicorn"}:
        raise
    pytest.skip("aiohttp and uvicorn are bench-group dependencies", allow_module_level=True)

pytestmark = pytest.mark.anyio

# Larger than a single write to the socket, so the response leaves through the
# partial-write path rather than in one go.
LARGE = b"x" * (1 << 20)

type Message = dict[str, object]
type Send = Callable[[Message], Awaitable[None]]
type Receive = Callable[[], Awaitable[Message]]


def bound_socket() -> socket.socket:
    """Bound here and handed to the server still bound.

    Reading a port from a throwaway socket and reconnecting to it later leaves a
    window where the kernel can give that port to something else, which on a
    runner building several of these at once is a flake nobody can reproduce.
    """
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    return sock


async def echo(request: web.Request) -> web.Response:
    return web.Response(body=await request.read(), content_type="text/plain")


async def bulk(_request: web.Request) -> web.Response:
    return web.Response(body=LARGE, content_type="application/octet-stream")


async def aiohttp_server() -> AsyncIterator[str]:
    assert isinstance(asyncio.get_running_loop(), zuvloop.EventLoop)
    app = web.Application()
    app.router.add_post("/echo", echo)
    app.router.add_get("/bulk", bulk)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    sock = bound_socket()
    port = sock.getsockname()[1]
    site = web.SockSite(runner, sock)
    await site.start()
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        await runner.cleanup()


async def test_aiohttp_round_trips_a_request() -> None:
    async for base in aiohttp_server():
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{base}/echo", data=b"ping") as response:
                assert response.status == 200
                assert await response.read() == b"ping"


async def test_aiohttp_streams_a_large_response() -> None:
    """A megabyte does not fit one write, so this rides the flush path."""
    async for base in aiohttp_server():
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base}/bulk") as response:
                assert await response.read() == LARGE


async def test_aiohttp_serves_concurrent_requests() -> None:
    async for base in aiohttp_server():
        async with aiohttp.ClientSession() as session:

            async def once(index: int) -> bytes:
                payload = str(index).encode()
                async with session.post(f"{base}/echo", data=payload) as response:
                    return await response.read()

            results = await asyncio.gather(*(once(index) for index in range(50)))
        assert results == [str(index).encode() for index in range(50)]


async def app(scope: Message, receive: Receive, send: Send) -> None:
    assert scope["type"] == "http"
    body = LARGE if scope["path"] == "/bulk" else b"Hello, World!"
    await send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"text/plain"), (b"content-length", str(len(body)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": body})


async def uvicorn_server() -> AsyncIterator[str]:
    assert isinstance(asyncio.get_running_loop(), zuvloop.EventLoop)
    sock = bound_socket()
    port = sock.getsockname()[1]
    config = uvicorn.Config(app, log_level="error", access_log=False)
    server = uvicorn.Server(config)
    # uvicorn installs process-wide SIGINT and SIGTERM handlers otherwise, which
    # is the runner's business rather than a test's.
    server.capture_signals = _no_signals  # type: ignore[assignment]
    serving = asyncio.ensure_future(server.serve(sockets=[sock]))
    try:
        while not server.started:
            # Nothing else is watching this task. Left unwatched, a server that
            # raised before it started serving would spin here until the job
            # timed out rather than reporting what went wrong.
            if serving.done():  # pragma: no cover - only reached when the server fails to start
                await serving
                raise RuntimeError("uvicorn stopped before it began serving")
            await asyncio.sleep(0.01)
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        await serving


class _no_signals:
    """Stands in for `uvicorn.Server.capture_signals`, which this does not want."""

    def __enter__(self) -> None:
        return None

    def __exit__(self, *exc: object) -> None:
        return None


async def test_uvicorn_serves_a_plaintext_response() -> None:
    async for base in uvicorn_server():
        async with aiohttp.ClientSession() as session:
            async with session.get(base) as response:
                assert response.status == 200
                assert await response.read() == b"Hello, World!"


async def test_uvicorn_serves_a_large_body() -> None:
    async for base in uvicorn_server():
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{base}/bulk") as response:
                assert await response.read() == LARGE
