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

import aiohttp
import pytest
import uvicorn
from aiohttp import web

import zuvloop

pytestmark = pytest.mark.anyio

# Larger than a single write to the socket, so the response leaves through the
# partial-write path rather than in one go.
LARGE = b"x" * (1 << 20)

type Message = dict[str, object]
type Send = Callable[[Message], Awaitable[None]]
type Receive = Callable[[], Awaitable[Message]]


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


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
    port = free_port()
    site = web.TCPSite(runner, "127.0.0.1", port)
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
    port = free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error", access_log=False)
    server = uvicorn.Server(config)
    # uvicorn installs process-wide SIGINT and SIGTERM handlers otherwise, which
    # is the runner's business rather than a test's.
    server.capture_signals = _no_signals  # type: ignore[assignment]
    serving = asyncio.ensure_future(server.serve())
    try:
        while not server.started:
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
