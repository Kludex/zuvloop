from __future__ import annotations

import asyncio
import socket

import pytest

pytestmark = pytest.mark.anyio


async def test_getaddrinfo_resolves_localhost() -> None:
    loop = asyncio.get_running_loop()
    results = await loop.getaddrinfo("localhost", 80, type=socket.SOCK_STREAM)
    assert results
    for family, kind, proto, canonname, sockaddr in results:
        assert family in (socket.AF_INET, socket.AF_INET6)
        assert kind is socket.SOCK_STREAM
        assert isinstance(proto, int)
        assert canonname == ""
        assert sockaddr[1] == 80


async def test_getaddrinfo_accepts_a_service_name() -> None:
    loop = asyncio.get_running_loop()
    results = await loop.getaddrinfo("127.0.0.1", "http", family=socket.AF_INET, type=socket.SOCK_STREAM)
    assert results[0][4] == ("127.0.0.1", 80)


async def test_getaddrinfo_accepts_bytes() -> None:
    loop = asyncio.get_running_loop()
    results = await loop.getaddrinfo(b"127.0.0.1", b"80", family=socket.AF_INET, type=socket.SOCK_STREAM)
    assert results[0][4] == ("127.0.0.1", 80)


async def test_getaddrinfo_binds_a_wildcard_address() -> None:
    loop = asyncio.get_running_loop()
    results = await loop.getaddrinfo(None, 0, family=socket.AF_INET, type=socket.SOCK_STREAM, flags=socket.AI_PASSIVE)
    assert results[0][4] == ("0.0.0.0", 0)


async def test_getaddrinfo_returns_canonical_names() -> None:
    loop = asyncio.get_running_loop()
    results = await loop.getaddrinfo(
        "localhost", 80, family=socket.AF_INET, type=socket.SOCK_STREAM, flags=socket.AI_CANONNAME
    )
    assert results[0][3] != ""


async def test_getaddrinfo_reports_failures() -> None:
    loop = asyncio.get_running_loop()
    with pytest.raises(socket.gaierror):
        await loop.getaddrinfo("this-host-should-not-exist.invalid", 80, type=socket.SOCK_STREAM)


async def test_getaddrinfo_rejects_unusable_arguments() -> None:
    loop = asyncio.get_running_loop()
    with pytest.raises(TypeError, match="host must be"):
        await loop.getaddrinfo(object(), 80)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="too long"):
        await loop.getaddrinfo("x" * 2048, 80)


async def test_getaddrinfo_can_be_cancelled() -> None:
    loop = asyncio.get_running_loop()
    task = loop.create_task(loop.getaddrinfo("localhost", 80, type=socket.SOCK_STREAM))  # type: ignore[arg-type]
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.05)


async def test_getnameinfo_resolves_an_address() -> None:
    loop = asyncio.get_running_loop()
    host, service = await loop.getnameinfo(("127.0.0.1", 80), socket.NI_NUMERICHOST | socket.NI_NUMERICSERV)
    assert host == "127.0.0.1"
    assert service == "80"


async def test_getnameinfo_handles_ipv6() -> None:
    loop = asyncio.get_running_loop()
    host, _service = await loop.getnameinfo(("::1", 80, 0, 0), socket.NI_NUMERICHOST | socket.NI_NUMERICSERV)
    assert host == "::1"


async def test_getnameinfo_rejects_a_malformed_address() -> None:
    loop = asyncio.get_running_loop()
    with pytest.raises(TypeError, match="must be a tuple"):
        await loop.getnameinfo("127.0.0.1")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="\\(host, port\\)"):
        await loop.getnameinfo(("127.0.0.1",))
    with pytest.raises(OSError):
        await loop.getnameinfo(("not an address", 80))
