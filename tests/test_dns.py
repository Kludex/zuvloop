from __future__ import annotations

import asyncio
import socket
from collections.abc import Sequence

import pytest

from tests.conftest import running_loop
from zuvloop import new_event_loop

pytestmark = pytest.mark.anyio

type Sockaddr = tuple[str, int] | tuple[str, int, int, int] | tuple[int, bytes]
type AddrInfo = tuple[int, int, int, str, Sockaddr]
# What a lookup produced: the addresses, or the `(errno, strerror)` it failed with.
# `Sequence` rather than `list` because the two APIs disagree on how narrowly they
# type the family and the address, and only a covariant container accepts both.
type Outcome = Sequence[AddrInfo] | tuple[int, str]


async def test_getaddrinfo_resolves_localhost() -> None:
    loop = running_loop()
    results = await loop.getaddrinfo("localhost", 80, type=socket.SOCK_STREAM)
    assert results
    for family, kind, proto, canonname, sockaddr in results:
        assert family in (socket.AF_INET, socket.AF_INET6)
        assert kind is socket.SOCK_STREAM
        assert isinstance(proto, int)
        # Whether libc fills the canonical name in without `AI_CANONNAME` is its
        # own business: musl answers `localhost` here where glibc and BSD leave
        # it empty. Agreeing with the standard library is asserted below.
        assert isinstance(canonname, str)
        assert sockaddr[1] == 80


async def test_getaddrinfo_accepts_a_service_name() -> None:
    loop = running_loop()
    results = await loop.getaddrinfo("127.0.0.1", "http", family=socket.AF_INET, type=socket.SOCK_STREAM)
    assert results[0][4] == ("127.0.0.1", 80)


async def test_getaddrinfo_accepts_bytes() -> None:
    loop = running_loop()
    results = await loop.getaddrinfo(b"127.0.0.1", b"80", family=socket.AF_INET, type=socket.SOCK_STREAM)
    assert results[0][4] == ("127.0.0.1", 80)


async def test_getaddrinfo_binds_a_wildcard_address() -> None:
    loop = running_loop()
    results = await loop.getaddrinfo(None, 0, family=socket.AF_INET, type=socket.SOCK_STREAM, flags=socket.AI_PASSIVE)
    assert results[0][4] == ("0.0.0.0", 0)


async def test_getaddrinfo_returns_canonical_names() -> None:
    loop = running_loop()
    results = await loop.getaddrinfo(
        "localhost", 80, family=socket.AF_INET, type=socket.SOCK_STREAM, flags=socket.AI_CANONNAME
    )
    assert results[0][3] != ""


async def test_getaddrinfo_reports_failures() -> None:
    """The exception the standard library raises, carrying the code it carries.

    libuv numbers its resolver errors itself, and those numbers are neither the
    platform's `EAI_*` nor stable across platforms, so a caller comparing against
    `socket.EAI_NONAME` would never match.
    """
    loop = running_loop()
    with pytest.raises(socket.gaierror) as caught:
        await loop.getaddrinfo("this-host-should-not-exist.invalid", 80, type=socket.SOCK_STREAM)
    assert caught.value.errno in (socket.EAI_NONAME, socket.EAI_NODATA)

    with pytest.raises(socket.gaierror) as stdlib:
        socket.getaddrinfo("this-host-should-not-exist.invalid", 80, type=socket.SOCK_STREAM)
    assert caught.value.args == stdlib.value.args


@pytest.mark.parametrize(
    ("host", "port"),
    [
        ("localhost", "http"),
        ("this-host-should-not-exist.invalid", "http"),
        # What `""` means is the platform's business, and the platforms disagree:
        # BSD reads it as the null host and resolves the wildcard address, glibc
        # calls it a name it cannot find. libuv reaches neither, because it runs
        # every hostname through IDNA first and that rejects an empty one. Each
        # service form reaches libc differently, and the null one is what CPython
        # carries a macOS workaround for.
        ("", "http"),
        ("", "80"),
        ("", None),
    ],
)
async def test_getaddrinfo_answers_as_the_stdlib_does(host: str, port: str | None) -> None:
    loop = running_loop()
    kwargs = {"family": socket.AF_INET, "type": socket.SOCK_STREAM}
    try:
        theirs: Outcome = sorted(socket.getaddrinfo(host, port, **kwargs))
    except socket.gaierror as exc:
        theirs = exc.args
    try:
        mine: Outcome = sorted(await loop.getaddrinfo(host, port, **kwargs))
    except socket.gaierror as exc:
        mine = exc.args
    assert mine == theirs


async def test_getaddrinfo_without_a_host_or_a_port_reports_no_name() -> None:
    """libuv rejects the pair as EINVAL; the resolver would call it a missing name."""
    loop = running_loop()
    with pytest.raises(socket.gaierror) as caught:
        await loop.getaddrinfo(None, None)
    with pytest.raises(socket.gaierror) as stdlib:
        socket.getaddrinfo(None, None)
    assert caught.value.args == stdlib.value.args


def test_getaddrinfo_rechecks_the_loop_after_argument_conversion() -> None:
    loop = new_event_loop()

    class ClosingFamily:
        def __index__(self) -> int:
            loop.close()
            return int(socket.AF_UNSPEC)

    call = loop.getaddrinfo("localhost", 80, family=ClosingFamily())  # type: ignore[arg-type]
    with pytest.raises(RuntimeError, match="Event loop is closed"):
        call.send(None)


@pytest.mark.parametrize(
    "flags",
    [
        # Contradictory, and the resolver says so - the failure path.
        socket.NI_NAMEREQD | socket.NI_NUMERICHOST,
        # A real reverse lookup, answered from the hosts file - the success path.
        socket.NI_NAMEREQD,
    ],
)
async def test_getnameinfo_answers_as_the_stdlib_does(flags: int) -> None:
    """Whether a reverse lookup succeeds is the resolver's business; agreeing is not."""
    loop = running_loop()
    try:
        theirs: tuple[str, str] | tuple[int, str] = socket.getnameinfo(("127.0.0.1", 80), flags)
    except socket.gaierror as exc:
        theirs = exc.args
    try:
        mine: tuple[str, str] | tuple[int, str] = await loop.getnameinfo(("127.0.0.1", 80), flags)
    except socket.gaierror as exc:
        mine = exc.args
    assert mine == theirs


async def test_getaddrinfo_rejects_unusable_arguments() -> None:
    loop = running_loop()
    with pytest.raises(TypeError, match="host must be"):
        await loop.getaddrinfo(object(), 80)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="too long"):
        await loop.getaddrinfo("x" * 2048, 80)


async def test_getaddrinfo_can_be_cancelled() -> None:
    loop = running_loop()
    task = loop.create_task(loop.getaddrinfo("localhost", 80, type=socket.SOCK_STREAM))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.05)


async def test_getnameinfo_resolves_an_address() -> None:
    loop = running_loop()
    host, service = await loop.getnameinfo(("127.0.0.1", 80), socket.NI_NUMERICHOST | socket.NI_NUMERICSERV)
    assert host == "127.0.0.1"
    assert service == "80"


async def test_getnameinfo_handles_ipv6() -> None:
    loop = running_loop()
    host, _service = await loop.getnameinfo(("::1", 80, 0, 0), socket.NI_NUMERICHOST | socket.NI_NUMERICSERV)
    assert host == "::1"


async def test_getnameinfo_rejects_a_malformed_address() -> None:
    """A bad argument stays the caller's mistake; only the host is the resolver's."""
    loop = running_loop()
    with pytest.raises(TypeError, match="must be a tuple"):
        await loop.getnameinfo("127.0.0.1")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="\\(host, port\\)"):
        await loop.getnameinfo(("127.0.0.1",))
    with pytest.raises(socket.gaierror):
        await loop.getnameinfo(("not an address", 80))
    with pytest.raises(ValueError, match="embedded null character"):
        await loop.getnameinfo(("127.0.0.1\0hidden", 80))


@pytest.mark.parametrize("host", ["localhost", "example.com", "not a host"])
async def test_getnameinfo_reports_a_name_it_cannot_use_as_the_stdlib_does(host: str) -> None:
    """libuv refuses to parse a non-literal host and reports `EINVAL`, where the
    standard library raises `socket.gaierror`."""
    loop = running_loop()
    with pytest.raises(socket.gaierror) as mine:
        await loop.getnameinfo((host, 80))
    with pytest.raises(socket.gaierror) as theirs:
        socket.getnameinfo((host, 80), 0)
    assert mine.value.args == theirs.value.args
