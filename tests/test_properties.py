from __future__ import annotations

import asyncio
import ipaddress
import socket

import pytest
from hypothesis import given, settings, strategies as st

from tests.conftest import running_loop

pytestmark = pytest.mark.anyio


@given(
    address=st.one_of(st.ip_addresses(v=4), st.ip_addresses(v=6)),
    port=st.integers(min_value=0, max_value=65535),
    socktype=st.sampled_from([socket.SOCK_DGRAM, socket.SOCK_STREAM]),
)
@settings(max_examples=100, deadline=None)
async def test_numeric_address_resolution_matches_socket(
    address: ipaddress.IPv4Address | ipaddress.IPv6Address, port: int, socktype: socket.SocketKind
) -> None:
    family = socket.AF_INET if isinstance(address, ipaddress.IPv4Address) else socket.AF_INET6
    flags = socket.AI_NUMERICHOST | socket.AI_NUMERICSERV
    expected = socket.getaddrinfo(str(address), port, family=family, type=socktype, flags=flags)
    actual = await running_loop().getaddrinfo(str(address), port, family=family, type=socktype, flags=flags)
    assert actual == expected


@given(cancelled=st.sets(st.integers(min_value=0, max_value=63), max_size=64))
@settings(max_examples=100, deadline=None)
async def test_ready_queue_cancellation_matches_the_handle_contract(cancelled: set[int]) -> None:
    seen: list[int] = []
    handles = [running_loop().call_soon(seen.append, index) for index in range(64)]
    for index in cancelled:
        handles[index].cancel()

    await asyncio.sleep(0)
    assert seen == [index for index in range(64) if index not in cancelled]
