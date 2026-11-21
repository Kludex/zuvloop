from __future__ import annotations

import asyncio
import ipaddress
import socket
from collections.abc import Callable
from dataclasses import dataclass

import pytest
from hypothesis import given, settings, strategies as st

import zuvloop
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


@dataclass(frozen=True)
class Soon:
    """Schedule one callback."""

    tag: int


@dataclass(frozen=True)
class SoonThenCancel:
    """Schedule one callback and cancel it straight away; it must never run."""

    tag: int


@dataclass(frozen=True)
class Nested:
    """Schedule a callback that itself schedules `children` more."""

    tag: int
    children: int


Operation = Soon | SoonThenCancel | Nested

programs = st.lists(
    st.one_of(
        st.builds(Soon, tag=st.integers(0, 999)),
        st.builds(SoonThenCancel, tag=st.integers(0, 999)),
        st.builds(Nested, tag=st.integers(0, 999), children=st.integers(1, 3)),
    ),
    min_size=1,
    max_size=24,
)


async def _execute(program: list[Operation]) -> list[str]:
    """Run the program on the running loop and return the order the callbacks were seen in."""
    loop = asyncio.get_running_loop()
    seen: list[str] = []

    def record(label: str) -> None:
        seen.append(label)

    def nested(label: str, children: int) -> None:
        seen.append(label)
        for index in range(children):
            loop.call_soon(record, f"{label}.{index}")

    for position, operation in enumerate(program):
        label = f"{position}:{operation.tag}"
        match operation:
            case Soon():
                loop.call_soon(record, label)
            case SoonThenCancel():
                loop.call_soon(record, label).cancel()
            # `case _` rather than `case Nested()`: `Operation` has no fourth member, and a
            # named last case leaves the match a fall-through arm that cannot be taken,
            # which the 100% branch gate has no way to account for.
            case _:
                loop.call_soon(nested, label, operation.children)

    for _ in range(8):  # enough turns for every generation of nested callbacks to drain
        await asyncio.sleep(0)
    return seen


def _run_under(factory: Callable[[], asyncio.AbstractEventLoop], program: list[Operation]) -> list[str]:
    with asyncio.Runner(loop_factory=factory) as runner:
        return runner.run(_execute(program))


# These are synchronous, unlike the rest of the file: each example has to run the same program
# on two different loops, which cannot be done from inside one of them.


@given(program=programs)
@settings(max_examples=200, deadline=None)
def test_callback_order_matches_asyncio(program: list[Operation]) -> None:
    """asyncio is the oracle: the same program must produce the same observable order."""
    assert _run_under(zuvloop.new_event_loop, program) == _run_under(asyncio.new_event_loop, program)


@given(program=programs)
@settings(max_examples=200, deadline=None)
def test_cancelled_handles_never_run_and_the_rest_keep_their_order(program: list[Operation]) -> None:
    """Stated directly, so a failure names the rule that broke rather than just "differs"."""
    seen = _run_under(zuvloop.new_event_loop, program)

    expected = [
        f"{position}:{operation.tag}"
        for position, operation in enumerate(program)
        if not isinstance(operation, SoonThenCancel)
    ]
    assert [label for label in seen if "." not in label] == expected

    # Ordered, not a set: the parents run in program order and each queues its children
    # in index order, so which child runs when is determined rather than incidental.
    nested_expected = [
        f"{position}:{operation.tag}.{index}"
        for position, operation in enumerate(program)
        if isinstance(operation, Nested)
        for index in range(operation.children)
    ]
    assert [label for label in seen if "." in label] == nested_expected


@given(program=programs)
@settings(max_examples=100, deadline=None)
def test_a_callback_scheduled_from_a_callback_waits_its_turn(program: list[Operation]) -> None:
    """Nothing scheduled from inside a callback may run before the queued batch drains."""
    seen = _run_under(zuvloop.new_event_loop, program)

    # The whole program is queued before the loop starts, so every top-level callback is
    # in the first batch and none of them may be overtaken - not just a child's own
    # parent. Which makes the rule a statement about one boundary: the batch drains, and
    # only then does anything it scheduled run.
    top_level = [label for label in seen if "." not in label]
    assert seen[: len(top_level)] == top_level, (
        "a callback scheduled from inside the batch ran before the batch had drained"
    )
