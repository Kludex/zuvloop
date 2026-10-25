"""Teardown with work still queued.

Closing a loop has to release whatever is still in the ready queue and the timer heap. Nothing
else in the suite reaches those paths: every other test drains its queue before closing, so the
cleanup loops in `zig/collections.zig` never execute and a mistake in them is invisible.

The queue is a power-of-two ring buffer that starts at 64 entries, so the sizes here cross that
boundary deliberately - below it, with `head` still zero, a wrong index mask reads the right
slots anyway.
"""

from __future__ import annotations

import gc
import sys

import pytest

import zuvloop


class Sentinel:
    """An argument whose reference count is worth watching."""

    __slots__ = ()


@pytest.mark.parametrize("queued", [1, 63, 64, 65, 200])
def test_closing_runs_none_of_the_queued_callbacks(queued: int) -> None:
    loop = zuvloop.new_event_loop()
    seen: list[int] = []
    for index in range(queued):
        loop.call_soon(seen.append, index)
    loop.close()
    assert seen == []


@pytest.mark.parametrize("queued", [1, 64, 200])
def test_closing_releases_the_arguments_of_queued_callbacks(queued: int) -> None:
    sentinel = Sentinel()
    baseline = sys.getrefcount(sentinel)

    loop = zuvloop.new_event_loop()
    for _ in range(queued):
        loop.call_soon(len, sentinel)
    assert sys.getrefcount(sentinel) > baseline, "the queue did not take references"

    loop.close()
    gc.collect()
    assert sys.getrefcount(sentinel) == baseline


@pytest.mark.parametrize("scheduled", [1, 64, 200])
def test_closing_releases_the_arguments_of_scheduled_timers(scheduled: int) -> None:
    sentinel = Sentinel()
    baseline = sys.getrefcount(sentinel)

    loop = zuvloop.new_event_loop()
    for _ in range(scheduled):
        loop.call_later(3600.0, len, sentinel)
    assert sys.getrefcount(sentinel) > baseline, "the timer heap did not take references"

    loop.close()
    gc.collect()
    assert sys.getrefcount(sentinel) == baseline


def test_closing_after_the_queue_has_wrapped() -> None:
    """Close with `head` past zero and the live entries wrapped round the end of the ring.

    `Ready.deinit` walks what it owns as `(head + i) & (capacity - 1)`. While `head` is
    zero that mask changes nothing, so every other test here would pass with it wrong.
    Draining one batch and leaving the next queued is what moves `head` along and puts
    the tail back at the start of the buffer.

    The second batch is queued from inside the first, which is what leaves it pending: a
    pass runs exactly the callbacks that were queued when it started, so `stop()` from
    within that pass ends the loop with the new ones still owned by the queue.
    """
    sentinel = Sentinel()
    baseline = sys.getrefcount(sentinel)

    loop = zuvloop.new_event_loop()
    ran: list[int] = []

    def queue_the_second_batch() -> None:
        for _ in range(80):
            loop.call_soon(len, sentinel)
        loop.stop()

    for index in range(100):
        loop.call_soon(ran.append, index)
    loop.call_soon(queue_the_second_batch)
    loop.run_forever()

    # 101 callbacks ran, so `head` is 101 into a 128-entry buffer and the 80 that follow
    # begin at 101 and wrap at 128. Both assertions matter: the first says the second
    # batch did not run, the second that the queue is still holding its arguments.
    assert ran == list(range(100)), "the first batch should have run, and only it"
    assert sys.getrefcount(sentinel) > baseline, "the queue did not keep the arguments"

    loop.close()
    gc.collect()
    assert sys.getrefcount(sentinel) == baseline
    assert ran == list(range(100)), "closing ran callbacks that should have been dropped"
