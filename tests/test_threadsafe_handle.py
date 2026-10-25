"""The cancel protocol on a thread-safe handle, driven from foreign threads.

`call_soon_threadsafe` hands a handle to the loop, and any number of other threads may then
call `cancel()` on it. `zig/tshandle.zig` keeps an atomic state word - PENDING, RUNNING,
DONE - and a list of parked waiters that the loop thread releases once the callback has
finished, so a `cancel()` that arrives mid-run blocks until the callback it was too late to
stop is over.

Two things hold that together, and neither is visible at the call site:

* `cancel` is registered with `py.methodNoArgs`, whose wrapper holds
  `PyCriticalSection(handle)` across the call. That is what makes "read the state word,
  then link a waiter" atomic against the loop thread draining the list.
* `awaitCompletion` then calls `PyEval_SaveThread()`, releasing that section, before it
  blocks. That is what stops the two sides deadlocking on each other.

`verification/ThreadSafeHandle.tla` states the same protocol, and TLC finds a lost wakeup
if the first of those is removed. These tests are the runtime half, on a real
free-threaded build, where the symptom would be a thread that never returns rather than a
wrong answer.

Which is why each test first reaches the window it aims at and says so. A `cancel()` that
arrives before the loop picks the handle up wins outright, and one that arrives after the
callback is over returns without parking; a test that only checked "nothing broke" would
pass having exercised neither the waiter list nor the release.
"""

from __future__ import annotations

import asyncio
import threading
import time

import pytest

import zuvloop

# Every wait here is bounded. The failure being looked for is a thread that never returns,
# and a test that hangs instead of failing reports nothing.
WAIT = 10.0

# Long enough that a `cancel()` which wrongly returned straight away would have recorded
# itself before the callback looks.
GRACE = 0.05


def join_all(threads: list[threading.Thread]) -> None:
    """Bounded joins. A stranded waiter has to fail an assertion, not hang the suite."""
    for thread in threads:
        thread.join(WAIT)


@pytest.mark.parametrize("cancelling", [True, False], ids=["cancel", "cancelled"])
@pytest.mark.parametrize("callers", [1, 2, 8, 32])
def test_a_call_arriving_mid_run_waits_for_the_callback(callers: int, cancelling: bool) -> None:
    """Both blocking methods wait while the callback is running, and both then return.

    `cancel()` and `cancelled()` each wait out a run in progress on another thread, and they
    reach that through separate call sites in `tshandle.zig`, so both are exercised here.

    The callback is the observer: it waits until every caller has announced it is about to
    call, pauses, and only then records who has returned. Nobody should have, because the
    handle is RUNNING and they are all parked on it. That check has to happen from inside
    the callback - the loop thread is busy running it, so nothing scheduled on the loop
    could look at the right moment.
    """
    loop = zuvloop.new_event_loop()
    entered = threading.Event()
    about_to_call = threading.Semaphore(0)
    returned: list[float] = []
    returned_while_running: list[float] = []
    finished: list[float] = []

    def slow_callback() -> None:
        entered.set()
        for _ in range(callers):
            about_to_call.acquire(timeout=WAIT)
        time.sleep(GRACE)
        returned_while_running.extend(returned)
        finished.append(time.monotonic())
        loop.stop()

    handle = loop.call_soon_threadsafe(slow_callback)

    def caller() -> None:
        # Wait for RUNNING, so this call has to park rather than win the race.
        entered.wait(WAIT)
        about_to_call.release()
        if cancelling:
            handle.cancel()
        else:
            handle.cancelled()
        returned.append(time.monotonic())

    threads = [threading.Thread(target=caller, name=f"caller-{index}") for index in range(callers)]
    for thread in threads:
        thread.start()
    loop.run_forever()
    join_all(threads)
    loop.close()

    assert len(finished) == 1, "the callback did not run exactly once"
    assert returned_while_running == [], "a call returned while the callback was still running"
    assert [thread.name for thread in threads if thread.is_alive()] == [], "a call never returned"
    assert len(returned) == callers
    assert min(returned) >= finished[0], "a call returned before the callback it waited for"
    assert handle.cancelled() is cancelling


def test_a_cancel_arriving_first_stops_the_callback() -> None:
    """The other outcome: a cancel that lands before the loop reaches the handle prevents the run.

    The cancels happen while the loop is not yet turning, which makes the ordering a fact.
    Racing them against a running loop instead looks equivalent and is not: the loop drains
    a whole batch in one pass, so whether the cancels land first depends on how quickly the
    cancelling thread is scheduled. Written that way this test passed normally and failed
    under `coverage run`, which is the same bug either way.
    """
    loop = zuvloop.new_event_loop()
    ran: list[int] = []
    handles = [loop.call_soon_threadsafe(ran.append, index) for index in range(64)]
    # Last in the batch, and a batch runs to its end, so this stops the loop once the 64
    # have had their turn.
    loop.call_soon_threadsafe(loop.stop)

    def cancel_every_other() -> None:
        for index in range(0, 64, 2):
            handles[index].cancel()

    thread = threading.Thread(target=cancel_every_other)
    thread.start()
    join_all([thread])
    loop.run_forever()
    loop.close()

    assert all(handles[index].cancelled() for index in range(0, 64, 2))
    assert not any(handles[index].cancelled() for index in range(1, 64, 2))
    assert ran == [index for index in range(64) if index % 2], "a cancelled handle ran, or a survivor did not"


def test_racing_cancels_never_produce_a_second_run() -> None:
    """Race a cancel against the loop picking the handle up, repeatedly.

    Where the two tests above each pin one outcome deliberately, this one takes whichever
    the machine gives and holds the invariants that must survive either: a callback runs at
    most once, and `cancelled()` agrees once `cancel()` has returned.
    """
    loop = zuvloop.new_event_loop()
    runs: list[int] = []
    agreed: list[bool] = []

    async def main() -> None:
        running = asyncio.get_running_loop()
        for round_index in range(200):
            ran: list[int] = []
            handle = running.call_soon_threadsafe(ran.append, round_index)

            def cancel_and_report(target: asyncio.Handle = handle) -> None:
                target.cancel()
                agreed.append(target.cancelled())

            thread = threading.Thread(target=cancel_and_report)
            thread.start()
            await asyncio.sleep(0)
            await asyncio.to_thread(thread.join, WAIT)
            for _ in range(4):
                await asyncio.sleep(0)
            runs.append(len(ran))

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()

    assert len(agreed) == 200, "a cancel() never returned"
    assert all(agreed), "cancel() returned but cancelled() denied it"
    assert max(runs) <= 1, "a callback ran more than once"


def test_a_callback_cancelling_its_own_handle_does_not_deadlock() -> None:
    """From the running thread there is nothing to wait for, so neither call may block.

    `mustWait` says so with `running_payload != self`. Drop that and a callback cancelling
    its own handle parks on a run only it could finish, which is a deadlock rather than a
    wrong answer - and `Handle.cancel()` from inside the callback is ordinary asyncio.
    """
    loop = zuvloop.new_event_loop()
    handles: list[asyncio.Handle] = []
    observed: list[bool] = []

    def callback() -> None:
        handles[0].cancel()
        observed.append(handles[0].cancelled())
        loop.stop()

    handles.append(loop.call_soon_threadsafe(callback))
    loop.run_forever()
    loop.close()

    assert observed == [True], "cancelling from inside the callback did not take effect"


def test_racing_cancelled_against_the_end_of_a_run_loses_no_waiter() -> None:
    """Arrive while the loop is already releasing the waiters, not safely before it.

    In the test above every caller parks before the callback finishes, so only one side
    ever touches the waiter list at a time and an unsynchronised link would still work.
    The hazard is the overlap: a waiter linked while the drain is already walking the chain
    is a thread nobody will release. So here the callback ends as soon as it starts and the
    threads arrive whenever they arrive, across enough rounds to land inside the window.
    """
    loop = zuvloop.new_event_loop()
    stuck: list[str] = []

    async def main() -> None:
        running = asyncio.get_running_loop()
        for round_index in range(400):
            handle = running.call_soon_threadsafe(round_index.bit_length)
            threads = [
                threading.Thread(target=handle.cancelled, name=f"round-{round_index}-{index}") for index in range(4)
            ]
            for thread in threads:
                thread.start()
            await asyncio.sleep(0)
            # Joined off the loop thread: a waiter that was dropped is waiting on this
            # loop, so joining here would hang instead of failing.
            await asyncio.to_thread(join_all, threads)
            stuck.extend(thread.name for thread in threads if thread.is_alive())

    try:
        loop.run_until_complete(main())
    finally:
        loop.close()

    assert stuck == [], f"cancelled() never returned on {stuck}"
