from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any

from ._loop import EventLoop


def new_event_loop() -> EventLoop:
    """Create a libuv-backed event loop."""
    return EventLoop()


def run[T](main: Coroutine[Any, Any, T], *, debug: bool | None = None) -> T:
    """Run `main` on a libuv-backed loop, mirroring `asyncio.run`."""
    with asyncio.Runner(debug=debug, loop_factory=new_event_loop) as runner:
        return runner.run(main)
