"""Measure the peak resident memory added by a queue of callback handles.

Each measurement runs in a child process because `ru_maxrss` is a process-wide
high-water mark. The callbacks remain queued while memory is sampled.

    uv run --group bench python benchmarks/handle_queue.py
    uv run --group bench python benchmarks/handle_queue.py --depths 1000000
"""

from __future__ import annotations

import argparse
import asyncio
import resource
import subprocess
import sys
from collections.abc import Callable

import zuvloop

Factory = Callable[[], asyncio.AbstractEventLoop]


def main() -> int:
    factories: dict[str, Factory] = {"asyncio": asyncio.new_event_loop, "zuvloop": zuvloop.new_event_loop}
    try:
        import uvloop
    except ImportError:
        pass
    else:
        factories["uvloop"] = uvloop.new_event_loop

    parser = argparse.ArgumentParser(description="Measure callback queue memory by event loop.")
    parser.add_argument("--depths", nargs="+", type=int, default=[100_000, 1_000_000])
    parser.add_argument("--only", nargs="+", choices=factories, default=list(factories))
    parser.add_argument("--measure", choices=factories, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if any(depth < 1 for depth in args.depths):
        parser.error("depths must be positive")

    if args.measure is not None:
        loop = factories[args.measure]()
        scale = 1 if sys.platform == "darwin" else 1024
        baseline = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * scale

        def callback() -> None:
            pass

        try:
            for _ in range(args.depths[0]):
                loop.call_soon(callback)
            peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * scale
        finally:
            loop.close()
        added = max(0, peak - baseline)
        print(args.measure, args.depths[0], added)
        return 0

    print(f"python {sys.version.split()[0]}\n")
    for depth in args.depths:
        print(f"{depth:,} queued callbacks")
        for label in args.only:
            result = subprocess.run(
                [sys.executable, __file__, "--measure", label, "--depths", str(depth)],
                check=True,
                capture_output=True,
                text=True,
            )
            measured_label, measured_depth, child_added = result.stdout.split()
            if measured_label != label or int(measured_depth) != depth:
                raise RuntimeError(f"unexpected child result: {result.stdout!r}")
            added_bytes = int(child_added)
            print(f"  {label:<8}{added_bytes / (1 << 20):>9.1f} MiB  {added_bytes / depth:>6.1f} bytes/callback")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
