"""Measure native callback timing overhead with a tracing provider installed."""

from __future__ import annotations

import argparse
import statistics
import time

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider

import zuvloop


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpu-time", action="store_true")
    args = parser.parse_args()
    trace.set_tracer_provider(TracerProvider())
    loop = zuvloop.new_event_loop()
    loop.slow_callback_duration = 1000000
    if args.cpu_time:
        loop.slow_callback_cpu_time_enabled = True
    samples = []
    for _ in range(9):
        remaining = 200000

        def callback() -> None:
            nonlocal remaining
            remaining -= 1
            if remaining:
                loop.call_soon(callback)
            else:
                loop.stop()

        loop.call_soon(callback)
        started = time.perf_counter_ns()
        loop.run_forever()
        samples.append((time.perf_counter_ns() - started) / 200000)
    loop.close()
    print({"cpu_time": args.cpu_time, "median_ns_per_callback": statistics.median(samples), "samples": samples})


if __name__ == "__main__":
    main()
