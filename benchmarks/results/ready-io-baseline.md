# Ready callbacks and I/O fairness

```sh
uv sync --locked --group dev --group bench
uv run --group bench python -m benchmarks.ready_io_compare --loop zuvloop
uv run pytest benchmarks/test_benchmarks.py -k ready_chain --timeout=60
```

You can select `asyncio` or `uvloop` with `--loop`. The script writes JSON lines.
It runs two warmups and nine measured rounds per case. Connections remain open
across rounds, and their setup stays outside the throughput measurement.
Garbage collection uses Python's defaults.

The ready chain yields 10,000 times through `asyncio.sleep(0)`. Connection count
zero creates no listener. Counts 1 and 250 include a listener and both ends of
each idle TCP connection.

The socket probe keeps one callback chain ready and sends one byte whenever the
previous byte has been read. It records 200 send-to-reader delays per round.
The 100-microsecond case adds busy work to each callback. It exposes scheduling
delays that a throughput benchmark with idle connections cannot show.
These are synthetic readiness delays, not network service latency bounds.

CodSpeed records the elapsed time for each workload. The separate script reports
the median and 95th percentile of individual read delays, plus the callbacks that
ran between sending and reading. Socket creation stays outside the measurement;
reader registration and cleanup are included in the CodSpeed workload.

## Baseline

Measured on 2026-09-08 at `b1b90098ec16fac5d527b154dd79ca6ac27ff7ff`, with an Apple
M3 Max, macOS 26.6.2, CPython 3.14.6 with the GIL, Zig 0.16.0 `ReleaseFast`, and
libuv 1.51.0. Values below are medians across the nine rounds.
You can inspect every round in [the raw results](ready-io-baseline.jsonl).

| Idle connections | Ready-chain iterations/second |
| ---: | ---: |
| 0 | 2,065,564 |
| 1 | 525,552 |
| 250 | 383,722 |

| Callback work | Median read delay | 95th percentile | Callbacks before read |
| --- | ---: | ---: | ---: |
| None | 2.42 us | 3.79 us | 8 |
| 100 us | 904.54 us | 915.00 us | 8 |
