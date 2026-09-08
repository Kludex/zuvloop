# Bounded ready-queue batching

```sh
uv sync --locked --group dev --group bench
uv run --group bench python -m benchmarks.ready_io_compare --loop zuvloop --rounds 3
```

You can run this command in separate checkouts of `c1597ff` and `a77ced4` to
compare the baseline and batching change. Both use the same benchmark code.
See [the workload definitions and original baseline](ready-io-baseline.md).

The loop tracks initialized external handles and pending closes as handles open
and close. You no longer pay for a handle scan while the ready queue drains.
Failed opens and failed process spawns remain counted until their close callbacks.

With external I/O, the loop drains up to 256 single-callback batches and checks a
25-microsecond budget between batches. The time budget prevents a sequence of
slow callbacks from consuming all 256 batches before polling. A callback cannot
be interrupted, and the current batch still completes when you call `stop()`.
Timers, DNS requests, pending write flushes, and closing handles end batching.
Without external I/O, the existing 64-batch limit remains.

## Measurements

Measured on 2026-09-08 with an Apple M3 Max, macOS 26.6.2, CPython 3.14.6 with the
GIL, Zig 0.16.0 `ReleaseFast`, and libuv 1.51.0. Three process pairs ran in the
order baseline/batching, batching/baseline, baseline/batching. Each process ran
two warmups and three measured rounds per case, giving nine samples per variant.
Garbage collection used Python's defaults. Each JSON line records the variant,
commit, and process-pair index in [the raw results](ready-io-batching.jsonl).

| Idle connections | Baseline iterations/s | Batching iterations/s | Ratio |
| ---: | ---: | ---: | ---: |
| 0 | 1,859,053 | 1,878,728 | 1.01x |
| 1 | 487,135 | 2,242,152 | 4.60x |
| 250 | 331,298 | 2,262,848 | 6.83x |

| Callback work | Baseline median read delay | Batching median read delay | Baseline p95 | Batching p95 |
| --- | ---: | ---: | ---: | ---: |
| None | 2.54 us | 25.96 us | 5.83 us | 26.75 us |
| 100 us | 908.60 us | 202.81 us | 917.96 us | 209.42 us |

Values are medians across the nine rounds, including the per-round p95 values.
The 250-connection throughput samples ranged from 0.318M to 0.337M/s for the
baseline and 2.117M to 2.428M/s for batching. The 0- and 1-connection cases had
larger outliers; these measurements do not establish a no-I/O speedup.

The throughput gain costs about 23 microseconds of median synthetic read delay
with fast callbacks. Slow callbacks yield sooner. These observations describe
this workload and machine; the 25-microsecond budget is not a latency guarantee.
