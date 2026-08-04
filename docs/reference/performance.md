# Performance

Measured with `benchmarks/run.py`, `benchmarks/uvicorn_bench.py`,
`benchmarks/aiohttp_bench.py` and `benchmarks/write_batching.py`, on an M3 Max
running macOS 26 and CPython 3.14. Rounds are interleaved across loops and the
best of each is reported, with the run-to-run spread beside it.

| Benchmark | asyncio | uvloop | zuvloop | zuvloop / uvloop |
| --- | ---: | ---: | ---: | ---: |
| `call_soon` | 2.69M/s | 4.69M/s | **5.91M/s** | **1.26x** |
| `call_soon` with arguments | 2.43M/s | 3.87M/s | **6.47M/s** | **1.67x** |
| timer schedule + cancel | 1.58M/s | 2.62M/s | **9.55M/s** | **3.64x** |
| bulk stream | 8.4 GiB/s | 8.5 GiB/s | **10.2 GiB/s** | **1.20x** |
| loop iterations (`sleep(0)`) | 73.2k/s | 78.2k/s | 78.2k/s | 1.00x |
| echo round trips, 1 KiB | 39.0k/s | 56.8k/s | **58.5k/s** | **1.03x** |
| uvicorn, plaintext | 55.3k req/s | 71.9k req/s | **75.9k req/s** | **1.06x** |
| uvicorn, 10 KiB body | 52.6k req/s | 68.7k req/s | **73.6k req/s** | **1.07x** |
| aiohttp server | 49.0k req/s | 59.8k req/s | **60.6k req/s** | **1.01x** |
| aiohttp client | 13.2k req/s | 16.2k req/s | **16.9k req/s** | **1.04x** |
| UDP round trips, 512 B | 39.8k/s | 53.1k/s | **54.2k/s** | **1.02x** |
| `getaddrinfo`, numeric host | 28.5k/s | 1.57M/s | **1.90M/s** | **1.21x** |

## Reading these numbers

**Stock asyncio is the control.** A benchmark run is only trustworthy if
asyncio's number reproduces its known value; when it does not, nothing else in
the run means anything either. That check is what caught a run where uvicorn was
silently measured against its pure-Python parser instead of `httptools` — every
loop was a third of its real throughput, which reads exactly like a regression.

`benchmarks/uvicorn_bench.py` prints which parser it found, for that reason.

**Spreads matter more than ratios.** `aiohttp server` at 1.01x is parity, not a
win. `call_soon` with arguments has the widest spread in the suite; read its
1.67x loosely.

## Where the wins come from

**Scheduling and timers** are where the design differs most. Arguments live
inside the handle rather than in a tuple, and timers share one `uv_timer_t`
behind a heap instead of taking a libuv handle each.

**Writes are batched per turn.** `benchmarks/write_batching.py` isolates this by
serving one fixed response two ways — as a single `write()`, and as the header
write plus body write that ASGI and aiohttp actually do:

| response split into two writes | asyncio | uvloop | zuvloop |
| --- | ---: | ---: | ---: |
| throughput lost | -36% | -0.1% | **-0.7%** |

A loop that writes each piece as it arrives spends a syscall per piece. That
single difference is most of the gap on the HTTP rows.

**Address literals never reach libc.** `inet_pton` is an order of magnitude
cheaper than `getaddrinfo`, and the enum members every result tuple needs are
cached rather than looked up through `EnumMeta.__call__` at over a hundred
nanoseconds each.

## Running them

```console
$ uv run --group bench python benchmarks/run.py
$ uv run --group bench python benchmarks/uvicorn_bench.py
$ uv run --group bench python benchmarks/aiohttp_bench.py
$ uv run --group bench python benchmarks/write_batching.py
```

The HTTP benchmarks need [`oha`](https://github.com/hatoo/oha) on your `PATH`.
