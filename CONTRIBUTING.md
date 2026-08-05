# Contributing

## Getting set up

```console
$ uv venv --python 3.14
$ uv sync --group dev --group bench
$ uv run pytest
```

Building the extension needs Zig 0.16. `uv sync` installs it from the `ziglang`
package, so nothing has to be on your `PATH`; a system Zig is used if you have
one. After changing anything under `zig/`, rebuild before testing:

```console
$ uv pip install -e . --reinstall-package zuvloop
```

## What CI checks

Everything below runs on Linux and macOS, and all of it has to pass:

```console
$ uv run ruff check
$ uv run ruff format --check
$ uv run mypy
$ uv run coverage run -m pytest
$ uv run coverage report          # fails under 100%
```

Coverage is enforced at 100% branch coverage over `zuvloop` and `tests`. A line
that genuinely cannot be reached takes a `# pragma: no cover` with a reason
after it - but check first that it is really unreachable rather than merely
untested, because that comment is also how a dead branch hides.

The Zig is compiled for every target a wheel is published for. It reaches libc
through `std.c`, whose shape differs per target, so code that builds on your
machine can still fail elsewhere:

```console
$ zig build -Dtarget=x86_64-linux-gnu
```

Benchmarks run through CodSpeed on every commit. They are noisy, and a CodSpeed
regression on its own does not block a change.

## Conformance

CPython's own `test_asyncio` runs against this loop:

```console
$ uv run python scripts/conformance.py
```

It downloads the source of whichever interpreter you are running and runs each
test in its own process, so a hang is reported rather than stopping the run.
Everything that is skipped says why in the harness.

## Measuring against uvloop

Do not read `pytest --codspeed`'s table across rows. `pytest-codspeed` divides
by `iter_per_round` twice and picks that divisor per row, so the ratio between
two rows is scaled by an unrelated number. `benchmarks/compare.py` alternates
the loops in one process and is the harness for that question.

## Changes to the native layer

The Zig has no coverage gate, and most of the defects found in this project have
been there - reference counts and C API contracts especially. Two rules carry
most of the weight:

- Finish the mutation before releasing what it held. `Py_DECREF` can run
  arbitrary Python, and that Python can reach the structure being mutated.
- Take contracts from CPython's source rather than from memory. Which functions
  return borrowed references and which return new ones is not guessable.

Claims about behaviour are worth what they are measured at. A comparison against
stock asyncio and uvloop is usually a dozen lines, and this project has a
history of confident reasoning that measurement then contradicted.
