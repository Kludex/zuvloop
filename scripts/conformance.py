"""Run CPython's own `test_asyncio` loop conformance mixins against zuvloop.

CPython's test suite is not shipped with the interpreter, so this fetches the
source tarball matching the running interpreter and runs the mixins out of it.
Nothing is vendored: the suite tracks whichever Python is being tested.

    uv run python scripts/conformance.py            # run everything
    uv run python scripts/conformance.py -v         # per-test results
    uv run python scripts/conformance.py -k pipe    # a subset

Each test runs in its own interpreter, so a test that hangs is reported as a
result rather than stopping the run.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import tarfile
import unittest
import urllib.request
from pathlib import Path

CACHE = Path(__file__).resolve().parent.parent / ".conformance"
VERSION = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
URL = f"https://www.python.org/ftp/python/{VERSION}/Python-{VERSION}.tgz"
MODULE = "test.test_asyncio.test_zuvloop_conformance"

# Only what the asyncio suite reaches for, so the download stays a few megabytes
# rather than the whole tree.
WANTED = ("Lib/test/test_asyncio", "Lib/test/support", "Lib/test/certdata", "Lib/test/__init__.py")

HARNESS = '''\
"""CPython's loop conformance mixins, bound to zuvloop.

`SubprocessTestsMixin` has no `setUp`; every stdlib user composes it with
`EventLoopTestsMixin`, which is what builds `self.loop`.

Three tests are skipped. All three are white-box tests of CPython's own loop
internals rather than of observable behaviour, so no third-party loop can pass
them - which is not a reason to skip a test lightly, so each one says why.
"""
import unittest
import zuvloop
from test.test_asyncio import utils as test_utils
from test.test_asyncio import test_events, test_sock_lowlevel

_MIXIN = test_events.EventLoopTestsMixin


def _inapplicable(name, reason):
    return unittest.skip(reason)(getattr(_MIXIN, name))


class ZuvloopEventLoopTests(test_events.EventLoopTestsMixin,
                            test_events.SubprocessTestsMixin,
                            test_utils.TestCase):
    def create_event_loop(self):
        return zuvloop.new_event_loop()

    # Patches `asyncio.base_events.socket`, which only rebinds the name inside
    # that stdlib module, and stubs `loop._start_serving`, which only the stdlib
    # Server calls. The sockets it asserts on are mocks: it reads `getsockbyname`,
    # which real sockets do not have. The behaviour it targets - repeated hosts
    # collapsing to one socket - is covered in this repo's own suite instead.
    test_create_server_multiple_hosts_ipv4 = _inapplicable(
        "test_create_server_multiple_hosts_ipv4",
        "mocks asyncio.base_events.socket and loop._start_serving",
    )
    test_create_server_multiple_hosts_ipv6 = _inapplicable(
        "test_create_server_multiple_hosts_ipv6",
        "mocks asyncio.base_events.socket and loop._start_serving",
    )
    # Replaces `loop._run_once` to count how often `BaseEventLoop` rounds the
    # `select()` timeout. zuvloop iterates inside `uv_run`, so there is no such
    # callable to replace and the assertion would be vacuous.
    test_timeout_rounding = _inapplicable(
        "test_timeout_rounding",
        "counts BaseEventLoop._run_once select() rounding; zuvloop iterates in uv_run",
    )


class ZuvloopSockTests(test_sock_lowlevel.BaseSockTestsMixin, test_utils.TestCase):
    def create_event_loop(self):
        return zuvloop.new_event_loop()


if __name__ == "__main__":
    unittest.main()
'''


def lib_root() -> Path:
    """The `Lib` directory of a matching CPython source tree, downloading it once."""
    root = CACHE / f"Python-{VERSION}" / "Lib"
    if not root.exists():
        CACHE.mkdir(exist_ok=True)
        tarball = CACHE / f"Python-{VERSION}.tgz"
        if not tarball.exists():
            print(f"downloading {URL}", file=sys.stderr)
            urllib.request.urlretrieve(URL, tarball)
        prefix = f"Python-{VERSION}/"
        with tarfile.open(tarball) as archive:
            wanted = [f"{prefix}{name}" for name in WANTED]
            members = [m for m in archive.getmembers() if m.name.startswith(tuple(wanted))]
            archive.extractall(CACHE, members=members, filter="data")
    (root / "test" / "test_asyncio" / "test_zuvloop_conformance.py").write_text(HARNESS)
    return root


def collect(root: Path, pattern: str | None) -> list[str]:
    sys.path.insert(0, str(root))
    try:
        suite = unittest.defaultTestLoader.loadTestsFromName(MODULE)
    finally:
        sys.path.pop(0)
    found: list[str] = []

    def walk(item: object) -> None:
        if isinstance(item, unittest.TestSuite):
            for child in item:
                walk(child)
        else:
            found.append(item.id())  # type: ignore[attr-defined]

    walk(suite)
    return [t for t in found if pattern is None or pattern in t]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-k", dest="pattern", help="only tests whose id contains this")
    parser.add_argument("-v", dest="verbose", action="store_true", help="report every test")
    parser.add_argument("--timeout", type=float, default=30.0, help="per-test seconds")
    options = parser.parse_args()

    root = lib_root()
    tests = collect(root, options.pattern)
    passed, skipped, failed, hung = 0, 0, [], []

    for index, test in enumerate(tests, 1):
        name = test.rsplit(".", 2)[-2] + "." + test.rsplit(".", 1)[-1]
        try:
            done = subprocess.run(
                [sys.executable, "-m", "unittest", "-q", test],
                capture_output=True,
                text=True,
                timeout=options.timeout,
                cwd=root,
            )
        except subprocess.TimeoutExpired:
            hung.append(name)
            print(f"[{index:3d}/{len(tests)}] HANG {name}", flush=True)
            continue
        output = done.stdout + done.stderr
        if done.returncode == 0:
            if "skipped" in output:
                skipped += 1
            else:
                passed += 1
            if options.verbose:
                print(f"[{index:3d}/{len(tests)}] ok   {name}", flush=True)
        else:
            reason = next(
                (
                    line
                    for line in reversed(output.splitlines())
                    if line and not line.startswith(("-", "=", "Ran ", "FAILED", "OK"))
                ),
                "",
            )
            failed.append((name, reason))
            print(f"[{index:3d}/{len(tests)}] FAIL {name}\n         {reason[:140]}", flush=True)

    print(f"\npassed={passed} skipped={skipped} failed={len(failed)} hung={len(hung)} total={len(tests)}")
    for name in hung:
        print(f"  HANG {name}")
    return 1 if failed or hung else 0


if __name__ == "__main__":
    raise SystemExit(main())
