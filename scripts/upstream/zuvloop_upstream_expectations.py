"""Strict expected failures for upstream tests that assert another loop's identity."""

import pytest

EXPECTED_FAILURES = {
    "tests/test_compat.py::test_asyncio_run__default_loop_factory": (
        "the suite asks asyncio.run for its platform default, which this compatibility run intentionally replaces"
    ),
    "tests/test_web_functional.py::test_keepalive_expires_on_time[pyloop]": (
        "zuvloop's native timer heap intentionally does not follow a monkeypatched loop.time method"
    ),
}


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        reason = EXPECTED_FAILURES.get(item.nodeid)
        if reason is not None:
            item.add_marker(pytest.mark.xfail(reason=reason, strict=True))
