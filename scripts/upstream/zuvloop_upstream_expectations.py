"""Documented exceptions to otherwise strict upstream compatibility runs."""

import pytest

EXPECTED_FAILURES = {
    "tests/test_compat.py::test_asyncio_run__default_loop_factory": (
        "the suite asks asyncio.run for its platform default, which this compatibility run intentionally replaces"
    ),
    "tests/test_web_functional.py::test_keepalive_expires_on_time[pyloop]": (
        "zuvloop's native timer heap intentionally does not follow a monkeypatched loop.time method"
    ),
}

SKIPPED_TESTS = {
    "tests/test_client_ws_functional.py::test_concurrent_close_multiple_tasks[pyloop]": (
        "the assertion depends on selector-loop ready/I/O ordering and also fails intermittently on upstream uvloop"
    ),
}


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    for item in items:
        skipped_reason = SKIPPED_TESTS.get(item.nodeid)
        if skipped_reason is not None:
            item.add_marker(pytest.mark.skip(reason=skipped_reason))
            continue
        reason = EXPECTED_FAILURES.get(item.nodeid)
        if reason is not None:
            item.add_marker(pytest.mark.xfail(reason=reason, strict=True))
