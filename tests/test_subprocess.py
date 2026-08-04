from __future__ import annotations

import asyncio
import os
import signal
import sys
from typing import Any

import pytest

from conftest import running_loop

pytestmark = pytest.mark.anyio

PIPE = asyncio.subprocess.PIPE


async def test_a_command_runs_and_reports_its_output() -> None:
    process = await asyncio.create_subprocess_exec("/bin/echo", "hello", stdout=PIPE)
    stdout, stderr = await process.communicate()
    assert stdout == b"hello\n"
    assert stderr is None
    assert process.returncode == 0


async def test_a_shell_command_reports_its_exit_status() -> None:
    process = await asyncio.create_subprocess_shell("echo shell && exit 3", stdout=PIPE)
    stdout, _stderr = await process.communicate()
    assert stdout == b"shell\n"
    assert process.returncode == 3


async def test_stdin_reaches_the_child() -> None:
    process = await asyncio.create_subprocess_exec("/bin/cat", stdin=PIPE, stdout=PIPE)
    stdout, _stderr = await process.communicate(b"round trip")
    assert stdout == b"round trip"
    assert process.returncode == 0


async def test_stdout_and_stderr_stay_separate() -> None:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import sys; sys.stdout.write('out'); sys.stderr.write('err')",
        stdout=PIPE,
        stderr=PIPE,
    )
    stdout, stderr = await process.communicate()
    assert (stdout, stderr) == (b"out", b"err")


async def test_a_large_payload_survives_the_pipe() -> None:
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import sys; sys.stdout.buffer.write(b'x' * (1 << 20))", stdout=PIPE
    )
    stdout, _stderr = await process.communicate()
    assert stdout == b"x" * (1 << 20)


async def test_a_child_can_be_terminated() -> None:
    process = await asyncio.create_subprocess_exec(sys.executable, "-c", "import time; time.sleep(30)")
    process.terminate()
    assert await asyncio.wait_for(process.wait(), 10) == -signal.SIGTERM


async def test_a_child_can_be_killed() -> None:
    process = await asyncio.create_subprocess_exec(sys.executable, "-c", "import time; time.sleep(30)")
    process.kill()
    assert await asyncio.wait_for(process.wait(), 10) == -signal.SIGKILL


async def test_a_signal_can_be_sent_to_a_child() -> None:
    process = await asyncio.create_subprocess_exec(sys.executable, "-c", "import time; time.sleep(30)")
    process.send_signal(signal.SIGINT)
    assert await asyncio.wait_for(process.wait(), 10) != 0


async def test_the_environment_and_directory_are_passed_through() -> None:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import os; print(os.environ['ZUV_MARK'], os.path.realpath(os.getcwd()))",
        stdout=PIPE,
        env={**os.environ, "ZUV_MARK": "set"},
        cwd="/tmp",
    )
    stdout, _stderr = await process.communicate()
    assert stdout.startswith(b"set ")
    assert stdout.rstrip().endswith(b"/tmp")


async def test_the_transport_exposes_the_process() -> None:
    loop = running_loop()

    class Collector(asyncio.SubprocessProtocol):
        def __init__(self) -> None:
            self.output: dict[int, bytearray] = {}
            self.exited = loop.create_future()

        def pipe_data_received(self, fd: int, data: bytes) -> None:
            self.output.setdefault(fd, bytearray()).extend(data)

        def process_exited(self) -> None:
            self.exited.set_result(None)

    transport, protocol = await loop.subprocess_exec(Collector, "/bin/echo", "via transport", stdout=PIPE)
    try:
        await asyncio.wait_for(protocol.exited, 10)
        assert isinstance(transport, asyncio.SubprocessTransport)
        assert transport.get_pid() > 0
        assert transport.get_returncode() == 0
        assert transport.get_pipe_transport(1) is not None
        assert bytes(protocol.output[1]) == b"via transport\n"
    finally:
        transport.close()


async def test_text_mode_arguments_are_rejected() -> None:
    loop = running_loop()
    for kwargs, message in (
        ({"universal_newlines": True}, "universal_newlines"),
        ({"shell": True}, "shell must be False"),
        ({"bufsize": 8}, "bufsize"),
        ({"text": True}, "text"),
        ({"encoding": "utf-8"}, "encoding"),
        ({"errors": "strict"}, "errors"),
    ):
        with pytest.raises(ValueError, match=message):
            await loop.subprocess_exec(asyncio.SubprocessProtocol, "/bin/echo", **kwargs)

    with pytest.raises(ValueError, match="shell must be True"):
        await loop.subprocess_shell(asyncio.SubprocessProtocol, "echo hi", shell=False)


async def test_a_non_string_argument_is_rejected() -> None:
    loop = running_loop()
    with pytest.raises(TypeError, match="bytes or text string"):
        await loop.subprocess_exec(asyncio.SubprocessProtocol, "/bin/echo", 42)
    with pytest.raises(ValueError, match="cmd must be a string"):
        await loop.subprocess_shell(asyncio.SubprocessProtocol, 42)  # type: ignore[arg-type]


async def test_a_child_that_cannot_be_spawned_is_reported() -> None:
    with pytest.raises(FileNotFoundError):
        await asyncio.create_subprocess_exec("/nonexistent-program-zuvloop", stdout=PIPE)


async def test_several_children_run_concurrently() -> None:
    async def run(index: int) -> bytes:
        process = await asyncio.create_subprocess_exec("/bin/echo", str(index), stdout=PIPE)
        stdout, _stderr = await process.communicate()
        return stdout.strip()

    results = await asyncio.gather(*(run(index) for index in range(8)))
    assert results == [str(index).encode() for index in range(8)]


async def test_a_cancelled_spawn_reaps_the_child() -> None:
    task: asyncio.Task[Any] = asyncio.ensure_future(
        asyncio.create_subprocess_exec(sys.executable, "-c", "import time; time.sleep(30)", stdout=PIPE)
    )
    # The child is spawned synchronously and the setup then waits for its pipes,
    # which is where a cancellation has to close the transport and reap it.
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def test_the_private_reader_names_asyncio_uses_are_present() -> None:
    """The pidfd child watcher drives the loop through these, not the public names."""
    loop = running_loop()
    read_fd, write_fd = os.pipe()
    seen: asyncio.Future[None] = loop.create_future()

    def ready() -> None:
        seen.set_result(None)
        loop._remove_reader(read_fd)

    try:
        loop._add_reader(read_fd, ready)
        os.write(write_fd, b"x")
        await asyncio.wait_for(seen, 2)
        assert loop._remove_reader(read_fd) is False
    finally:
        os.close(read_fd)
        os.close(write_fd)
