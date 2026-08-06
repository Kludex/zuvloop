from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import sys
from asyncio.subprocess import Process
from pathlib import Path

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


async def test_an_empty_environment_is_not_the_absence_of_one(monkeypatch: pytest.MonkeyPatch) -> None:
    """`env={}` asks for a child with no variables; only `env=None` asks for ours."""
    monkeypatch.setenv("ZUV_INHERITED", "from the parent")

    process = await asyncio.create_subprocess_exec(
        "/usr/bin/env", stdout=PIPE, stderr=asyncio.subprocess.DEVNULL, env={}
    )
    stdout, _stderr = await process.communicate()
    assert stdout == b""
    assert process.returncode == 0

    inherited = await asyncio.create_subprocess_exec(
        "/usr/bin/env", stdout=PIPE, stderr=asyncio.subprocess.DEVNULL, env=None
    )
    stdout, _stderr = await inherited.communicate()
    assert b"ZUV_INHERITED=from the parent" in stdout
    assert inherited.returncode == 0


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
    task: asyncio.Task[Process] = asyncio.ensure_future(
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


async def test_devnull_is_accepted_for_every_stream() -> None:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import sys; sys.stdout.write('gone'); sys.stderr.write('gone')",
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    assert await asyncio.wait_for(process.wait(), 10) == 0
    assert process.stdout is None
    assert process.stderr is None


async def test_stderr_can_be_merged_into_stdout() -> None:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        "import sys; sys.stdout.write('out'); sys.stdout.flush(); sys.stderr.write('err')",
        stdout=PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, stderr = await process.communicate()
    assert stderr is None
    # Both streams have to reach the child's own pipe: merging into the parent's
    # stdout would leave only "out" here, and print "err" to the terminal.
    assert stdout == b"outerr"


async def test_an_unsupported_popen_argument_is_rejected() -> None:
    with pytest.raises(ValueError, match="preexec_fn is not supported"):
        await asyncio.create_subprocess_exec("/bin/echo", stdout=PIPE, preexec_fn=lambda: None)


async def test_default_popen_arguments_are_accepted() -> None:
    process = await asyncio.create_subprocess_exec(
        "/bin/echo",
        "defaults",
        stdout=PIPE,
        startupinfo=None,
        creationflags=0,
        pass_fds=(),
    )
    stdout, _stderr = await process.communicate()
    assert stdout == b"defaults\n"


async def test_non_default_popen_arguments_are_rejected() -> None:
    with pytest.raises(ValueError, match="startupinfo is not supported"):
        await asyncio.create_subprocess_exec("/bin/echo", stdout=PIPE, startupinfo=object())
    with pytest.raises(ValueError, match="creationflags is not supported"):
        await asyncio.create_subprocess_exec("/bin/echo", stdout=PIPE, creationflags=1)


WRITE_TO_FD = "import os, sys; os.write(int(sys.argv[1]), b'through-the-pipe')"


async def test_pass_fds_reaches_the_child_at_the_same_number() -> None:
    """A descriptor keeps its number in the child, which is what makes it usable:
    the number is what the parent told the child to write to."""
    read_fd, write_fd = os.pipe()
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable, "-c", WRITE_TO_FD, str(write_fd), pass_fds=(write_fd,)
        )
        assert await process.wait() == 0
        # Our end, so the read below sees EOF rather than blocking.
        os.close(write_fd)
        assert os.read(read_fd, 64) == b"through-the-pipe"
    finally:
        os.close(read_fd)
        with contextlib.suppress(OSError):
            os.close(write_fd)


async def test_pass_fds_naming_a_standard_stream_is_what_it_already_is() -> None:
    """0, 1 and 2 are claimed by stdin, stdout and stderr, so naming one asks for
    nothing more - and must not displace the stream that holds it."""
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-c", "import sys; sys.stderr.write('still stderr')", stderr=PIPE, pass_fds=(2,)
    )
    _stdout, stderr = await process.communicate()
    assert stderr == b"still stderr"
    assert process.returncode == 0


async def test_a_descriptor_not_passed_does_not_reach_the_child() -> None:
    """The other half: without `pass_fds` it is closed, so the write fails."""
    read_fd, write_fd = os.pipe()
    try:
        process = await asyncio.create_subprocess_exec(sys.executable, "-c", WRITE_TO_FD, str(write_fd), stderr=PIPE)
        _stdout, stderr = await process.communicate()
        assert process.returncode == 1
        assert b"Bad file descriptor" in stderr
    finally:
        os.close(read_fd)
        os.close(write_fd)


async def test_pass_fds_leaves_the_gap_before_it_closed() -> None:
    """The descriptors between stderr and a high `pass_fds` are not the child's.

    The spare is opened first so it takes the lower number, which is what puts it
    in the gap rather than past the end - asserted, since the whole test rests on it.
    """
    spare_read, spare_write = os.pipe()
    read_fd, write_fd = os.pipe()
    assert 3 <= spare_write < write_fd
    try:
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            "import os, sys; os.close(int(sys.argv[1]))",
            str(spare_write),
            stderr=PIPE,
            pass_fds=(write_fd,),
        )
        _stdout, stderr = await process.communicate()
        assert process.returncode == 1
        assert b"Bad file descriptor" in stderr
    finally:
        for fd in (read_fd, write_fd, spare_read, spare_write):
            os.close(fd)


async def test_signalling_an_exited_child_is_a_no_op() -> None:
    loop = running_loop()

    class Waiter(asyncio.SubprocessProtocol):
        def __init__(self) -> None:
            self.exited = loop.create_future()

        def process_exited(self) -> None:
            self.exited.set_result(None)

    transport, protocol = await loop.subprocess_exec(Waiter, "/bin/echo", stdout=PIPE)
    try:
        # An unread pipe keeps the transport from finishing, which is the window
        # the no-op exists for: a child already reaped, still held by asyncio.
        stdout = transport.get_pipe_transport(1)
        assert isinstance(stdout, asyncio.ReadTransport)
        stdout.pause_reading()

        await asyncio.wait_for(protocol.exited, 10)
        # asyncio makes signalling an exited child a no-op rather than an error.
        transport.send_signal(signal.SIGTERM)
        transport.terminate()
        transport.kill()
    finally:
        transport.close()


async def test_a_spawn_that_fails_closes_the_pipes_it_opened() -> None:
    before = len(os.listdir("/dev/fd"))
    for _ in range(20):
        with pytest.raises(OSError):
            await asyncio.create_subprocess_exec("/nonexistent-program-zuvloop", stdin=PIPE, stdout=PIPE, stderr=PIPE)
    # A leak would grow the table by three descriptors per attempt.
    assert len(os.listdir("/dev/fd")) < before + 10


async def test_a_descriptor_can_be_handed_to_the_child(tmp_path: Path) -> None:
    target = tmp_path / "out.txt"
    with open(target, "wb") as sink:
        process = await asyncio.create_subprocess_exec("/bin/echo", "to a file", stdout=sink)
        assert await asyncio.wait_for(process.wait(), 10) == 0
    assert target.read_bytes() == b"to a file\n"


async def test_a_raw_descriptor_number_can_be_handed_to_the_child(tmp_path: Path) -> None:
    target = tmp_path / "raw.txt"
    fd = os.open(target, os.O_WRONLY | os.O_CREAT)
    try:
        process = await asyncio.create_subprocess_exec("/bin/echo", "by number", stdout=fd)
        assert await asyncio.wait_for(process.wait(), 10) == 0
    finally:
        os.close(fd)
    assert target.read_bytes() == b"by number\n"
