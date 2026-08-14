from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
from collections.abc import Callable, Sequence
from typing import IO, TYPE_CHECKING, Any, Protocol

from . import _zuvloop

if TYPE_CHECKING:
    from ._connect import ConnectionOperations

_DEVNULL = -3
_NAMES = ("stdin", "stdout", "stderr")


class _PolledProcess(Protocol):
    def poll(self) -> int | None: ...


class _ReaperLoop(Protocol):
    def call_soon_threadsafe(self, callback: Callable[[int], object], returncode: int) -> object: ...


class _ExitReceiver(Protocol):
    def exited(self, returncode: int) -> None: ...


class Popen:
    """The surface `asyncio.BaseSubprocessTransport` expects, over `uv_spawn`.

    asyncio's transport talks to a `subprocess.Popen`: it reads `pid`, reads and
    writes `returncode`, calls `poll`, `send_signal`, `terminate` and `kill`, and
    takes the three pipe objects. Presenting that surface is what lets the whole
    of that transport work over a libuv process handle.
    """

    def __init__(
        self,
        loop: ConnectionOperations,
        args: list[str],
        *,
        executable: str | None = None,
        env: dict[str, str] | None = None,
        cwd: str | os.PathLike[str] | None = None,
        stdin: Any = None,
        stdout: Any = None,
        stderr: Any = None,
        start_new_session: bool = False,
        startupinfo: None = None,
        creationflags: int = 0,
        pass_fds: Sequence[int] = (),
        on_exit: Callable[[int], object],
        **unsupported: Any,
    ) -> None:
        if startupinfo is not None:
            raise ValueError("startupinfo is not supported by zuvloop's subprocess transport")
        if creationflags:
            raise ValueError("creationflags is not supported by zuvloop's subprocess transport")
        if unsupported:
            name = next(iter(unsupported))
            raise ValueError(f"{name} is not supported by zuvloop's subprocess transport")

        file = executable or args[0]
        cwd_text = None if cwd is None else os.fsdecode(cwd)
        env_items = None if env is None else [f"{key}={value}" for key, value in env.items()]
        for value in (file, *args, cwd_text, *(env_items or ())):
            if value is not None and "\0" in value:
                raise ValueError("embedded null byte")

        # Before any pipe of ours can reuse a freshly closed number, and in the
        # parent, where the error beats the child's bare exit code 127.
        pass_fds = tuple(pass_fds)
        for fd in pass_fds:
            if fd < 0:
                raise ValueError("bad value(s) in pass_fds")
            os.fstat(fd)

        self.stdin: IO[bytes] | None = None
        self.stdout: IO[bytes] | None = None
        self.stderr: IO[bytes] | None = None
        self.returncode: int | None = None
        self.pid: int
        self._on_exit = on_exit
        self._stdlib: subprocess.Popen[bytes] | None = None
        self._handle: _zuvloop.Process | None = None

        # libuv's Linux fork path has no close_fds operation. Let stdlib do
        # Linux spawns: its child-side close preserves Popen's default even when
        # another thread is opening or renumbering inheritable descriptors. On
        # macOS libuv uses POSIX_SPAWN_CLOEXEC_DEFAULT and is already safe.
        if sys.platform == "linux" or pass_fds:
            process = subprocess.Popen(
                args,
                executable=executable,
                env=env,
                cwd=cwd,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                bufsize=0,
                start_new_session=start_new_session,
                pass_fds=pass_fds,
            )
            self._stdlib = process
            self.stdin = process.stdin
            self.stdout = process.stdout
            self.stderr = process.stderr
            self.pid = process.pid
            process_reaper.register(process, loop, self)
            return

        self.spawn_libuv(  # pragma: no cover - platform path exercised by macOS CI
            loop, args, file, env_items, cwd_text, stdin, stdout, stderr, start_new_session
        )

    def spawn_libuv(  # pragma: no cover - platform path exercised by macOS CI
        self,
        loop: ConnectionOperations,
        args: list[str],
        file: str,
        env_items: list[str] | None,
        cwd: str | None,
        stdin: Any,
        stdout: Any,
        stderr: Any,
        start_new_session: bool,
    ) -> None:
        child_fds: list[int] = []
        try:
            for index, request in enumerate((stdin, stdout, stderr)):
                child, mine = open_stream(index, request, child_fds)
                child_fds.append(child)
                if mine is not None:
                    # Wrapping the descriptor hands it to the file object, which
                    # is what closes it from here.
                    setattr(self, _NAMES[index], open(mine, "wb" if index == 0 else "rb", 0))

            flags = _zuvloop.PROCESS_DETACHED if start_new_session else 0
            self._handle = loop._spawn_process(file, args, env_items, cwd, child_fds, flags, 0, 0, self.exited)
        except BaseException:
            for opened in (self.stdin, self.stdout, self.stderr):
                if opened is not None:
                    opened.close()
            raise
        finally:
            # libuv dups what it inherits, so our copies of the child's ends are
            # closed either way. Leaving them open would hold a pipe that only
            # the child should still have.
            for fd in child_fds:
                if fd >= 0:
                    os.close(fd)

        assert self._handle is not None
        self.pid = self._handle.get_pid()

    def exited(self, returncode: int) -> None:
        self.returncode = returncode
        self._on_exit(returncode)

    def poll(self) -> int | None:
        return self.returncode

    def send_signal(self, signum: int) -> None:
        if self._stdlib is not None:
            self._stdlib.send_signal(signum)
        else:  # pragma: no cover - platform path exercised by macOS CI
            assert self._handle is not None
            self._handle.send_signal(signum)

    def kill(self) -> None:
        # `BaseSubprocessTransport.close()` reaches for this on a child that is
        # still running.
        self.send_signal(signal.SIGKILL)


class ProcessReaper:
    """Poll every stdlib child from one bounded process-wide reaper thread."""

    def __init__(self) -> None:
        self.condition = threading.Condition()
        self.processes: dict[int, tuple[_PolledProcess, _ReaperLoop, _ExitReceiver]] = {}
        self.started = False

    def register(self, process: subprocess.Popen[bytes], loop: ConnectionOperations, wrapper: Popen) -> None:
        with self.condition:
            self.processes[process.pid] = (process, loop, wrapper)
            if not self.started:
                threading.Thread(target=self.run, name="zuvloop-process-reaper", daemon=True).start()
                self.started = True
            self.condition.notify()

    def run(self) -> None:
        while True:
            with self.condition:
                while not self.processes:
                    self.condition.wait()
                processes = tuple(self.processes.items())

            completed = False
            for pid, (process, loop, wrapper) in processes:
                returncode = process.poll()
                if returncode is None:
                    continue
                completed = True
                with self.condition:
                    current = self.processes.get(pid)
                    if current is not None and current[0] is process:
                        del self.processes[pid]
                try:
                    loop.call_soon_threadsafe(wrapper.exited, returncode)
                except RuntimeError:  # pragma: no cover - loop closed while child exited
                    pass

            if not completed:
                with self.condition:
                    self.condition.wait(0.01)


process_reaper = ProcessReaper()


def open_stream(  # pragma: no cover - platform helper exercised by macOS CI
    index: int, request: Any, child_fds: list[int]
) -> tuple[int, int | None]:
    """Returns the descriptor the child inherits, and ours if there is one.

    `child_fds` holds what the earlier streams resolved to, which is what lets a
    redirected stderr follow the child's stdout wherever it was pointed.
    """
    if request is None:
        return -1 if index == 0 else os.dup(index), None
    if request == subprocess.DEVNULL or request == _DEVNULL:
        return os.open(os.devnull, os.O_RDONLY if index == 0 else os.O_WRONLY), None
    if request == subprocess.PIPE:
        read_fd, write_fd = os.pipe()
        # Only the child's end may be inheritable. If our end leaked into the
        # child (as it would on Linux, where uv_spawn leaves inheritable
        # descriptors open), the child would hold a writer on its own stdin
        # pipe and never see EOF.
        child_fd, our_fd = (read_fd, write_fd) if index == 0 else (write_fd, read_fd)
        os.set_inheritable(child_fd, True)
        return child_fd, our_fd
    if request == subprocess.STDOUT:
        # Whatever stdout resolved to, which is a pipe back to us whenever one
        # was asked for - never our own stdout unless the child inherited it.
        return os.dup(child_fds[1]), None
    fd = request if isinstance(request, int) else request.fileno()
    return os.dup(fd), None
