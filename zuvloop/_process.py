from __future__ import annotations

import io
import os
import signal
import subprocess
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from . import _zuvloop

if TYPE_CHECKING:
    from ._connect import ConnectionOperations

_DEVNULL = -3
_NAMES = ("stdin", "stdout", "stderr")


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
        on_exit: Callable[[int], object],
        **unsupported: Any,
    ) -> None:
        if unsupported:
            name = next(iter(unsupported))
            raise ValueError(f"{name} is not supported by zuvloop's subprocess transport")

        self.stdin: io.BufferedWriter | None = None
        self.stdout: io.BufferedReader | None = None
        self.stderr: io.BufferedReader | None = None
        self.returncode: int | None = None
        self._on_exit = on_exit

        child_fds: list[int] = []
        try:
            for index, request in enumerate((stdin, stdout, stderr)):
                child, mine = _open_stream(index, request)
                child_fds.append(child)
                if mine is not None:
                    # Wrapping the descriptor hands it to the file object, which
                    # is what closes it from here.
                    setattr(self, _NAMES[index], open(mine, "wb" if index == 0 else "rb", 0))

            flags = _zuvloop.PROCESS_DETACHED if start_new_session else 0
            self._handle = loop._spawn_process(
                executable or args[0],
                args,
                None if env is None else [f"{k}={v}" for k, v in env.items()],
                None if cwd is None else os.fspath(cwd),
                child_fds,
                flags,
                0,
                0,
                self._exited,
            )
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

        self.pid: int = self._handle.get_pid()

    def _exited(self, returncode: int) -> None:
        self.returncode = returncode
        self._on_exit(returncode)

    def poll(self) -> int | None:
        return self.returncode

    def send_signal(self, signum: int) -> None:
        self._handle.send_signal(signum)

    def kill(self) -> None:
        # `BaseSubprocessTransport.close()` reaches for this on a child that is
        # still running.
        self.send_signal(signal.SIGKILL)


def _open_stream(index: int, request: Any) -> tuple[int, int | None]:
    """Returns the descriptor the child inherits, and ours if there is one."""
    if request is None:
        return -1 if index == 0 else _dup_std(index), None
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
        return _dup_std(1), None
    fd = request if isinstance(request, int) else request.fileno()
    return os.dup(fd), None


def _dup_std(index: int) -> int:
    return os.dup(index)
