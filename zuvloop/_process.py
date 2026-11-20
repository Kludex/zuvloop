from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading
from collections.abc import Callable, Iterable, Mapping
from typing import IO, TYPE_CHECKING, Protocol, TypedDict

from . import _zuvloop

if TYPE_CHECKING:
    from ._connect import ConnectionOperations

_DEVNULL = -3
_NAMES = ("stdin", "stdout", "stderr")

if sys.platform == "win32":
    # The signal module has no `SIGKILL` there; libuv accepts the number and
    # turns it into `TerminateProcess`.
    _SIGKILL = 9
else:
    _SIGKILL = signal.SIGKILL


class _PolledProcess(Protocol):
    def poll(self) -> int | None: ...


class _ReaperLoop(Protocol):
    def call_soon_threadsafe(self, callback: Callable[[int], object], returncode: int) -> object: ...


class _ExitReceiver(Protocol):
    def exited(self, returncode: int) -> None: ...


class _StartupInfo(Protocol):
    """`subprocess.STARTUPINFO`, which only exists on Windows."""

    dwFlags: int


# What a stdio argument may be: a descriptor, one of `subprocess`'s markers, an
# open file, or nothing.
type Stdio = int | IO[bytes] | IO[str] | None


class SpawnOptions(TypedDict, total=False):
    """The `subprocess.Popen` arguments a spawn forwards."""

    executable: str | None
    env: Mapping[str, str] | None
    cwd: str | bytes | os.PathLike[str] | os.PathLike[bytes] | None
    start_new_session: bool
    startupinfo: _StartupInfo | None
    creationflags: int
    pass_fds: Iterable[int]
    user: str | int | None
    group: str | int | None
    extra_groups: Iterable[str | int] | None
    umask: int
    process_group: int | None
    close_fds: bool
    restore_signals: bool
    preexec_fn: Callable[[], object] | None
    pipesize: int


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
        env: Mapping[str, str] | None = None,
        cwd: str | bytes | os.PathLike[str] | os.PathLike[bytes] | None = None,
        stdin: Stdio = None,
        stdout: Stdio = None,
        stderr: Stdio = None,
        start_new_session: bool = False,
        windows_verbatim: bool = False,
        startupinfo: _StartupInfo | None = None,
        creationflags: int = 0,
        pass_fds: Iterable[int] = (),
        user: str | int | None = None,
        group: str | int | None = None,
        extra_groups: Iterable[str | int] | None = None,
        umask: int = -1,
        process_group: int | None = None,
        close_fds: bool = True,
        restore_signals: bool = True,
        preexec_fn: Callable[[], object] | None = None,
        pipesize: int = -1,
        on_exit: Callable[[int], object],
    ) -> None:

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
        # libuv cannot change the child's groups, umask or process group either,
        # nor run a hook in it, keep its signal dispositions or size its pipes.
        # Windows has none of those: stdlib rejects the credentials and the hook,
        # and ignores the rest. `uv_spawn` takes no startup info or creation
        # flags, which are Windows' own and rejected by stdlib elsewhere.
        posix_only = umask != -1 or process_group is not None or not restore_signals or pipesize != -1
        if (
            sys.platform == "linux"
            or pass_fds
            or any(value is not None for value in (user, group, extra_groups, preexec_fn))
            or (posix_only and sys.platform != "win32")
            or startupinfo is not None
            or creationflags
            or not close_fds
        ):
            process = subprocess.Popen(
                # A string reaches `CreateProcess` as written, which is what the
                # shell's own quoting needs; a list would be escaped again.
                " ".join(args) if windows_verbatim else args,
                # As CPython's own shell spawn does: the unquoted shell path in
                # that string is not parsed when the program is named here.
                executable=file if windows_verbatim else executable,
                env=env,
                cwd=cwd,
                stdin=stdin,
                stdout=stdout,
                stderr=stderr,
                bufsize=0,
                start_new_session=start_new_session,
                pass_fds=pass_fds,
                user=user,
                group=group,
                extra_groups=extra_groups,
                umask=umask,
                process_group=process_group,
                close_fds=close_fds,
                restore_signals=restore_signals,
                preexec_fn=preexec_fn,
                pipesize=pipesize,
                startupinfo=startupinfo,
                creationflags=creationflags,
            )
            self._stdlib = process
            self.stdin = process.stdin
            self.stdout = process.stdout
            self.stderr = process.stderr
            self.pid = process.pid
            process_reaper.register(process, loop, self)
            return

        self.spawn_libuv(  # pragma: no cover - platform path exercised by macOS CI
            loop,
            args,
            file,
            env_items,
            cwd_text,
            stdin,
            stdout,
            stderr,
            start_new_session,
            windows_verbatim,
        )

    def spawn_libuv(  # pragma: no cover - platform path exercised by macOS CI
        self,
        loop: ConnectionOperations,
        args: list[str],
        file: str,
        env_items: list[str] | None,
        cwd: str | None,
        stdin: Stdio,
        stdout: Stdio,
        stderr: Stdio,
        start_new_session: bool,
        windows_verbatim: bool,
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

            flags = (_zuvloop.PROCESS_DETACHED if start_new_session else 0) | (
                _zuvloop.PROCESS_WINDOWS_VERBATIM if windows_verbatim else 0
            )
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
            # Windows' `Popen.send_signal` knows no signal 9; `kill` is the same
            # signal on POSIX.
            self._stdlib.kill() if signum == _SIGKILL else self._stdlib.send_signal(signum)
        else:  # pragma: no cover - platform path exercised by macOS CI
            assert self._handle is not None
            self._handle.send_signal(signum)

    def kill(self) -> None:
        # `BaseSubprocessTransport.close()` reaches for this on a child that is
        # still running.
        self.send_signal(_SIGKILL)


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
    index: int, request: Stdio, child_fds: list[int]
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
