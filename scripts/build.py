from __future__ import annotations

import subprocess
import sys
import sysconfig
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main() -> int:
    """Rebuild `zuvloop._zuvloop` in place, or run the Zig tests, against this interpreter."""
    command = [
        "zig",
        "build",
        # The Zig tests need the same options worked out below - the include path for
        # `PyObject`, and on Windows the import library build.zig asks for - so they are
        # run from here rather than from a second copy of this in CI.
        *(["test", "--summary", "all"] if "--test" in sys.argv else []),
        f"-Dpython-include={sysconfig.get_paths()['include']}",
        f"-Dext-suffix={sysconfig.get_config_var('EXT_SUFFIX')}",
        f"-Doptimize={'Debug' if '--debug' in sys.argv else 'ReleaseFast'}",
    ]
    if sys.platform == "win32":
        abi_thread = sysconfig.get_config_var("abi_thread") or ""
        command += [
            f"-Dpython-libdir={Path(sys.base_prefix) / 'libs'}",
            f"-Dpython-lib=python{sys.version_info.major}{sys.version_info.minor}{abi_thread}",
        ]
    return subprocess.run(command, cwd=ROOT).returncode


if __name__ == "__main__":
    raise SystemExit(main())
