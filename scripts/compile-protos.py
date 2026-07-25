#!/usr/bin/env python3
"""Drop-in shim for scitrera-repo-tools `compile-protos`.

Copy this file into any repo's `scripts/` directory (or run it from anywhere)
to compile that repo's `.proto` files for every language declared in the
`proto:` block of its `versions.yaml`.

Resolution order:

  1. Package already importable in the current Python  ->  call directly.

  2. `uvx` (or `uv`) on PATH  ->  run via uvx with no persistent install.
     Set `REPO_TOOLS_SOURCE` to override the package source. Default:
     `git+https://github.com/scitrera/repo-tools.git`. Pin to a tag or
     PyPI version, e.g.:
         REPO_TOOLS_SOURCE='scitrera-repo-tools==0.1.11'

  3. Otherwise  ->  print install instructions and exit 1.

Usage:
    python scripts/compile_protos.py            # regenerate missing/stale output
    python scripts/compile_protos.py --check    # never write; exit 1 on drift
    python scripts/compile_protos.py --lang go  # one language (repeatable)

Note that the Python codegen path runs `grpc_tools.protoc` under the
interpreter executing this script. When repo-tools is installed outside the
project's virtualenv (including via the uvx path above), point it at the right
one so the `grpcio_tools` pin is resolved against the environment that will
actually generate the code:

    python scripts/compile_protos.py --python .venv/bin/python

All flags pass through to `compile-protos`.
"""

#  Copyright (c) 2026. Scitrera LLC. Licensed under 3-clause BSD license
#  (see LICENSE file at https://github.com/scitrera/repo-tools/blob/main/LICENSE)

from __future__ import annotations

import os
import shutil
import sys
from typing import List

DEFAULT_SOURCE = "git+https://github.com/scitrera/repo-tools.git"


def _try_uvx(args: List[str]) -> None:
    """Replace the current process with a uvx invocation."""
    uv = shutil.which("uvx") or shutil.which("uv")
    if uv is None:
        return
    source = os.environ.get("REPO_TOOLS_SOURCE", DEFAULT_SOURCE)
    name = os.path.basename(uv)
    if name == "uv":
        cmd = [uv, "tool", "run", "--from", source, "compile-protos", *args]
    else:
        cmd = [uv, "--from", source, "compile-protos", *args]
    os.execvp(cmd[0], cmd)  # never returns


def _try_import(args: List[str]) -> bool:
    try:
        from scitrera_repo_tools.compile_protos.cli import main as protos_main
    except ImportError:
        return False
    sys.argv = ["compile-protos", *args]
    protos_main()
    return True


def main(argv: List[str]) -> int:
    if _try_import(argv):
        return 0
    _try_uvx(argv)
    source = os.environ.get("REPO_TOOLS_SOURCE", DEFAULT_SOURCE)
    sys.stderr.write(
        "scitrera-repo-tools is not available.\n"
        "Install one of:\n"
        f"  uv tool install --from {source} scitrera-repo-tools\n"
        f"  pip install {source}\n"
    )
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
