"""Package marker for the scratch demos — and the sys.path bootstrap they need.

The demos are written to be run two ways, and only one of them used to work:

    python scratch/scratch_audit.py        # sys.path[0] is scratch/
    python -m scratch.scratch_audit        # sys.path[0] is the repo root

Every demo starts with `from log_demo import start_demo_run`, which resolves
under the first form (the script's own directory leads sys.path) and raised
`ModuleNotFoundError: No module named 'log_demo'` under the second, because
`scratch/` is not on the path there — only its parent is. The demo died before
it printed a line.

Fixing it here rather than in each of the fifteen demos: `python -m
scratch.<demo>` imports this package before it runs the module, so the two
inserts below happen first, and a new demo inherits the fix instead of
rediscovering the bug. Both directories are named explicitly because the two
invocations each supply one of them:

- `scratch/` so `log_demo` (and any future shared demo helper) is importable
- the repo root so `config`, `logging_config` and the project packages are

`log_demo` inserts the repo root itself, which is what the direct-script form
relies on; that stays, so a demo run as a script does not depend on this file
having been imported.
"""

import sys
from pathlib import Path

_SCRATCH_DIR = Path(__file__).resolve().parent
_REPO_ROOT = _SCRATCH_DIR.parent

for _path in (str(_REPO_ROOT), str(_SCRATCH_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)
