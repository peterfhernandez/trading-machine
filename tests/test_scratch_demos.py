"""The scratch demos have to be runnable both documented ways.

    python scratch/scratch_audit.py        # sys.path[0] is scratch/
    python -m scratch.scratch_audit        # sys.path[0] is the repo root

Every demo opens with `from log_demo import start_demo_run`, and under the
second form that raised `ModuleNotFoundError: No module named 'log_demo'` — the
repo root is on the path, `scratch/` is not — so the demo died on line one,
before it did anything worth demonstrating. `scratch/__init__.py` puts both
directories on `sys.path`, and `python -m` imports the package before it runs
the module, so the inserts happen first.

The important tests here are subprocesses. The failure is a property of *how
the interpreter was started* — what leads `sys.path` — and the test process
cannot reproduce it, because pytest runs from the repo root with `scratch/`
reachable through neither entry. That is the Phase 5.6 lesson about
`python -m pipeline.nightly` again: an import-based assertion passes on exactly
the code path that is broken.
"""

import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent
SCRATCH_DIR = PROJECT_ROOT / "scratch"

# `check_db.py` is excluded deliberately: it is a utility rather than a demo,
# it has no `__main__` guard, and importing it reads the real datastore.
DEMO_MODULES = sorted(p.stem for p in SCRATCH_DIR.glob("scratch_*.py"))


def _run(args: list[str], tmp_path: Path) -> subprocess.CompletedProcess:
    """Run a demo in its own process, kept away from the real logs directory."""
    return subprocess.run(
        args,
        cwd=PROJECT_ROOT,
        env={
            "PATH": "/usr/bin:/bin",
            "PAPER": "true",
            "TM_LOG_DIR": str(tmp_path / "logs"),
            "SYSTEMROOT": "C:\\Windows",  # Windows needs this for sockets/random
        },
        capture_output=True,
        text=True,
        timeout=300,
    )


class TestScratchPackageBootstrap:
    """The one-line reason `python -m scratch.<demo>` can find `log_demo`."""

    def test_scratch_is_a_package(self):
        assert (SCRATCH_DIR / "__init__.py").exists()

    def test_importing_it_puts_the_scratch_directory_on_the_path(self):
        import scratch  # noqa: F401  (imported for its side effect)

        assert str(SCRATCH_DIR.resolve()) in sys.path

    def test_it_also_puts_the_repo_root_on_the_path(self):
        """`config` and `logging_config` live there, and every demo imports them."""
        import scratch  # noqa: F401

        assert str(PROJECT_ROOT.resolve()) in sys.path

    def test_repeated_imports_do_not_stack_duplicate_entries(self):
        import importlib

        import scratch

        before = sys.path.count(str(SCRATCH_DIR.resolve()))
        importlib.reload(scratch)
        assert sys.path.count(str(SCRATCH_DIR.resolve())) == before

    def test_there_are_demos_to_check(self):
        """A glob that silently matches nothing would make the sweep vacuous."""
        assert len(DEMO_MODULES) >= 10


@pytest.mark.integration
class TestEveryDemoResolvesItsImports:
    """One subprocess, every demo — the import that used to fail is at module scope.

    Importing rather than running: the module bodies only import and define, so
    this exercises the broken line without letting fifteen demos hit temp
    directories and exchange APIs.
    """

    def test_all_demos_import_as_package_submodules(self, tmp_path):
        program = (
            "import importlib\n"
            f"for name in {DEMO_MODULES!r}:\n"
            "    importlib.import_module('scratch.' + name)\n"
        )
        result = _run([sys.executable, "-c", program], tmp_path)
        assert result.returncode == 0, result.stderr


@pytest.mark.integration
class TestTheReportedInvocation:
    """`python -m scratch.scratch_audit`, the command in the bug report.

    `scratch_audit` is the demo to run for real: it is offline, it builds its
    own store under a temp directory, and it finishes in a couple of seconds.
    """

    def test_running_a_demo_as_a_module_succeeds(self, tmp_path):
        result = _run([sys.executable, "-m", "scratch.scratch_audit"], tmp_path)

        assert result.returncode == 0, result.stderr
        assert "ModuleNotFoundError" not in result.stderr
        assert "Audit Demo Complete" in result.stdout

    def test_running_the_same_demo_as_a_script_still_succeeds(self, tmp_path):
        """The form the README documents; the fix must not have traded one for the other."""
        result = _run([sys.executable, str(SCRATCH_DIR / "scratch_audit.py")], tmp_path)

        assert result.returncode == 0, result.stderr
        assert "Audit Demo Complete" in result.stdout

    def test_the_demo_logged_to_the_component_file_either_way(self, tmp_path):
        """A demo that cannot reach `log_demo` cannot set a run_id or write a tail."""
        _run([sys.executable, "-m", "scratch.scratch_audit"], tmp_path)

        audit_log = tmp_path / "logs" / "audit.log"
        assert audit_log.exists(), "the demo produced no audit.log"
        assert audit_log.read_text().splitlines()
